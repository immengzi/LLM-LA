#!/usr/bin/env python3
# zmq_subscriber.py
"""
KV Cache Event Listener Sidecar for vLLM

- Subscribes to vLLM KV events over ZMQ
- Decodes events using msgspec
- Stores KV block presence information in Redis

Redis schema (per MODEL_NAME):
  - {MODEL}:kvblock:{block_hash}        (HASH)  block -> { pod_name: timestamp }
  - {MODEL}:podblocks:{pod_name}        (SET)   pod   -> { block_hashes }
  - {MODEL}:kvblocks                    (HASH)  index of all block_hashes

Environment variables:
  - VLLM_HOST       : host of vLLM ZMQ publisher (inside pod, usually 127.0.0.1)
  - VLLM_SUB_PORT   : ZMQ port (e.g. "5557")
  - REDIS_HOST      : Redis hostname (e.g. "redis")
  - REDIS_PORT      : Redis port (default "6379")
  - CONTAINER_NAME  : Pod or container name, used as pod_name in Redis
  - MODEL_NAME      : Model identifier used as prefix in Redis keys
"""

import os
import time
import signal
import threading
from typing import Any

import zmq
import msgspec
import redis

from .config import get_config

_cfg = get_config()


class KVEvent(msgspec.Struct, omit_defaults=True):
    kind: str
    block_hash: int
    pod_name: str
    ts: float


class KVSubscriber:
    def __init__(self):
        self.vllm_host = _cfg.VLLM_HOST
        self.vllm_port = _cfg.VLLM_SUB_PORT
        self.redis = redis.Redis(host=_cfg.REDIS_HOST, port=_cfg.REDIS_PORT, decode_responses=True)
        self.model = _cfg.MODEL_NAME_REDIS
        self.pod_name = _cfg.CONTAINER_NAME
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._decoder = msgspec.json.Decoder(list[KVEvent])

    def start(self):
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        print(f"[KV-SUB] started (host={self.vllm_host}, port={self.vllm_port})")

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None
        print("[KV-SUB] stopped")

    def _loop(self):
        ctx = zmq.Context()
        sub = ctx.socket(zmq.SUB)
        sub.connect(f"tcp://{self.vllm_host}:{self.vllm_port}")
        sub.setsockopt_string(zmq.SUBSCRIBE, "")
        print("[KV-SUB] connected to vLLM publisher")

        try:
            while not self._stop.is_set():
                try:
                    msg = sub.recv(flags=zmq.NOBLOCK)
                except zmq.Again:
                    time.sleep(0.01)
                    continue

                try:
                    events = self._decoder.decode(msg)
                except Exception as e:
                    print(f"[KV-SUB] decode error: {e}")
                    continue

                self._handle_events(events)
        finally:
            sub.close(0)
            ctx.term()

    def _handle_events(self, events: list[KVEvent]):
        pipe = self.redis.pipeline()
        now = time.time()

        for ev in events:
            key_block = f"{self.model}:kvblock:{ev.block_hash}"
            key_podblocks = f"{self.model}:podblocks:{ev.pod_name}"
            key_kvblocks = f"{self.model}:kvblocks"

            if ev.kind == "BlockAdded":
                pipe.hset(key_block, ev.pod_name, now)
                pipe.sadd(key_podblocks, ev.block_hash)
                pipe.hset(key_kvblocks, ev.block_hash, now)
            elif ev.kind == "BlockRemoved":
                pipe.hdel(key_block, ev.pod_name)
                pipe.srem(key_podblocks, ev.block_hash)
            # you can add more kinds if needed

        try:
            pipe.execute()
        except Exception as e:
            print(f"[KV-SUB] redis error: {e}")
