#!/usr/bin/env python3
# zmq_subscriber.py
"""
KV Cache Event Listener Sidecar for vLLM (per-pod)

- Subscribes to vLLM KV events over ZMQ
- Decodes msgpack KVEventBatch (same wire format as former centralized listener)
- Stores KV block presence information in Redis

Redis schema (per MODEL_NAME_REDIS):
  - {MODEL}:kvblock:{block_hash}        (HASH)  block -> { pod_name: timestamp }
  - {MODEL}:podblocks:{pod_name}        (SET)   pod   -> { block_hashes }
  - {MODEL}:kvblocks                    (HASH)  index of all block_hashes
"""

import os
import socket
import time
import threading
from typing import Any, Optional, Union, NewType

import zmq
import msgspec
import redis

from .config import get_config

_cfg = get_config()

# ------------------------------
# Type definitions (match old listener)
# ------------------------------

BlockHash = NewType("BlockHash", int)


class EventBatch(msgspec.Struct, array_like=True, omit_defaults=True, gc=False):
    ts: float
    events: list[Any]


class KVCacheEvent(msgspec.Struct, array_like=True, omit_defaults=True, gc=False, tag=True):
    """Base class for KV cache events."""
    pass


class BlockStored(KVCacheEvent):
    block_hashes: list[BlockHash]
    parent_block_hash: Optional[BlockHash]
    token_ids: list[int]
    block_size: int
    lora_id: Optional[int]


class BlockRemoved(KVCacheEvent):
    block_hashes: list[BlockHash]


class AllBlocksCleared(KVCacheEvent):
    pass


class KVEventBatch(EventBatch):
    events: list[Union[BlockStored, BlockRemoved, AllBlocksCleared]]


# ------------------------------
# Helpers
# ------------------------------

def _resolve_zmq_endpoints(
    leader_name: str,
    base_host: str,
    base_port: int,
    dp_size: int,
    dp_size_local: int,
    namespace: str = "vllm",
) -> tuple[list[str], list[tuple[int, str, int]]]:
    """Build ZMQ endpoints for leader-sidecar DP fan-in.

    Rank 0 is local. Worker ranks are resolved through the LWS headless-service
    DNS convention and retried later if they are not ready at startup.
    """
    resolved: list[str] = [f"tcp://{base_host}:{base_port}"]
    pending: list[tuple[int, str, int]] = []

    if dp_size <= 1:
        return resolved, pending

    svc_name = leader_name.rsplit("-", 1)[0] if "-" in leader_name else leader_name
    for rank in range(1, dp_size):
        worker_name = f"{leader_name}-{rank}"
        fqdn = f"{worker_name}.{svc_name}.{namespace}.svc.cluster.local"
        zmq_port = base_port + rank * dp_size_local
        try:
            worker_ip = socket.gethostbyname(fqdn)
            ep = f"tcp://{worker_ip}:{zmq_port}"
            resolved.append(ep)
            print(f"[KV-SUB] resolved worker rank {rank}: {fqdn} -> {ep}")
        except socket.gaierror:
            print(
                f"[KV-SUB] WARNING: cannot resolve {fqdn} at startup — "
                f"will retry in background (rank {rank}, port {zmq_port})"
            )
            pending.append((rank, fqdn, zmq_port))

    return resolved, pending


# ------------------------------
# Subscriber
# ------------------------------

class KVSubscriber:
    def __init__(self):
        self.vllm_host = _cfg.VLLM_HOST
        self.vllm_port = _cfg.VLLM_SUB_PORT
        self.redis = redis.Redis(
            host=_cfg.REDIS_HOST,
            port=_cfg.REDIS_PORT,
            decode_responses=True,
        )
        self.model = _cfg.MODEL_NAME_REDIS
        self.pod_name = _cfg.CONTAINER_NAME
        self.dp_size = _cfg.DP_SIZE
        self.dp_size_local = _cfg.DP_SIZE_LOCAL

        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

        # IMPORTANT: msgpack + KVEventBatch, not JSON
        self._decoder = msgspec.msgpack.Decoder(type=KVEventBatch)

    # ------------- lifecycle -------------

    def start(self):
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        print(
            f"[KV-SUB] started (host={self.vllm_host}, port={self.vllm_port}, "
            f"pod={self.pod_name}, model={self.model}, "
            f"dp_size={self.dp_size}, dp_size_local={self.dp_size_local})"
        )

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None
        print("[KV-SUB] stopped")

    # ------------- core loop -------------

    _RETRY_INTERVAL_S: float = 10.0
    _RETRY_MAX: int = 90

    def _loop(self):
        ctx = zmq.Context()
        sub = ctx.socket(zmq.SUB)

        resolved, pending = _resolve_zmq_endpoints(
            leader_name=self.pod_name,
            base_host=self.vllm_host,
            base_port=self.vllm_port,
            dp_size=self.dp_size,
            dp_size_local=self.dp_size_local,
        )

        for ep in resolved:
            sub.connect(ep)
            print(f"[KV-SUB] connected to {ep}")

        # Match old listener: subscribe only to KV topic prefix
        sub.setsockopt_string(zmq.SUBSCRIBE, "kv@")

        pending_note = f", {len(pending)} pending DNS retry" if pending else ""
        print(
            f"[KV-SUB] subscribed to {len(resolved)} endpoint(s) "
            f"(topic prefix 'kv@'){pending_note}"
        )

        retry_pending = list(pending)
        retry_attempt = 0
        retry_last_t = time.monotonic()

        try:
            while not self._stop.is_set():
                try:
                    # Publisher: [topic, seq_bytes, payload]
                    frames = sub.recv_multipart(flags=zmq.NOBLOCK)
                except zmq.Again:
                    if retry_pending and retry_attempt < self._RETRY_MAX:
                        now = time.monotonic()
                        if now - retry_last_t >= self._RETRY_INTERVAL_S:
                            retry_last_t = now
                            retry_attempt += 1
                            still_pending: list[tuple[int, str, int]] = []
                            for rank, fqdn, zmq_port in retry_pending:
                                try:
                                    worker_ip = socket.gethostbyname(fqdn)
                                    ep = f"tcp://{worker_ip}:{zmq_port}"
                                    sub.connect(ep)
                                    print(
                                        f"[KV-SUB] rank {rank} resolved after "
                                        f"{retry_attempt} retries: {fqdn} -> {ep}, connected"
                                    )
                                except socket.gaierror:
                                    still_pending.append((rank, fqdn, zmq_port))
                            retry_pending = still_pending
                            if not retry_pending:
                                print("[KV-SUB] all pending worker endpoints resolved")
                    elif retry_pending and retry_attempt >= self._RETRY_MAX:
                        for rank, fqdn, _ in retry_pending:
                            print(
                                f"[KV-SUB] WARNING: gave up resolving rank {rank} "
                                f"({fqdn}) after {self._RETRY_MAX} retries "
                                f"(~{self._RETRY_MAX * self._RETRY_INTERVAL_S / 60:.0f} min) "
                                f"— rank {rank} ZMQ events will be missed"
                            )
                        retry_pending = []
                    time.sleep(0.01)
                    continue
                except Exception as e:
                    print(f"[KV-SUB] ZMQ recv error: {e}")
                    time.sleep(1.0)
                    continue

                if len(frames) != 3:
                    print(f"[KV-SUB] unexpected frame count: {len(frames)} (expected 3)")
                    continue

                topic, seq_bytes, payload = frames
                # seq = int.from_bytes(seq_bytes, "big")  # unused but available

                try:
                    batch = self._decoder.decode(payload)
                except Exception as e:
                    print(f"[KV-SUB] decode error (msgpack KVEventBatch): {e}")
                    continue

                try:
                    self._handle_batch(batch)
                except Exception as e:
                    print(f"[KV-SUB] error handling KV batch: {e}")

        finally:
            try:
                sub.close(0)
            except Exception:
                pass
            ctx.term()

    # ------------- Redis update -------------

    def _handle_batch(self, event_batch: KVEventBatch) -> None:
        """
        Update Redis for KV cache events.

        In DP mode, all ranks' blocks are registered under `self.pod_name`
        (the leader sidecar identity), so the router sees one endpoint.

        Redis schema:
          - {model}:kvblocks               -> HSET(block_hash -> "kvblock:{block_hash}")
          - {model}:kvblock:{block_hash}   -> HSET(pod_name -> timestamp)
          - {model}:podblocks:{pod_name}   -> SET(block_hashes held by pod)
        """
        key_prefix = f"{self.model}:" if self.model else ""
        kvblocks_key = f"{key_prefix}kvblocks"
        podblocks_key = f"{key_prefix}podblocks:{self.pod_name}"

        pipe = self.redis.pipeline(transaction=False)
        ts = int(time.time())

        for ev in event_batch.events:
            # BlockStored
            if isinstance(ev, BlockStored):
                for block_hash in ev.block_hashes:
                    bh_str = str(block_hash)
                    kvblock_key = f"{key_prefix}kvblock:{bh_str}"

                    # block -> pod
                    pipe.hset(kvblock_key, self.pod_name, ts)
                    # pod -> block
                    pipe.sadd(podblocks_key, bh_str)
                    # index of all blocks
                    pipe.hset(kvblocks_key, bh_str, kvblock_key)

            # BlockRemoved
            elif isinstance(ev, BlockRemoved):
                for block_hash in ev.block_hashes:
                    bh_str = str(block_hash)
                    kvblock_key = f"{key_prefix}kvblock:{bh_str}"
                    pipe.hdel(kvblock_key, self.pod_name)
                    pipe.srem(podblocks_key, bh_str)

            # AllBlocksCleared
            elif isinstance(ev, AllBlocksCleared):
                for bh_str in self.redis.sscan_iter(podblocks_key):
                    kvblock_key = f"{key_prefix}kvblock:{bh_str}"
                    pipe.hdel(kvblock_key, self.pod_name)
                pipe.delete(podblocks_key)

        try:
            pipe.execute()
        except Exception as e:
            print(f"[KV-SUB] redis error: {e}")
