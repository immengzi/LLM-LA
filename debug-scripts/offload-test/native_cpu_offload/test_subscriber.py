#!/usr/bin/env python3
import os
import time
import threading
from typing import Any, Optional, Union

import zmq
import msgspec
import redis

# ------------------------------
# 1. Configuration
# ------------------------------
VLLM_HOST = os.getenv("VLLM_HOST", "127.0.0.1")
VLLM_PORT = os.getenv("VLLM_SUB_PORT", "5557")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
MODEL_NAME = os.getenv("MODEL_NAME_REDIS", "qwen3-8b")
CONTAINER_NAME = os.getenv("CONTAINER_NAME", "vllm-offload")

# ------------------------------
# 2. Type definitions
# ------------------------------
class EventBatch(msgspec.Struct, array_like=True, omit_defaults=True, gc=False):
    ts: float
    events: list[Any]

class KVCacheEvent(msgspec.Struct, array_like=True, omit_defaults=True, gc=False, tag=True):
    pass

class BlockStored(KVCacheEvent):
    block_hashes: list[Any]
    parent_block_hash: Optional[Any]
    token_ids: list[int]
    block_size: int
    lora_id: Optional[int]
    medium: str

class BlockRemoved(KVCacheEvent):
    block_hashes: list[Any]
    medium: str  

class AllBlocksCleared(KVCacheEvent):
    pass

class KVEventBatch(EventBatch):
    events: list[Union[BlockStored, BlockRemoved, AllBlocksCleared]]

# ------------------------------
# 3. The Subscriber Class
# ------------------------------
class KVSubscriber:
    def __init__(self):
        self.redis = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True,
        )
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._decoder = msgspec.msgpack.Decoder(type=KVEventBatch)

    def _normalize_bh(self, bh: Any) -> str:
        """
        核心对齐逻辑：将 bytes (CPU) 和 int (NPU) 统一为数字字符串
        原理：取 SHA256 摘要的后 8 字节转大端 uint64
        """
        if isinstance(bh, int):
            return str(bh)
        if isinstance(bh, bytes):
            # 这里的 [-8:] 是根据 sniff 抓包对比得出的关键逻辑
            val = int.from_bytes(bh[-8:], byteorder="big")
            return str(val)
        return str(bh)

    def start(self):
        if self._thread is not None: return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        print(f" [KV-SUB] Subscriber started at {VLLM_HOST}:{VLLM_PORT}")

    def _loop(self):
        ctx = zmq.Context()
        sub = ctx.socket(zmq.SUB)
        sub.connect(f"tcp://{VLLM_HOST}:{VLLM_PORT}")
        specific_topic = f"kv@{CONTAINER_NAME}@"
        sub.setsockopt_string(zmq.SUBSCRIBE, specific_topic)

        try:
            while not self._stop.is_set():
                try:
                    frames = sub.recv_multipart(flags=zmq.NOBLOCK)
                except zmq.Again:
                    time.sleep(0.01)
                    continue

                if len(frames) != 3: continue
                _, _, payload = frames
                try:
                    batch = self._decoder.decode(payload)
                    self._handle_batch(batch)
                except Exception:
                    pass 
        finally:
            sub.close(0)
            ctx.term()

    def _handle_batch(self, event_batch: KVEventBatch) -> None:
        key_prefix = f"{MODEL_NAME}:" if MODEL_NAME else ""
        podblocks_key = f"{key_prefix}podblocks:{CONTAINER_NAME}"
        kvblocks_index = f"{key_prefix}kvblocks"

        pipe = self.redis.pipeline(transaction=False)
        ts = int(time.time())

        for ev in event_batch.events:
            # 处理存储事件 (Stored)
            if isinstance(ev, BlockStored):
                med = ev.medium.upper()
                processed_hashes = []
                for bh in ev.block_hashes:
                    bh_str = self._normalize_bh(bh)
                    processed_hashes.append(bh_str)
                    
                    kvblock_key = f"{key_prefix}kvblock:{bh_str}"
                    pipe.hset(kvblock_key, f"{CONTAINER_NAME}:medium", med)
                    pipe.hset(kvblock_key, CONTAINER_NAME, ts)
                    pipe.sadd(podblocks_key, bh_str)
                    pipe.hset(kvblocks_index, bh_str, kvblock_key)
                
                # 打印所有哈希
                print(f" [STORED] Medium: {med}, Hash: {processed_hashes}")

            # 处理移除事件 (Removed)
            elif isinstance(ev, BlockRemoved):
                med = ev.medium.upper() if ev.medium else "UNKNOWN"
                processed_hashes = []
                for bh in ev.block_hashes:
                    bh_str = self._normalize_bh(bh)
                    processed_hashes.append(bh_str)
                    
                    kvblock_key = f"{key_prefix}kvblock:{bh_str}"
                    pipe.hdel(kvblock_key, CONTAINER_NAME)
                    pipe.hdel(kvblock_key, f"{CONTAINER_NAME}:medium")
                    pipe.srem(podblocks_key, bh_str)
                
                print(f" [REMOVED] Medium: {med}, Hash: {processed_hashes}")

        pipe.execute()

if __name__ == "__main__":
    sub = KVSubscriber()
    sub.start()
    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down.")