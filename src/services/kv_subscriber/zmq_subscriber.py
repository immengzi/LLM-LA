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

import asyncio
import os
import sys
import time
from typing import Any, Optional, Union, List, NewType

import zmq
import zmq.asyncio
import msgspec
from redis import asyncio as aioredis


# ------------------------------
# Type Definitions
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
    # REMOVED: medium: Optional[str]
    # vLLM >= 0.10.x / 0.11.x does NOT send this field anymore.


class BlockRemoved(KVCacheEvent):
    block_hashes: list[BlockHash]
    # REMOVED: medium: Optional[str]
    # vLLM >= 0.10.x / 0.11.x does NOT send this field anymore.


class AllBlocksCleared(KVCacheEvent):
    pass


class KVEventBatch(EventBatch):
    events: list[Union[BlockStored, BlockRemoved, AllBlocksCleared]]


# ------------------------------
# Redis Updater
# ------------------------------

async def process_event(
    event_batch: KVEventBatch,
    redis: aioredis.Redis,
    pod_name: str,
    model_name: Optional[str] = None,
) -> None:
    """
    Update Redis for KV cache events.

    Redis schema:
      - {model}:kvblocks               -> HSET(block_hash -> "kvblock:{block_hash}")
      - {model}:kvblock:{block_hash}   -> HSET(pod_name -> timestamp)
      - {model}:podblocks:{pod_name}   -> SET(block_hashes held by pod)
    """
    key_prefix = f"{model_name}:" if model_name else ""
    kvblocks_key = f"{key_prefix}kvblocks"
    podblocks_key = f"{key_prefix}podblocks:{pod_name}"

    print(f"[{pod_name}] ⏱ Event batch at {event_batch.ts:.3f}: {len(event_batch.events)} events")

    pipe = redis.pipeline(transaction=False)
    ts = int(time.time())

    for event in event_batch.events:
        try:
            # ----------------------
            # BlockStored event
            # ----------------------
            if isinstance(event, BlockStored):
                for block_hash in event.block_hashes:
                    kvblock_key = f"{key_prefix}kvblock:{block_hash}"

                    # record mapping: block -> pod
                    pipe.hset(kvblock_key, pod_name, ts)
                    # record reverse mapping: pod -> block
                    pipe.sadd(podblocks_key, str(block_hash))
                    # optional: add block hash reference
                    pipe.hset(kvblocks_key, str(block_hash), kvblock_key)

                print(f"[{pod_name}] Stored {len(event.block_hashes)} block→pod mappings")

            # ----------------------
            # BlockRemoved event
            # ----------------------
            elif isinstance(event, BlockRemoved):
                for block_hash in event.block_hashes:
                    kvblock_key = f"{key_prefix}kvblock:{block_hash}"
                    pipe.hdel(kvblock_key, pod_name)
                    pipe.srem(podblocks_key, str(block_hash))

                print(f"[{pod_name}] Removed {len(event.block_hashes)} block→pod mappings")

            # ----------------------
            # AllBlocksCleared event
            # ----------------------
            elif isinstance(event, AllBlocksCleared):
                print(f"[{pod_name}] Clearing all blocks for this pod")

                # Iterate over all blocks for this pod and remove this pod from their hashes
                async for block_hash in redis.sscan_iter(podblocks_key):
                    kvblock_key = f"{key_prefix}kvblock:{block_hash}"
                    pipe.hdel(kvblock_key, pod_name)

                pipe.delete(podblocks_key)

        except Exception as e:
            print(f"[{pod_name}] ⚠️ Error processing event: {e}", file=sys.stderr)

    await pipe.execute()
    print(f"[{pod_name}] ✅ Redis updated")


# ------------------------------
# Main loop
# ------------------------------

async def main() -> None:
    vllm_host = os.environ.get("VLLM_HOST", "127.0.0.1")
    sub_port = os.environ.get("VLLM_SUB_PORT", "5557")
    redis_host = os.environ.get("REDIS_HOST", "redis")
    redis_port = int(os.environ.get("REDIS_PORT", "6379"))
    container_name = os.environ.get("CONTAINER_NAME", "vllm-pod")
    model_name = os.environ.get("MODEL_NAME", None)

    print(f"[{container_name}] Starting KV listener for model={model_name or 'unset'}")
    print(f"[{container_name}] vLLM ZMQ endpoint: tcp://{vllm_host}:{sub_port}")
    print(f"[{container_name}] Redis: {redis_host}:{redis_port}")

    # Redis client
    redis = aioredis.from_url(
        f"redis://{redis_host}:{redis_port}",
        decode_responses=True,
    )

    # ZMQ subscriber
    ctx = zmq.asyncio.Context()
    sub = ctx.socket(zmq.SUB)
    sub.connect(f"tcp://{vllm_host}:{sub_port}")
    # Subscribe to all KV topics (vLLM typically prefixes topics with 'kv@...')
    sub.setsockopt_string(zmq.SUBSCRIBE, "kv@")

    decoder = msgspec.msgpack.Decoder(type=KVEventBatch)
    print(f"[{container_name}] Listening for KV events...")

    reconnect_backoff = 1

    try:
        while True:
            try:
                # Message format: [topic, seq_bytes, payload]
                msg = await sub.recv_multipart()
                if len(msg) != 3:
                    print(f"[{container_name}] Unexpected ZMQ frame length: {len(msg)}")
                    continue

                topic, seq_bytes, payload = msg
                seq = int.from_bytes(seq_bytes, "big")

                event_batch = decoder.decode(payload)
                print(f"[{container_name}] Received batch seq={seq}")
                await process_event(event_batch, redis, container_name, model_name)
                reconnect_backoff = 1

            except KeyboardInterrupt:
                print(f"[{container_name}] Interrupted, shutting down.")
                break
            except zmq.ZMQError as e:
                print(f"[{container_name}] ZMQ error: {e}, retrying in {reconnect_backoff}s")
                await asyncio.sleep(reconnect_backoff)
                reconnect_backoff = min(reconnect_backoff * 2, 30)
            except Exception as e:
                print(f"[{container_name}] Unexpected error: {e}", file=sys.stderr)
                await asyncio.sleep(1)

    finally:
        await redis.close()
        sub.close()
        ctx.term()


if __name__ == "__main__":
    asyncio.run(main())
