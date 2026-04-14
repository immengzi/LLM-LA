# zmq_subscriber.py

import asyncio
import os
import sys
import time
import zmq
import zmq.asyncio
import msgspec
from redis import asyncio as aioredis
from typing import Any, Optional, Union, List, NewType

# ------------------------------
# Type Definitions
# ------------------------------
BlockHash = NewType("BlockHash", int)

class EventBatch(msgspec.Struct, array_like=True, omit_defaults=True, gc=False):
    ts: float
    events: list[Any]

class KVCacheEvent(msgspec.Struct, array_like=True, omit_defaults=True, gc=False, tag=True):
    pass

class BlockStored(KVCacheEvent):
    block_hashes: list[BlockHash]
    parent_block_hash: Optional[BlockHash]
    token_ids: list[int]
    block_size: int
    lora_id: Optional[int]
    medium: Optional[str]

class BlockRemoved(KVCacheEvent):
    block_hashes: list[BlockHash]
    medium: Optional[str]

class AllBlocksCleared(KVCacheEvent):
    pass

class KVEventBatch(EventBatch):
    events: list[Union[BlockStored, BlockRemoved, AllBlocksCleared]]

# ------------------------------
# Redis Updater
# ------------------------------
async def process_event(event_batch: KVEventBatch, redis, pod_name: str, model_name: Optional[str] = None):
    """
    Update Redis for KV cache events.

    Redis schema:
      - kvblocks:{model}               -> HSET(block_hash -> "kvblock:{block_hash}")
      - kvblock:{block_hash}           -> HSET(pod_name -> timestamp)
      - podblocks:{model}:{pod_name}   -> SET(block_hashes held by pod)
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

                print(f"Stored {len(event.block_hashes)} block→pod hash mappings for '{pod_name}'")

            # ----------------------
            # BlockRemoved event
            # ----------------------
            elif isinstance(event, BlockRemoved):
                for block_hash in event.block_hashes:
                    kvblock_key = f"{key_prefix}kvblock:{block_hash}"
                    pipe.hdel(kvblock_key, pod_name)
                    pipe.srem(podblocks_key, str(block_hash))

                print(f"Removed {len(event.block_hashes)} block→pod hash mappings for '{pod_name}'")

            # ----------------------
            # AllBlocksCleared event
            # ----------------------
            elif isinstance(event, AllBlocksCleared):
                print(f"Clearing all blocks for {pod_name}")

                async for block_hash in redis.sscan_iter(podblocks_key):
                    kvblock_key = f"{key_prefix}kvblock:{block_hash}"
                    pipe.hdel(kvblock_key, pod_name)

                pipe.delete(podblocks_key)

        except Exception as e:
            print(f"⚠️ Error processing event: {e}", file=sys.stderr)

    await pipe.execute()
    print(f"Redis updated for {pod_name}")


# ------------------------------
# Main loop
# ------------------------------
async def main():
    vllm_host = os.environ.get("VLLM_HOST", "localhost")
    sub_port = os.environ.get("VLLM_SUB_PORT", "5557")
    redis_host = os.environ.get("REDIS_HOST", "localhost")
    container_name = os.environ.get("CONTAINER_NAME", "vllm-pod-1")
    model_name = os.environ.get("MODEL_NAME", None)

    print(f"Starting KV listener for {container_name} ({model_name or 'no-model'})")
    print(f"vLLM: tcp://{vllm_host}:{sub_port}")
    print(f"Redis: {redis_host}")

    redis = aioredis.from_url(f"redis://{redis_host}:6379", decode_responses=True)

    ctx = zmq.asyncio.Context()
    sub = ctx.socket(zmq.SUB)
    sub.connect(f"tcp://{vllm_host}:{sub_port}")
    sub.setsockopt_string(zmq.SUBSCRIBE, "kv@")

    decoder = msgspec.msgpack.Decoder(type=KVEventBatch)
    print("Listening for KV events...")

    reconnect_backoff = 1

    while True:
        try:
            msg = await sub.recv_multipart()
            _, seq_bytes, payload = msg
            seq = int.from_bytes(seq_bytes, "big")

            event_batch = decoder.decode(payload)
            await process_event(event_batch, redis, container_name, model_name)
            reconnect_backoff = 1

        except KeyboardInterrupt:
            print("Interrupted.")
            break
        except zmq.ZMQError as e:
            print(f"ZMQ error: {e}, retrying in {reconnect_backoff}s")
            await asyncio.sleep(reconnect_backoff)
            reconnect_backoff = min(reconnect_backoff * 2, 30)
        except Exception as e:
            print(f"Unexpected error: {e}", file=sys.stderr)
            await asyncio.sleep(1)

    await redis.close()
    sub.close()
    ctx.term()


if __name__ == "__main__":
    asyncio.run(main())
