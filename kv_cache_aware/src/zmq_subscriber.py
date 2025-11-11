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
# Async Redis Updater
# ------------------------------
async def process_event(event_batch: KVEventBatch, redis, pod_name: str, model_name: Optional[str] = None):
    """
    Async handler: update Redis mappings:
      - kvblocks{:{model}}  => block_hash -> pod_name
      - podblocks:{pod_name}{:{model}} => set of block_hashes
    """
    key_prefix = f"{model_name}:" if model_name else ""
    kvblocks_key = f"{key_prefix}kvblocks"
    podblocks_key = f"{key_prefix}podblocks:{pod_name}"

    print(f"[{pod_name}] ⏱ Event batch at {event_batch.ts:.3f}: {len(event_batch.events)} events")

    pipe = redis.pipeline(transaction=False)
    ts = int(time.time())

    for event in event_batch.events:
        try:
            if isinstance(event, BlockStored):
                for block_hash in event.block_hashes:
                    pipe.hset(kvblocks_key, str(block_hash), f"{pod_name}:{ts}")
                    pipe.sadd(podblocks_key, str(block_hash))
                print(f"  -> Stored {len(event.block_hashes)} hash->container mappings for '{pod_name}'.")

            elif isinstance(event, BlockRemoved):
                for block_hash in event.block_hashes:
                    pipe.hdel(kvblocks_key, str(block_hash))
                    pipe.srem(podblocks_key, str(block_hash))
                print(f"  -> Removed {len(event.block_hashes)} hash->container mappings for '{pod_name}'.")

            elif isinstance(event, AllBlocksCleared):
                # Clear both directions for this pod only
                pipe.delete(podblocks_key)
                # Optionally, clean kvblocks entries belonging to this pod
                # This is optional because O(n) scan; can be deferred to background cleanup
                # Example:
                # async for key, value in redis.hscan_iter(kvblocks_key):
                #     if value.startswith(pod_name):
                #         await redis.hdel(kvblocks_key, key)

        except Exception as e:
            print(f"⚠️ Error processing event: {e}", file=sys.stderr)

    await pipe.execute()
    print(f"✅ Redis updated for {pod_name}")


# ------------------------------
# Main async listener loop
# ------------------------------
async def main():
    vllm_host = os.environ.get("VLLM_HOST", "localhost")
    sub_port = os.environ.get("VLLM_SUB_PORT", "5557")
    redis_host = os.environ.get("REDIS_HOST", "localhost")
    container_name = os.environ.get("CONTAINER_NAME", "vllm-pod-1")
    model_name = os.environ.get("MODEL_NAME", None)

    print(f"🚀 Starting KV listener for {container_name} ({model_name or 'no-model'})")
    print(f"→ vLLM: tcp://{vllm_host}:{sub_port}")
    print(f"→ Redis: {redis_host}")

    # Redis
    redis = aioredis.from_url(f"redis://{redis_host}:6379", decode_responses=True)

    # ZMQ
    ctx = zmq.asyncio.Context()
    sub = ctx.socket(zmq.SUB)
    sub.connect(f"tcp://{vllm_host}:{sub_port}")
    sub.setsockopt_string(zmq.SUBSCRIBE, "kv@")

    decoder = msgspec.msgpack.Decoder(type=KVEventBatch)

    print("🧩 Listening for KV events...")

    reconnect_backoff = 1

    while True:
        try:
            msg = await sub.recv_multipart()
            _, seq_bytes, payload = msg
            seq = int.from_bytes(seq_bytes, "big")

            event_batch = decoder.decode(payload)
            await process_event(event_batch, redis, container_name, model_name)

            reconnect_backoff = 1  # reset on success

        except KeyboardInterrupt:
            print("🛑 Interrupted.")
            break
        except zmq.ZMQError as e:
            print(f"⚠️ ZMQ error: {e}, retrying in {reconnect_backoff}s")
            await asyncio.sleep(reconnect_backoff)
            reconnect_backoff = min(reconnect_backoff * 2, 30)
        except Exception as e:
            print(f"⚠️ Unexpected error: {e}", file=sys.stderr)
            await asyncio.sleep(1)

    await redis.close()
    sub.close()
    ctx.term()


if __name__ == "__main__":
    asyncio.run(main())
