"""Redis helpers for KV-cache ownership."""

from typing import Any

from redis.exceptions import WatchError


_CLEAR_RETRIES = 8


def redis_key_prefix(model: str) -> str:
    return f"{model}:" if model else ""


def clear_pod_ownership(
    redis_client: Any,
    model: str,
    pod_name: str,
) -> None:
    """Atomically remove every block owned by a pod.

    WATCH closes the gap between reading podblocks and committing the deletes.
    This assumes the schema's normal single-writer-per-pod contract, while also
    failing safely if another writer changes the ownership set concurrently.
    """
    prefix = redis_key_prefix(model)
    podblocks_key = f"{prefix}podblocks:{pod_name}"
    for _attempt in range(_CLEAR_RETRIES):
        pipe = redis_client.pipeline(transaction=True)
        try:
            pipe.watch(podblocks_key)
            block_hashes = pipe.smembers(podblocks_key)
            pipe.multi()
            for block_hash in block_hashes:
                if isinstance(block_hash, bytes):
                    block_hash = block_hash.decode()
                pipe.hdel(f"{prefix}kvblock:{block_hash}", pod_name)
            pipe.delete(podblocks_key)
            pipe.execute()
            return
        except WatchError:
            continue
        finally:
            pipe.reset()
    raise RuntimeError(f"ownership changed while clearing {podblocks_key}")
