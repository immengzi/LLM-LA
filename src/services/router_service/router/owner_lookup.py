# -*- coding: utf-8 -*-
"""
Targeted, on-demand KV-block ownership lookup for prefix routing.

Instead of blindly scanning the whole Redis keyspace (see kv_watcher.py), this
resolves ownership for exactly the block hashes of the request being routed.
The per-pod sidecar keeps ``{model}:kvblock:{hash}`` fresh -- it applies
``BlockRemoved`` with ``hdel`` on eviction -- so a direct ``HGETALL`` of a
request's own leading blocks yields exact, current owners without the sampling
lag of the background scan.

Ownership is keyed by pod name, matching how the sidecar writes its own
identity and how the router already treats endpoint identity (see
kv_watcher._endpoint_for_pod).
"""
from __future__ import annotations

from typing import Dict, List, Optional, Set

import redis.asyncio as aioredis

from .config import get_config

_cfg = get_config()

_redis: Optional[aioredis.Redis] = None


async def init_owner_lookup() -> None:
    """Create the shared async Redis client (idempotent)."""
    global _redis
    if _redis is not None:
        return
    url = f"redis://{_cfg.REDIS_HOST}:{_cfg.REDIS_PORT}"
    _redis = aioredis.from_url(url, decode_responses=True)


async def close_owner_lookup() -> None:
    global _redis
    if _redis is not None:
        try:
            await _redis.close()
        except Exception:
            pass
        _redis = None


def _key_prefix() -> str:
    # Single-model deployments use MODEL_NAME as the Redis key prefix, matching
    # kv_watcher's scan pattern and the sidecar's MODEL_NAME_REDIS. Multi-model
    # setups would need the per-model prefix from the model registry.
    model = _cfg.MODEL_NAME
    return f"{model}:" if model else ""


async def fetch_block_owners(block_hashes: List[int]) -> Dict[int, Set[str]]:
    """Return ``{block_hash: {owner_pod, ...}}`` for a request's leading blocks.

    Only the leading prefix matters for routing, so the fan-out is capped at
    ``KV_LOOKUP_MAX_BLOCKS``. Returns an empty dict on any error or when the
    client is not initialised; the caller then falls back to affinity / no KV
    credit, so a Redis hiccup can never break dispatch.
    """
    if not block_hashes or _redis is None:
        return {}

    cap = int(getattr(_cfg, "KV_LOOKUP_MAX_BLOCKS", 512))
    if cap > 0:
        block_hashes = block_hashes[:cap]

    prefix = _key_prefix()
    try:
        pipe = _redis.pipeline(transaction=False)
        for h in block_hashes:
            pipe.hgetall(f"{prefix}kvblock:{h}")
        results = await pipe.execute()
    except Exception:
        return {}

    owners: Dict[int, Set[str]] = {}
    for h, mapping in zip(block_hashes, results):
        if mapping:
            owners[h] = set(mapping.keys())
    return owners
