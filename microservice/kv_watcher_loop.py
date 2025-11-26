#!/usr/bin/env python3
"""
Periodic KV-cache watcher for Redis.

Continuously scans keys of the form:
    <MODEL>:kvblock:<hash>

and prints changes whenever they occur.
"""

import asyncio
import time
from typing import Dict

import redis.asyncio as aioredis

from config import get_config


# ----------------------
# Utility
# ----------------------
async def snapshot_kv(redis, model: str) -> Dict[str, Dict[str, str]]:
    """Return a dictionary: key -> {pod -> "1"}."""
    out: Dict[str, Dict[str, str]] = {}
    async for key in redis.scan_iter(match=f"{model}:kvblock:*"):
        mapping = await redis.hgetall(key)
        out[key] = mapping
    return out


def diff_snapshots(old: Dict, new: Dict) -> Dict:
    """Return only the differences between two KV snapshots."""
    changes = {}

    all_keys = set(old.keys()) | set(new.keys())
    for k in all_keys:
        if old.get(k) != new.get(k):
            changes[k] = {"old": old.get(k), "new": new.get(k)}

    return changes


# ----------------------
# Main watch loop
# ----------------------
async def watch_loop():
    cfg = get_config()

    model_name = cfg.MODEL_NAME
    redis_host = cfg.REDIS_HOST
    redis_port = int(cfg.REDIS_PORT)
    interval = float(getattr(cfg, "KV_WATCH_INTERVAL_S", 2.0))

    print(f"🔍 Watching Redis KV every {interval}s")
    print(f"Redis: {redis_host}:{redis_port}")
    print(f"Model: {model_name}")
    print("Press Ctrl+C to stop.\n")

    redis = aioredis.from_url(
        f"redis://{redis_host}:{redis_port}",
        decode_responses=True,
    )

    prev: Dict[str, Dict[str, str]] = {}

    while True:
        try:
            snap = await snapshot_kv(redis, model_name)

            if not prev:
                print("📥 Initial KV snapshot:")
                for k, v in snap.items():
                    print(f"  {k} -> {v}")
                print()
            else:
                changes = diff_snapshots(prev, snap)
                if changes:
                    print(f"\n🟦 KV changes detected at {time.strftime('%H:%M:%S')}:")
                    for k, delta in changes.items():
                        print(f"  {k}:")
                        print(f"     old = {delta['old']}")
                        print(f"     new = {delta['new']}")
                else:
                    print(f"⏳ No KV changes ({time.strftime('%H:%M:%S')})")

            prev = snap

        except Exception as e:
            print(f"❌ Error: {e}")

        await asyncio.sleep(interval)


# ----------------------
# Entrypoint
# ----------------------
if __name__ == "__main__":
    try:
        asyncio.run(watch_loop())
    except KeyboardInterrupt:
        print("\n👋 Exiting.")
