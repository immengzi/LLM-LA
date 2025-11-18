#!/usr/bin/env python3
"""
Periodic KV-cache watcher for Redis.

Continuously scans keys of the form:
    <MODEL>:kvblock:<hash>

and prints changes whenever they occur.
"""

import asyncio
import os
import time
from typing import Dict

import redis.asyncio as aioredis


# ----------------------
# Config defaults
# ----------------------
RUNNING_IN_CLUSTER = os.getenv("KUBERNETES_SERVICE_HOST") is not None

if RUNNING_IN_CLUSTER:
    DEFAULT_REDIS_HOST = "redis.vllm.svc.cluster.local"
    DEFAULT_REDIS_PORT = 6379
else:
    DEFAULT_REDIS_HOST = "127.0.0.1"
    DEFAULT_REDIS_PORT = 30079

MODEL_NAME = os.getenv("MODEL_NAME", "served-model")
REDIS_HOST = os.getenv("REDIS_HOST", DEFAULT_REDIS_HOST)
REDIS_PORT = int(os.getenv("REDIS_PORT", DEFAULT_REDIS_PORT))

INTERVAL = float(os.getenv("KV_WATCH_INTERVAL", "2.0"))  # seconds


# ----------------------
# Utility
# ----------------------
async def snapshot_kv(redis, model: str) -> Dict[str, Dict[str, str]]:
    """Return a dictionary: key -> {pod -> "1"}."""
    out = {}
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
    print(f"🔍 Watching Redis KV every {INTERVAL}s")
    print(f"Redis: {REDIS_HOST}:{REDIS_PORT}")
    print(f"Model: {MODEL_NAME}")
    print("Press Ctrl+C to stop.\n")

    redis = aioredis.from_url(
        f"redis://{REDIS_HOST}:{REDIS_PORT}",
        decode_responses=True,
    )

    prev = {}

    while True:
        try:
            snap = await snapshot_kv(redis, MODEL_NAME)

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

        await asyncio.sleep(INTERVAL)


# ----------------------
# Entrypoint
# ----------------------
if __name__ == "__main__":
    try:
        asyncio.run(watch_loop())
    except KeyboardInterrupt:
        print("\n👋 Exiting.")
