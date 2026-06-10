#!/usr/bin/env python3
# watch_redis_kv.py
#
# Live monitor for vLLM KV-block keys stored in Redis.
# Designed to run on your host machine while experiments run.
#
# Your Redis YAML exposes Redis via a NodePort service:
#   nodePort: 30079  (namespace vllm, service name redis)
#
# So from your host you typically connect with:
#   --host <k8s-node-ip> --port 30079
#
# If you use kubectl port-forward instead, use:
#   --host localhost --port 6379
#
# It watches keys like:  <model_name>:kvblock:<block_hash>
# Each key is a Redis hash where fields are pod names (owners).

from __future__ import annotations

import argparse
import sys
import time
from collections import Counter
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Any

import redis  # pip install redis


@dataclass(frozen=True)
class BlockState:
    owners: Tuple[str, ...]  # sorted
    nfields: int


def _now() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _shorten(s: str, n: int) -> str:
    s = str(s)
    return s if len(s) <= n else (s[: n - 1] + "…")


def _to_str(x: Any) -> str:
    if isinstance(x, (bytes, bytearray)):
        try:
            return x.decode("utf-8", errors="replace")
        except Exception:
            return repr(x)
    return str(x)


def _scan_kvblocks(
    r: redis.Redis,
    *,
    pattern: str,
    max_keys: int,
    scan_count: int,
) -> Dict[str, BlockState]:
    """
    Returns mapping: redis_key -> BlockState
    """
    out: Dict[str, BlockState] = {}
    cursor = 0
    seen = 0

    while True:
        cursor, keys = r.scan(cursor=cursor, match=pattern, count=scan_count)

        if keys:
            # Pipeline hgetall for speed
            pipe = r.pipeline(transaction=False)
            for k in keys:
                pipe.hgetall(k)
            vals = pipe.execute()

            for k, mapping in zip(keys, vals):
                if not mapping:
                    continue

                owners = sorted(_to_str(field) for field in mapping.keys())
                key_s = _to_str(k)

                out[key_s] = BlockState(owners=tuple(owners), nfields=len(mapping))

                seen += 1
                if seen >= max_keys:
                    return out

        if cursor == 0:
            break

    return out


def _diff_blocks(prev: Dict[str, BlockState], cur: Dict[str, BlockState]) -> List[str]:
    """
    Create human-friendly change lines.
    """
    lines: List[str] = []

    prev_keys = set(prev.keys())
    cur_keys = set(cur.keys())

    added_keys = sorted(cur_keys - prev_keys)
    removed_keys = sorted(prev_keys - cur_keys)
    common_keys = sorted(prev_keys & cur_keys)

    for k in added_keys:
        lines.append(f"+ key {_shorten(k, 80)} owners={list(cur[k].owners)}")

    for k in removed_keys:
        lines.append(f"- key {_shorten(k, 80)}")

    for k in common_keys:
        a = prev[k].owners
        b = cur[k].owners
        if a == b:
            continue
        a_set = set(a)
        b_set = set(b)
        added = sorted(b_set - a_set)
        removed = sorted(a_set - b_set)
        parts = []
        if added:
            parts.append(f"+owners={added}")
        if removed:
            parts.append(f"-owners={removed}")
        lines.append(f"* key {_shorten(k, 80)} " + " ".join(parts))

    return lines


def _summarize(cur: Dict[str, BlockState], *, top_n: int) -> str:
    """
    Build a compact summary string for current scan.
    """
    nkeys = len(cur)
    if nkeys == 0:
        return "keys=0"

    owner_counts = Counter()
    shared_counts = Counter()  # blocks with >=2 owners, >=3 owners, etc.
    owner_set = set()
    most_shared: List[Tuple[str, int]] = []

    for k, st in cur.items():
        owners = st.owners
        owner_set.update(owners)
        for o in owners:
            owner_counts[o] += 1
        most_shared.append((k, len(owners)))
        if len(owners) >= 2:
            shared_counts[len(owners)] += 1

    top_owners = owner_counts.most_common(top_n)
    top_owners_str = ", ".join([f"{o}:{c}" for o, c in top_owners]) if top_owners else "-"

    shared_str = ", ".join([f"{k}owners:{v}" for k, v in sorted(shared_counts.items())]) if shared_counts else "-"

    most_shared.sort(key=lambda x: x[1], reverse=True)
    top_shared = [(k, n) for (k, n) in most_shared[:top_n] if n >= 2]
    top_shared_str = ", ".join([f"{_shorten(k, 40)}({n})" for k, n in top_shared]) if top_shared else "-"

    return (
        f"keys={nkeys} unique_owners={len(owner_set)} "
        f"top_owners=[{top_owners_str}] "
        f"shared_blocks=[{shared_str}] "
        f"top_shared_blocks=[{top_shared_str}]"
    )


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Live watcher for Redis KV block ownership keys (vLLM kvblock:*)."
    )

    # For your YAML: NodePort is 30079
    ap.add_argument(
        "--host",
        default="localhost",
        help="Redis host. For NodePort: use a Kubernetes node IP. For port-forward: localhost.",
    )
    ap.add_argument(
        "--node-ip",
        default=None,
        help="Alias for --host (convenience when using NodePort).",
    )
    ap.add_argument(
        "--port",
        type=int,
        default=30079,
        help="Redis port. Your YAML uses NodePort 30079. For port-forward use 6379.",
    )
    ap.add_argument("--db", type=int, default=0, help="Redis DB (default: 0)")
    ap.add_argument("--password", default=None, help="Redis password (optional; your YAML sets none)")

    ap.add_argument(
        "--model",
        default="served-model",
        help="Model name prefix used in Redis keys (default: served-model). Pattern is <model>:kvblock:*",
    )
    ap.add_argument("--interval", type=float, default=1.0, help="Polling interval seconds (default: 1.0)")
    ap.add_argument("--max-keys", type=int, default=200, help="Max keys to include per tick (default: 200)")
    ap.add_argument("--scan-count", type=int, default=200, help="SCAN count hint (default: 200)")

    ap.add_argument("--show-diff", action="store_true", help="Print per-key diffs each tick")
    ap.add_argument("--show-full", action="store_true", help="Print full per-key owner lists each tick")
    ap.add_argument("--top-n", type=int, default=8, help="Top-N owners/blocks to show in summary (default: 8)")

    args = ap.parse_args()

    if args.node_ip:
        args.host = args.node_ip

    pattern = f"{args.model}:kvblock:*"

    r = redis.Redis(
        host=args.host,
        port=args.port,
        db=args.db,
        password=args.password,
        socket_timeout=2.0,
        socket_connect_timeout=2.0,
        decode_responses=False,  # keep bytes; we decode safely via _to_str()
    )

    try:
        r.ping()
    except Exception as e:
        print(
            f"[{_now()}] ERROR: cannot connect to redis at {args.host}:{args.port} db={args.db}: {e}",
            file=sys.stderr,
        )
        print(
            f"[{_now()}] HINT: With your YAML NodePort, use: --host <node-ip> --port 30079\n"
            f"[{_now()}]       Or port-forward: kubectl -n vllm port-forward svc/redis 6379:6379 "
            f"then use --host localhost --port 6379",
            file=sys.stderr,
        )
        return 2

    prev: Dict[str, BlockState] = {}
    tick = 0

    print(f"[{_now()}] Connected. Watching pattern='{pattern}' interval={args.interval}s max_keys={args.max_keys}")
    sys.stdout.flush()

    try:
        while True:
            t0 = time.time()
            tick += 1

            try:
                cur = _scan_kvblocks(
                    r,
                    pattern=pattern,
                    max_keys=max(1, int(args.max_keys)),
                    scan_count=max(1, int(args.scan_count)),
                )
            except Exception as e:
                print(f"[{_now()}] WARN: scan failed: {e}", file=sys.stderr)
                time.sleep(args.interval)
                continue

            summary = _summarize(cur, top_n=max(1, int(args.top_n)))
            dt = time.time() - t0
            print(f"[{_now()}] tick={tick} scan_s={dt:.3f} {summary}")

            if args.show_diff:
                lines = _diff_blocks(prev, cur)
                for ln in lines[:2000]:
                    print("  " + ln)
                if len(lines) > 2000:
                    print(f"  ... truncated {len(lines) - 2000} diff lines")

            if args.show_full:
                for k in sorted(cur.keys()):
                    st = cur[k]
                    print(f"  key={k} owners={list(st.owners)}")

            sys.stdout.flush()
            prev = cur

            sleep_s = args.interval - (time.time() - t0)
            if sleep_s > 0:
                time.sleep(sleep_s)

    except KeyboardInterrupt:
        print(f"\n[{_now()}] Stopped.")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
