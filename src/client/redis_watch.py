#!/usr/bin/env python3
"""
redis_watch.py
~~~~~~~~~~~~~~~
Background collector that watches the KV-block ownership Redis writes during a
run and appends a per-tick summary to ``redis_kv_watch.jsonl`` in the experiment
directory (started/stopped by main.py, mirroring the Prometheus collector).

It periodically SCANs ``<model>:kvblock:*`` -- Redis hashes whose fields are the
owning vLLM pods -- and records, per tick: the number of blocks (kv_blocks), the
number of unique owners, the shared-block distribution (blocks owned by 2, 3,
... pods), the top owners by block count, and a bounded KV-cache *snapshot* --
the block-hash -> owner-pods map (capped at snapshot_max_blocks). This gives a
timeline of how KV ownership builds up and spreads across pods under load,
alongside the router's own kv_hit truth in router_logs.json.

Promoted from debug/watch_redis_kv.py; connects to Redis directly (NodePort or
port-forward). Fully best-effort: any connection/scan failure is logged and the
run continues.
"""
from __future__ import annotations

import json
import threading
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


def _to_str(x: Any) -> str:
    if isinstance(x, (bytes, bytearray)):
        try:
            return x.decode("utf-8", errors="replace")
        except Exception:
            return repr(x)
    return str(x)


class RedisKVWatcher:
    """Background poller for ``<model>:kvblock:*`` ownership in Redis."""

    def __init__(
        self,
        *,
        out_path: str | Path,
        host: str,
        port: int,
        model: str,
        db: int = 0,
        password: Optional[str] = None,
        interval_s: float = 2.0,
        max_keys: int = 5000,
        scan_count: int = 500,
        top_n: int = 10,
        snapshot_max_blocks: int = 200,
        snapshot_mode: str = "full",
        snapshot_full_every_n_ticks: int = 0,
    ):
        self._out_path = Path(out_path)
        self._host = host
        self._port = int(port)
        self._model = model
        self._db = int(db)
        self._password = password
        self._interval_s = max(0.5, float(interval_s))
        self._max_keys = max(1, int(max_keys))
        self._scan_count = max(1, int(scan_count))
        self._top_n = max(1, int(top_n))
        self._snapshot_max_blocks = max(0, int(snapshot_max_blocks))
        self._snapshot_mode = str(snapshot_mode or "full").strip().lower()
        if self._snapshot_mode not in ("full", "delta"):
            self._snapshot_mode = "full"
        self._snapshot_full_every_n_ticks = max(0, int(snapshot_full_every_n_ticks))
        # Previous scan (block-hash -> sorted owner tuple) for delta snapshots.
        self._prev: Dict[str, Tuple[str, ...]] = {}
        self._pattern = f"{model}:kvblock:*"
        self._key_prefix = f"{model}:kvblock:"

        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._fh: Optional[Any] = None
        self._redis: Optional[Any] = None
        self._tick = 0

    # ------------------------------------------------------------------
    def start(self) -> bool:
        """Connect + start the background thread. Returns True on success.

        Best-effort: on any failure it prints a warning and returns False
        without raising, so the run proceeds without the watcher.
        """
        try:
            import redis  # type: ignore
        except Exception as e:
            print(f"[redis-watch] disabled: python 'redis' package not available ({e})")
            return False

        try:
            self._redis = redis.Redis(
                host=self._host,
                port=self._port,
                db=self._db,
                password=self._password,
                socket_timeout=3.0,
                socket_connect_timeout=3.0,
                decode_responses=False,
            )
            self._redis.ping()
        except Exception as e:
            print(
                f"[redis-watch] disabled: cannot connect to redis at "
                f"{self._host}:{self._port} db={self._db}: {e}"
            )
            self._redis = None
            return False

        try:
            self._out_path.parent.mkdir(parents=True, exist_ok=True)
            self._fh = open(self._out_path, "a", encoding="utf-8")
        except Exception as e:
            print(f"[redis-watch] disabled: cannot open {self._out_path}: {e}")
            self._redis = None
            return False

        self._thread = threading.Thread(
            target=self._run, name="redis-kv-watcher", daemon=True
        )
        self._thread.start()
        print(
            f"[redis-watch] started -> {self._out_path} "
            f"(pattern={self._pattern!r} interval={self._interval_s}s)"
        )
        return True

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self._interval_s + 5.0)
            self._thread = None
        # One last scan so the tail of the run is captured.
        try:
            self._tick_once()
        except Exception:
            pass
        if self._fh is not None:
            try:
                self._fh.close()
            except Exception:
                pass
            self._fh = None

    # ------------------------------------------------------------------
    def _scan(self) -> Dict[str, Tuple[str, ...]]:
        """Return redis_key -> sorted tuple of owner pod names."""
        out: Dict[str, Tuple[str, ...]] = {}
        cursor = 0
        seen = 0
        assert self._redis is not None
        while True:
            cursor, keys = self._redis.scan(
                cursor=cursor, match=self._pattern, count=self._scan_count
            )
            if keys:
                pipe = self._redis.pipeline(transaction=False)
                for k in keys:
                    pipe.hkeys(k)
                vals = pipe.execute()
                for k, owners in zip(keys, vals):
                    if not owners:
                        continue
                    out[_to_str(k)] = tuple(sorted(_to_str(o) for o in owners))
                    seen += 1
                    if seen >= self._max_keys:
                        return out
            if cursor == 0:
                break
        return out

    def _tick_once(self) -> None:
        if self._redis is None or self._fh is None:
            return
        self._tick += 1
        t0 = time.time()
        cur = self._scan()
        scan_s = time.time() - t0

        owner_counts: Counter = Counter()
        shared_counts: Counter = Counter()  # #owners -> #blocks
        owner_set = set()
        for _k, owners in cur.items():
            owner_set.update(owners)
            for o in owners:
                owner_counts[o] += 1
            if len(owners) >= 2:
                shared_counts[len(owners)] += 1

        summary = {
            "ts": t0,
            "tick": self._tick,
            "scan_s": round(scan_s, 4),
            "keys": len(cur),
            "kv_blocks": len(cur),          # blocks currently owned in Redis
            "unique_owners": len(owner_set),
            "shared_blocks": {str(k): int(v) for k, v in sorted(shared_counts.items())},
            "top_owners": [[o, int(c)] for o, c in owner_counts.most_common(self._top_n)],
        }

        # KV-cache snapshot: the block-hash -> owner-pods map so the log carries
        # the actual cache contents (the "kv_block" detail), not just aggregate
        # counts. snapshot_max_blocks == 0 disables the block-level detail.
        #   "full"  -> dump the whole (bounded) map every tick. Simple but bulky:
        #              the mostly-static map is rewritten every interval, so the
        #              file grows as map_size x ticks.
        #   "delta" -> dump only blocks added/changed/removed vs the previous
        #              tick. The first tick (and every snapshot_full_every_n_ticks
        #              tick) is a full "baseline"; reconstruct the full state at
        #              any tick by taking the last baseline and replaying the
        #              deltas after it. File grows with churn, not total size.
        if self._snapshot_max_blocks != 0 and cur:
            cur_bh: Dict[str, Tuple[str, ...]] = {}
            for k, owners in cur.items():
                bh = k[len(self._key_prefix):] if k.startswith(self._key_prefix) else k
                cur_bh[bh] = owners

            if self._snapshot_mode == "delta":
                is_baseline = self._tick == 1 or (
                    self._snapshot_full_every_n_ticks > 0
                    and self._tick % self._snapshot_full_every_n_ticks == 0
                )
                if is_baseline:
                    keys = sorted(cur_bh.keys())[: self._snapshot_max_blocks]
                    summary["snapshot_kind"] = "baseline"
                    summary["blocks"] = {bh: list(cur_bh[bh]) for bh in keys}
                    summary["blocks_truncated"] = len(cur_bh) > self._snapshot_max_blocks
                else:
                    added: Dict[str, list] = {}
                    changed: Dict[str, list] = {}
                    for bh, owners in cur_bh.items():
                        prev_owners = self._prev.get(bh)
                        if prev_owners is None:
                            added[bh] = list(owners)
                        elif prev_owners != owners:
                            changed[bh] = list(owners)
                    removed = [bh for bh in self._prev if bh not in cur_bh]
                    summary["snapshot_kind"] = "delta"
                    summary["blocks_added"] = added
                    summary["blocks_changed"] = changed
                    summary["blocks_removed"] = removed
                self._prev = cur_bh
            else:
                keys = sorted(cur_bh.keys())[: self._snapshot_max_blocks]
                summary["blocks"] = {bh: list(cur_bh[bh]) for bh in keys}
                summary["blocks_truncated"] = len(cur_bh) > self._snapshot_max_blocks
        try:
            self._fh.write(json.dumps(summary, ensure_ascii=False) + "\n")
            self._fh.flush()
        except Exception:
            pass

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._tick_once()
            except Exception as e:
                print(f"[redis-watch] scan error: {e}")
            self._stop.wait(self._interval_s)
