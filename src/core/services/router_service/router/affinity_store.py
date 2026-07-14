# router/affinity_store.py
# -*- coding: utf-8 -*-
"""
Durable backing store for the conversation key-affinity map.

The in-memory ``AffinityMap`` (see affinity.py) is the hot-path source of
truth. This module adds an *optional* Redis-backed persistence layer so the
``affinity_key -> endpoint(pod)`` decisions survive router restarts and full
redeploys:

  * write-through: every ``claim`` is queued to a background writer that
    pipelines ``SET`` into Redis (off the request/pull hot path);
  * warm-on-startup: ``warm()`` bulk-loads the existing namespace back into the
    in-memory cache so the router does not start empty;
  * read-on-arrival: ``get()`` is used once per request at admission (prefetch)
    on an in-memory miss, never per pull-scan iteration.

It reuses the same ``redis`` package, URL and colon-namespaced key conventions
already used by the KV watcher (kv_watcher.py); it does NOT stand up a second
Redis server or a parallel client abstraction. A synchronous client is used
(the pull path is synchronous under a lock) while the KV watcher keeps its
async client — they talk to the same Redis service.

Key schema:  ``{prefix}:{cluster}:{model}:{affinity_key}``  ->  endpoint
"""
from __future__ import annotations

import queue
import sys
import threading
from typing import Dict, Optional


def _log(msg: str) -> None:
    print(f"[AffinityStore] {msg}")
    sys.stdout.flush()


def build_redis_client(url: str):
    """Build a synchronous redis client from the shared package/URL convention.

    Returns None (and logs) if the redis package or the connection object
    cannot be constructed. Note: from_url is lazy and does not actually connect
    here, so failures surface later on the first command (handled gracefully).
    """
    try:
        import redis  # same package kv_watcher.py already depends on

        return redis.Redis.from_url(url, decode_responses=True)
    except Exception as e:  # pragma: no cover - defensive
        _log(f"failed to build redis client for {url!r}: {e!r}")
        return None


class RedisAffinityStore:
    """Redis persistence for affinity mappings with an off-path async writer.

    Parameters
    ----------
    client:
        A redis-py style client (duck-typed: ``get``, ``set``, ``scan_iter``,
        ``pipeline``, ``close``). Injectable so tests can pass a fake.
    namespace:
        Fully-resolved key namespace, e.g. ``"affinity:bz:served-model-minmax"``.
        The per-conversation key is appended as ``"{namespace}:{affinity_key}"``.
    ttl_seconds:
        Per-key expiry. 0 (or negative) means no expiry.
    writer_queue_max / writer_batch / writer_idle_s:
        Background-writer tunables. The writer batches queued upserts into a
        single pipeline; last-write-wins for the same key (matches
        claim-follows-latest-puller semantics).
    """

    def __init__(
        self,
        *,
        client,
        namespace: str,
        ttl_seconds: int = 0,
        writer_queue_max: int = 100000,
        writer_batch: int = 256,
        writer_idle_s: float = 0.2,
    ):
        self._client = client
        self._ns = namespace.rstrip(":")
        self._ttl = int(ttl_seconds or 0)
        self._batch = max(1, int(writer_batch))
        self._idle_s = max(0.01, float(writer_idle_s))

        self._q: "queue.Queue[tuple]" = queue.Queue(maxsize=max(1, int(writer_queue_max)))
        self._stop = threading.Event()
        self._dropped = 0

        self._writer = threading.Thread(target=self._writer_loop, name="affinity-writer", daemon=True)
        self._writer.start()
        _log(f"started (namespace={self._ns!r}, ttl_seconds={self._ttl})")

    # ---- key helpers -------------------------------------------------
    def _rkey(self, key: str) -> str:
        return f"{self._ns}:{key}"

    # ---- write path (off the hot path) ------------------------------
    def put(self, key: str, endpoint: str) -> None:
        """Queue a write-through upsert. Never blocks; drops+logs when full."""
        if not key or not endpoint:
            return
        try:
            self._q.put_nowait((key, endpoint))
        except queue.Full:
            self._dropped += 1
            if self._dropped % 1000 == 1:
                _log(f"writer queue full; dropped {self._dropped} upserts (Redis slow/down?)")

    def _writer_loop(self) -> None:
        while not self._stop.is_set():
            try:
                item = self._q.get(timeout=self._idle_s)
            except queue.Empty:
                continue
            # Coalesce a batch: last-write-wins per key within the batch.
            pending: Dict[str, str] = {}
            k, ep = item
            pending[k] = ep
            for _ in range(self._batch - 1):
                try:
                    k, ep = self._q.get_nowait()
                    pending[k] = ep
                except queue.Empty:
                    break
            self._flush(pending)

    def _flush(self, pending: Dict[str, str]) -> None:
        if not pending:
            return
        try:
            pipe = self._client.pipeline(transaction=False)
            for k, ep in pending.items():
                if self._ttl > 0:
                    pipe.set(self._rkey(k), ep, ex=self._ttl)
                else:
                    pipe.set(self._rkey(k), ep)
            pipe.execute()
        except Exception as e:
            # Log-and-continue: persistence is best-effort, never fatal.
            _log(f"flush of {len(pending)} upserts failed: {e!r}")

    # ---- read path ---------------------------------------------------
    def get(self, key: str) -> Optional[str]:
        """Fetch a single mapping (used at admission on an in-memory miss)."""
        if not key:
            return None
        try:
            v = self._client.get(self._rkey(key))
            if v is None:
                return None
            return v.decode() if isinstance(v, (bytes, bytearray)) else str(v)
        except Exception as e:
            _log(f"get({key!r}) failed: {e!r}")
            return None

    def warm(self) -> Dict[str, str]:
        """Scan the namespace and return the full ``key -> endpoint`` mapping.

        Used once at startup (and optionally on a refresh interval) to warm the
        in-memory cache. Log-and-continue on error (returns what was gathered).
        """
        out: Dict[str, str] = {}
        prefix = self._ns + ":"
        plen = len(prefix)
        try:
            for rk in self._client.scan_iter(match=prefix + "*", count=500):
                rk_s = rk.decode() if isinstance(rk, (bytes, bytearray)) else str(rk)
                if not rk_s.startswith(prefix):
                    continue
                try:
                    v = self._client.get(rk_s)
                except Exception:
                    continue
                if v is None:
                    continue
                ep = v.decode() if isinstance(v, (bytes, bytearray)) else str(v)
                out[rk_s[plen:]] = ep
            _log(f"warm complete: {len(out)} mappings loaded from namespace {self._ns!r}")
        except Exception as e:
            _log(f"warm failed after {len(out)} mappings: {e!r}")
        return out

    def close(self) -> None:
        self._stop.set()
        try:
            self._writer.join(timeout=2.0)
        except Exception:
            pass
        # Best-effort final flush of anything still queued.
        pending: Dict[str, str] = {}
        try:
            while True:
                k, ep = self._q.get_nowait()
                pending[k] = ep
        except queue.Empty:
            pass
        if pending:
            self._flush(pending)
        try:
            self._client.close()
        except Exception:
            pass
        _log("closed")


def build_affinity_store(cfg) -> Optional[RedisAffinityStore]:
    """Construct a RedisAffinityStore from router config, or None on failure.

    Namespace = ``{AFFINITY_REDIS_KEY_PREFIX}:{cluster}:{model}`` where cluster
    falls back to the k8s NAMESPACE when CLUSTER is empty.
    """
    url = f"redis://{cfg.REDIS_HOST}:{cfg.REDIS_PORT}"
    client = build_redis_client(url)
    if client is None:
        _log("persistence requested but no redis client could be built; disabling persistence")
        return None
    cluster = (getattr(cfg, "CLUSTER", "") or getattr(cfg, "NAMESPACE", "") or "default").strip()
    model = (getattr(cfg, "MODEL_NAME", "") or "model").strip()
    prefix = (getattr(cfg, "AFFINITY_REDIS_KEY_PREFIX", "affinity") or "affinity").strip()
    namespace = f"{prefix}:{cluster}:{model}"
    return RedisAffinityStore(
        client=client,
        namespace=namespace,
        ttl_seconds=int(getattr(cfg, "AFFINITY_REDIS_TTL_SECONDS", 0)),
    )
