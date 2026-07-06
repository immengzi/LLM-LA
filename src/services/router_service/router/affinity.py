# router/affinity.py
# -*- coding: utf-8 -*-
"""
Conversation key-affinity for pull-based routing.

Keeps all turns of a single chat conversation on the same vLLM pod so that the
engine's internal prefix cache is reused across turns. This is a deliberately
simple heuristic that is entirely router-side: no BooM, sidecar, vLLM, or client
changes are required.

The affinity key is derived from the stable prefix of a conversation
(model + system prompt + first user message), which is identical across every
turn because each turn resends the full message history.
"""
import hashlib
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


def derive_affinity_key(model: str, messages: List[Dict[str, Any]]) -> Optional[str]:
    """
    Derive a stable per-conversation key from the model and the conversation's
    opening (system prompt + first user message).

    Returns None if there is no user message to key on (in which case the caller
    should skip affinity for this request).
    """
    if not messages:
        return None

    parts: List[str] = [f"model:{model}"]
    saw_user = False
    for m in messages:
        if not isinstance(m, dict):
            continue
        role = m.get("role", "")
        content = m.get("content", "")
        # OpenAI / Anthropic content-block arrays -> concatenate text parts.
        if isinstance(content, list):
            content = "".join(
                b.get("text", "")
                for b in content
                if isinstance(b, dict) and b.get("type") == "text"
            )
        elif content is None:
            content = ""
        if role == "system":
            parts.append(f"system:{content}")
        elif role == "user":
            parts.append(f"user:{content}")
            saw_user = True
            break  # first user message only -> stable across turns

    if not saw_user:
        return None

    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:16]


@dataclass
class AffinityEntry:
    endpoint: str
    last_seen: float  # time.monotonic()


class AffinityMap:
    """
    Thread-safe conversation-key -> endpoint map with TTL expiry.

    Lock ordering: this map's lock is always acquired while the caller already
    holds the RouterState lock; this class never calls back into RouterState, so
    the ordering is strict (RouterState -> AffinityMap) and deadlock-free.

    Optional durability: when a ``store`` (RedisAffinityStore-like) is provided,
    ``claim`` is write-through to the store (off the hot path via the store's
    background writer), ``warm`` bulk-loads the store into memory at startup,
    and ``prefetch`` does the single per-request Redis GET at admission on a
    memory miss. The pull hot path (``lookup``) stays purely in-memory. With no
    store, behavior is byte-for-byte identical to before.
    """

    def __init__(self, ttl_s: float, store=None, cache_max: int = 0):
        self._map: Dict[str, AffinityEntry] = {}
        self._ttl = float(ttl_s)
        self._lock = threading.Lock()
        self._store = store
        self._cache_max = int(cache_max or 0)

    @property
    def persistent(self) -> bool:
        return self._store is not None

    def _evict_if_needed_locked(self) -> None:
        """Bound the in-memory cache; evicted keys remain durable in the store."""
        if self._cache_max <= 0 or len(self._map) <= self._cache_max:
            return
        # Evict oldest-by-last_seen down to the bound (cheap; runs rarely).
        overflow = len(self._map) - self._cache_max
        for k, _e in sorted(self._map.items(), key=lambda kv: kv[1].last_seen)[:overflow]:
            del self._map[k]

    def lookup(self, key: str) -> Optional[str]:
        """Return the mapped endpoint if present and not expired, else None.

        In-memory only — safe to call repeatedly on the pull hot path. Use
        ``prefetch`` (once per request) to consult the durable store.
        """
        if not key:
            return None
        with self._lock:
            e = self._map.get(key)
            if e is None:
                return None
            if time.monotonic() - e.last_seen > self._ttl:
                del self._map[key]
                return None
            return e.endpoint

    def prefetch(self, key: str) -> Optional[str]:
        """Warm one key from the durable store into memory on an in-memory miss.

        Called once per request at admission. No-op (falls back to ``lookup``)
        when there is no store. Returns the resolved endpoint or None.
        """
        if not key:
            return None
        hit = self.lookup(key)
        if hit is not None or self._store is None:
            return hit
        ep = self._store.get(key)
        if ep:
            with self._lock:
                self._map[key] = AffinityEntry(ep, time.monotonic())
                self._evict_if_needed_locked()
            return ep
        return None

    def claim(self, key: str, endpoint: str) -> None:
        """Record (or refresh) that this conversation key is served by endpoint.

        Write-through to the durable store when one is configured (the store's
        writer is asynchronous, so this stays off the hot path).
        """
        if not key or not endpoint:
            return
        with self._lock:
            self._map[key] = AffinityEntry(endpoint, time.monotonic())
            self._evict_if_needed_locked()
        if self._store is not None:
            self._store.put(key, endpoint)

    def warm(self) -> int:
        """Bulk-load the durable store into the in-memory cache. Returns count."""
        if self._store is None:
            return 0
        loaded = self._store.warm()
        if not loaded:
            return 0
        now = time.monotonic()
        with self._lock:
            for k, ep in loaded.items():
                if ep:
                    self._map[k] = AffinityEntry(ep, now)
            self._evict_if_needed_locked()
            return len(self._map)

    def close(self) -> None:
        if self._store is not None:
            self._store.close()

    def prune(self) -> None:
        """Opportunistic TTL sweep to bound memory; safe to call often."""
        now = time.monotonic()
        with self._lock:
            stale = [k for k, e in self._map.items() if now - e.last_seen > self._ttl]
            for k in stale:
                del self._map[k]

    def size(self) -> int:
        with self._lock:
            return len(self._map)
