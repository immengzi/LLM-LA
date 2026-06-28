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
    """

    def __init__(self, ttl_s: float):
        self._map: Dict[str, AffinityEntry] = {}
        self._ttl = float(ttl_s)
        self._lock = threading.Lock()

    def lookup(self, key: str) -> Optional[str]:
        """Return the mapped endpoint if present and not expired, else None."""
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

    def claim(self, key: str, endpoint: str) -> None:
        """Record (or refresh) that this conversation key is served by endpoint."""
        if not key or not endpoint:
            return
        with self._lock:
            self._map[key] = AffinityEntry(endpoint, time.monotonic())

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
