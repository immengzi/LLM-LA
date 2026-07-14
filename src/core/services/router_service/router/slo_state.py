# router/slo_state.py
# -*- coding: utf-8 -*-
"""
Per-request SLO registry (in-memory, thread-safe).

Stores deadline timestamps, predictions, and actuals for every request
that carries SLO annotations.  Accessed on every pull scoring iteration
and every /result callback — must be fast.

Cleanup: entries are removed when results are ingested or when TTL expires.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from threading import RLock
from typing import Dict, Optional


@dataclass
class SLOEntry:
    """All SLO-relevant state for a single request."""

    req_id: str

    # SLO specification (set at enqueue)
    slo_type: Optional[str] = None          # "ttft" | "tpot" | "ttft+tpot" | "e2e" | None
    deadline_ttft: Optional[float] = None   # absolute epoch seconds
    deadline_tpot_s: Optional[float] = None # per-token budget in seconds (not a timestamp)
    deadline_e2e: Optional[float] = None    # absolute epoch seconds
    task_type: Optional[str] = None         # e.g. "chat", "summarize", "code"
    output_len_hint: Optional[int] = None   # client-supplied hint (takes priority over predictor)

    # Derived at enqueue
    arrival_ts: float = 0.0
    input_tokens: int = 0
    predicted_output_len: Optional[int] = None

    # Set at dispatch / scoring time (refreshed on each pull)
    predicted_ttft: Optional[float] = None
    predicted_tpot: Optional[float] = None
    predicted_e2e: Optional[float] = None
    slack: Optional[float] = None
    binding_constraint: Optional[str] = None  # "ttft" | "tpot" | None
    assigned_endpoint: Optional[str] = None
    dispatch_ts: Optional[float] = None

    # Set on result arrival
    actual_ttft: Optional[float] = None
    actual_output_len: Optional[int] = None
    actual_e2e: Optional[float] = None
    result_ts: Optional[float] = None


class SLORegistry:
    """
    Thread-safe in-memory registry for per-request SLO state.

    NOT stored in Redis (too hot — updated on every pull and result).
    """

    def __init__(self, ttl_s: float = 300.0):
        self._lock = RLock()
        self._entries: Dict[str, SLOEntry] = {}
        self._ttl_s = max(1.0, float(ttl_s))

    # -------------------------------------------------------
    # Registration
    # -------------------------------------------------------

    def register(
        self,
        req_id: str,
        *,
        slo_type: Optional[str] = None,
        slo_ttft_ms: Optional[float] = None,
        slo_tpot_ms: Optional[float] = None,
        slo_e2e_ms: Optional[float] = None,
        task_type: Optional[str] = None,
        output_len_hint: Optional[int] = None,
        input_tokens: int = 0,
        arrival_ts: Optional[float] = None,
    ) -> SLOEntry:
        now = arrival_ts or time.time()

        entry = SLOEntry(req_id=req_id, arrival_ts=now, input_tokens=input_tokens)
        entry.task_type = task_type
        entry.output_len_hint = output_len_hint

        if slo_type:
            slo_type = slo_type.strip().lower()
            entry.slo_type = slo_type

            if slo_type == "ttft" and slo_ttft_ms is not None:
                entry.deadline_ttft = now + slo_ttft_ms / 1000.0

            elif slo_type == "tpot" and slo_tpot_ms is not None:
                entry.deadline_tpot_s = slo_tpot_ms / 1000.0

            elif slo_type == "ttft+tpot":
                if slo_ttft_ms is not None:
                    entry.deadline_ttft = now + slo_ttft_ms / 1000.0
                if slo_tpot_ms is not None:
                    entry.deadline_tpot_s = slo_tpot_ms / 1000.0

            elif slo_type == "e2e" and slo_e2e_ms is not None:
                entry.deadline_e2e = now + slo_e2e_ms / 1000.0

        with self._lock:
            self._entries[req_id] = entry

        return entry

    # -------------------------------------------------------
    # Lookups
    # -------------------------------------------------------

    def get(self, req_id: str) -> Optional[SLOEntry]:
        with self._lock:
            return self._entries.get(req_id)

    def has_slo(self, req_id: str) -> bool:
        with self._lock:
            e = self._entries.get(req_id)
            return e is not None and e.slo_type is not None

    # -------------------------------------------------------
    # Updates
    # -------------------------------------------------------

    def set_predicted_output_len(self, req_id: str, tokens: int) -> None:
        with self._lock:
            e = self._entries.get(req_id)
            if e:
                e.predicted_output_len = tokens

    def set_input_tokens(self, req_id: str, tokens: int) -> None:
        with self._lock:
            e = self._entries.get(req_id)
            if e:
                e.input_tokens = tokens

    def update_dispatch(
        self,
        req_id: str,
        *,
        endpoint: str,
        predicted_ttft: Optional[float] = None,
        predicted_tpot: Optional[float] = None,
        predicted_e2e: Optional[float] = None,
        slack: Optional[float] = None,
        binding_constraint: Optional[str] = None,
    ) -> None:
        with self._lock:
            e = self._entries.get(req_id)
            if not e:
                return
            e.assigned_endpoint = endpoint
            e.dispatch_ts = time.time()
            e.predicted_ttft = predicted_ttft
            e.predicted_tpot = predicted_tpot
            e.predicted_e2e = predicted_e2e
            e.slack = slack
            e.binding_constraint = binding_constraint

    def ingest_result(
        self,
        req_id: str,
        *,
        actual_ttft: Optional[float] = None,
        actual_output_len: Optional[int] = None,
        actual_e2e: Optional[float] = None,
    ) -> Optional[SLOEntry]:
        """
        Record actuals from /result callback.  Returns the entry for
        callers that want to compute prediction error.
        """
        with self._lock:
            e = self._entries.get(req_id)
            if not e:
                return None
            e.result_ts = time.time()
            if actual_ttft is not None:
                e.actual_ttft = actual_ttft
            if actual_output_len is not None:
                e.actual_output_len = actual_output_len
            if actual_e2e is not None:
                e.actual_e2e = actual_e2e
            return e

    # -------------------------------------------------------
    # Cleanup
    # -------------------------------------------------------

    def remove(self, req_id: str) -> None:
        with self._lock:
            self._entries.pop(req_id, None)

    def cleanup_expired(self) -> int:
        """Remove entries older than TTL.  Returns count removed."""
        now = time.time()
        to_del = []
        with self._lock:
            for rid, e in self._entries.items():
                age = now - e.arrival_ts
                if age >= self._ttl_s:
                    to_del.append(rid)
            for rid in to_del:
                del self._entries[rid]
        return len(to_del)

    def size(self) -> int:
        with self._lock:
            return len(self._entries)

    # -------------------------------------------------------
    # Debug
    # -------------------------------------------------------

    def debug_entry(self, req_id: str) -> Optional[dict]:
        """Return a JSON-safe snapshot of an entry for the debug endpoint."""
        with self._lock:
            e = self._entries.get(req_id)
            if not e:
                return None
            from dataclasses import asdict
            return asdict(e)

    def debug_summary(self) -> dict:
        with self._lock:
            total = len(self._entries)
            with_slo = sum(1 for e in self._entries.values() if e.slo_type)
            by_type: Dict[str, int] = {}
            for e in self._entries.values():
                t = e.slo_type or "none"
                by_type[t] = by_type.get(t, 0) + 1
            return {"total": total, "with_slo": with_slo, "by_type": by_type}
