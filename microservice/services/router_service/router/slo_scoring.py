# router/slo_scoring.py
# -*- coding: utf-8 -*-
"""
Slack computation for SLO-aware pull routing.

compute_slack() is called for each (request, endpoint) pair during pull scoring.
It returns a float slack value:
  - positive: request is ahead of schedule
  - zero: request is exactly at deadline
  - negative: request is predicted to miss SLO
  - +inf: no SLO annotation (lowest priority)

Dependencies: slo_state, latency_predictor, kv_aware.
"""

from __future__ import annotations

import math
import time
from collections import deque
from threading import RLock
from typing import Dict, Optional, Tuple

from .slo_state import SLORegistry, SLOEntry
from .latency_predictor import LatencyPredictor
from .kv_aware import prefix_len


# -------------------------------------------------------
# Batch size estimation
# -------------------------------------------------------

class BatchSizeEstimator:
    """Provides batch_size estimates for slack computation."""

    def __init__(self, mode: str = "fixed", fixed_value: int = 8):
        self._mode = mode
        self._fixed = max(1, fixed_value)
        self._lock = RLock()
        self._inflight: Dict[str, int] = {}          # endpoint -> inflight count
        self._reported: Dict[str, int] = {}           # endpoint -> sidecar-reported batch

    def estimate(self, endpoint: str) -> int:
        if self._mode == "inflight":
            with self._lock:
                return max(1, self._inflight.get(endpoint, self._fixed))
        elif self._mode == "reported":
            with self._lock:
                return max(1, self._reported.get(endpoint, self._fixed))
        return self._fixed

    def increment_inflight(self, endpoint: str, n: int = 1) -> None:
        with self._lock:
            self._inflight[endpoint] = self._inflight.get(endpoint, 0) + n

    def decrement_inflight(self, endpoint: str, n: int = 1) -> None:
        with self._lock:
            cur = self._inflight.get(endpoint, 0)
            self._inflight[endpoint] = max(0, cur - n)

    def get_inflight(self, endpoint: str) -> int:
        with self._lock:
            return self._inflight.get(endpoint, 0)

    def set_reported(self, endpoint: str, batch_size: int) -> None:
        with self._lock:
            self._reported[endpoint] = max(1, batch_size)


# -------------------------------------------------------
# Queue wait estimation
# -------------------------------------------------------

class QueueWaitEstimator:
    """Estimates time a request will wait in vLLM's internal queue after pull."""

    def __init__(self, mode: str = "none"):
        self._mode = mode
        self._lock = RLock()
        # Per-endpoint: rolling average of inter-completion time
        self._inter_completion: Dict[str, float] = {}
        self._last_completion_ts: Dict[str, float] = {}

    def estimate(self, endpoint: str, queue_position: int = 0) -> float:
        if self._mode == "none":
            return 0.0

        if self._mode == "simple":
            with self._lock:
                avg_ict = self._inter_completion.get(endpoint, 0.0)
            return max(0.0, queue_position * avg_ict)

        return 0.0

    def record_completion(self, endpoint: str) -> None:
        """Called on /result to update inter-completion time estimate."""
        now = time.time()
        with self._lock:
            last = self._last_completion_ts.get(endpoint)
            if last is not None:
                ict = now - last
                old_avg = self._inter_completion.get(endpoint, ict)
                self._inter_completion[endpoint] = 0.9 * old_avg + 0.1 * ict
            self._last_completion_ts[endpoint] = now


# -------------------------------------------------------
# Core slack computation
# -------------------------------------------------------

def compute_slack(
    entry: SLOEntry,
    endpoint: str,
    *,
    latency_predictor: LatencyPredictor,
    batch_size: int,
    cached_tokens: int = 0,
    queue_wait_s: float = 0.0,
    block_size_tokens: int = 16,
) -> Tuple[float, Optional[str]]:
    """
    Compute slack for a (request, endpoint) pair.

    Returns (slack_seconds, binding_constraint).
    binding_constraint is "ttft" | "tpot" | None.

    slack = deadline - predicted_completion_time
    Positive = ahead of schedule. Negative = predicted miss.
    +inf = no SLO.
    """
    if entry.slo_type is None:
        return (float("inf"), None)

    now = time.time()
    input_tokens = max(1, entry.input_tokens)
    l_cached = max(0, cached_tokens) * block_size_tokens
    predicted_output = entry.predicted_output_len or entry.output_len_hint or 256

    slo_type = entry.slo_type

    if slo_type == "ttft":
        if entry.deadline_ttft is None:
            return (float("inf"), None)
        predicted_ttft = latency_predictor.predict_ttft(input_tokens, l_cached, batch_size)
        predicted_completion = now + queue_wait_s + predicted_ttft
        slack = entry.deadline_ttft - predicted_completion
        return (slack, "ttft")

    elif slo_type == "tpot":
        if entry.deadline_tpot_s is None:
            return (float("inf"), None)
        avg_accum = input_tokens + predicted_output // 2
        predicted_tpot = latency_predictor.predict_tpot(batch_size, avg_accum)
        slack = entry.deadline_tpot_s - predicted_tpot
        return (slack, "tpot")

    elif slo_type == "ttft+tpot":
        ttft_slack = float("inf")
        tpot_slack = float("inf")

        if entry.deadline_ttft is not None:
            predicted_ttft = latency_predictor.predict_ttft(input_tokens, l_cached, batch_size)
            predicted_completion = now + queue_wait_s + predicted_ttft
            ttft_slack = entry.deadline_ttft - predicted_completion

        if entry.deadline_tpot_s is not None:
            avg_accum = input_tokens + predicted_output // 2
            predicted_tpot = latency_predictor.predict_tpot(batch_size, avg_accum)
            tpot_slack = entry.deadline_tpot_s - predicted_tpot

        slack = min(ttft_slack, tpot_slack)
        binding = "ttft" if ttft_slack <= tpot_slack else "tpot"
        return (slack, binding)

    elif slo_type == "e2e":
        if entry.deadline_e2e is None:
            return (float("inf"), None)
        predicted_e2e = latency_predictor.predict_e2e(
            input_tokens, l_cached, predicted_output, batch_size,
        )
        predicted_completion = now + queue_wait_s + predicted_e2e
        slack = entry.deadline_e2e - predicted_completion

        # Infer binding constraint from latency breakdown
        predicted_ttft = latency_predictor.predict_ttft(input_tokens, l_cached, batch_size)
        avg_accum = input_tokens + predicted_output // 2
        predicted_decode_total = predicted_output * latency_predictor.predict_tpot(batch_size, avg_accum)
        binding = "ttft" if predicted_ttft >= predicted_decode_total else "tpot"
        return (slack, binding)

    return (float("inf"), None)


def compute_slack_for_pool(
    pool: list,
    endpoint: str,
    *,
    slo_registry: SLORegistry,
    latency_predictor: LatencyPredictor,
    batch_estimator: BatchSizeEstimator,
    queue_wait_estimator: QueueWaitEstimator,
    block_size_tokens: int = 16,
) -> list:
    """
    Compute slack for every item in a pool.

    pool: list of (req_id, prompt, t_enq, meta)
    Returns: list of (req_id, prompt, t_enq, meta, slack, binding_constraint, cached_tokens)
    """
    batch_size = batch_estimator.estimate(endpoint)
    results = []

    for i, (rid, prompt, ts, meta) in enumerate(pool):
        entry = slo_registry.get(rid)
        if entry is None:
            results.append((rid, prompt, ts, meta, float("inf"), None, 0))
            continue

        cached_blocks = prefix_len(endpoint, rid)
        queue_wait = queue_wait_estimator.estimate(endpoint, queue_position=i)

        slack, binding = compute_slack(
            entry,
            endpoint,
            latency_predictor=latency_predictor,
            batch_size=batch_size,
            cached_tokens=cached_blocks,
            queue_wait_s=queue_wait,
            block_size_tokens=block_size_tokens,
        )

        results.append((rid, prompt, ts, meta, slack, binding, cached_blocks))

    return results
