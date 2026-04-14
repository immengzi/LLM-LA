# router/admission.py
# -*- coding: utf-8 -*-
"""
Admission control: limits how many requests the router returns per pull.

compute_max_safe_admit(latency_predictor, current_inflight, tpot_budget_s, accumulated_len_avg)
  -> int

Binary search for largest N such that
  predict_tpot(inflight + N, l_a_avg) <= tpot_budget_s.
"""

from __future__ import annotations

from .latency_predictor import LatencyPredictor


def compute_max_safe_admit(
    latency_predictor: LatencyPredictor,
    current_inflight: int,
    tpot_budget_s: float,
    accumulated_len_avg: int,
    *,
    max_search: int = 128,
) -> int:
    """
    Binary search for the largest N such that
    predict_tpot(current_inflight + N, accumulated_len_avg) <= tpot_budget_s.

    Returns at least 1 (always admit at least one request).
    """
    if tpot_budget_s <= 0:
        return 1

    lo, hi = 1, max_search
    best = 1

    while lo <= hi:
        mid = (lo + hi) // 2
        predicted = latency_predictor.predict_tpot(
            current_inflight + mid,
            max(1, accumulated_len_avg),
        )
        if predicted <= tpot_budget_s:
            best = mid
            lo = mid + 1
        else:
            hi = mid - 1

    return max(1, best)
