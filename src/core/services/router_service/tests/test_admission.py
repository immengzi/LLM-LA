# tests/test_admission.py
# -*- coding: utf-8 -*-
"""Unit tests for admission control binary search (router.admission)."""
from router.admission import compute_max_safe_admit


class _StepTPOTPredictor:
    """Predicts TPOT proportional to concurrency, so there's a hard cap on N."""

    def __init__(self, per_request_s: float):
        self._c = per_request_s

    def predict_tpot(self, batch_size, accumulated_len):
        return self._c * batch_size


def test_returns_at_least_one_when_budget_tight():
    pred = _StepTPOTPredictor(1.0)  # 1s per concurrent request
    # Budget only fits ~ fewer than the current inflight, but we always admit 1.
    n = compute_max_safe_admit(pred, current_inflight=100, tpot_budget_s=0.5,
                               accumulated_len_avg=100)
    assert n == 1


def test_finds_largest_safe_n():
    pred = _StepTPOTPredictor(0.01)  # 10ms per concurrent request
    # budget 0.1s -> tpot ok while (inflight + N) * 0.01 <= 0.1 -> inflight+N <= 10
    n = compute_max_safe_admit(pred, current_inflight=2, tpot_budget_s=0.1,
                               accumulated_len_avg=100)
    assert (2 + n) <= 10
    assert (2 + n + 1) * 0.01 > 0.1  # n is maximal


def test_non_positive_budget_returns_one():
    pred = _StepTPOTPredictor(0.01)
    assert compute_max_safe_admit(pred, 0, 0.0, 100) == 1
    assert compute_max_safe_admit(pred, 0, -1.0, 100) == 1


def test_capped_by_max_search():
    pred = _StepTPOTPredictor(0.0)  # always 0 -> everything fits
    n = compute_max_safe_admit(pred, 0, 10.0, 100, max_search=32)
    assert n == 32
