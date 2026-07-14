# tests/test_slo_scoring.py
# -*- coding: utf-8 -*-
"""Unit tests for slack computation & estimators (router.slo_scoring)."""
import time

from router.slo_scoring import BatchSizeEstimator, QueueWaitEstimator, compute_slack
from router.slo_state import SLOEntry


class _FixedLatencyPredictor:
    """Deterministic predictor for slack math."""

    def predict_ttft(self, input_tokens, cached_tokens, batch_size):
        return 0.1

    def predict_tpot(self, batch_size, accumulated_len):
        return 0.02

    def predict_e2e(self, input_tokens, cached_tokens, output_tokens, batch_size):
        return 0.1 + output_tokens * 0.02


def test_batch_estimator_modes():
    est = BatchSizeEstimator(mode="fixed", fixed_value=8)
    assert est.estimate("ep") == 8

    est_inflight = BatchSizeEstimator(mode="inflight", fixed_value=4)
    assert est_inflight.estimate("ep") == 4  # default before any inflight
    est_inflight.increment_inflight("ep", 3)
    assert est_inflight.estimate("ep") == 3
    est_inflight.decrement_inflight("ep", 2)
    assert est_inflight.get_inflight("ep") == 1

    est_reported = BatchSizeEstimator(mode="reported", fixed_value=2)
    est_reported.set_reported("ep", 12)
    assert est_reported.estimate("ep") == 12


def test_queue_wait_estimator_none_is_zero():
    est = QueueWaitEstimator(mode="none")
    assert est.estimate("ep", queue_position=5) == 0.0


def test_queue_wait_estimator_simple_scales_with_position():
    est = QueueWaitEstimator(mode="simple")
    # No completion history yet -> 0.
    assert est.estimate("ep", queue_position=3) == 0.0
    est.record_completion("ep")
    time.sleep(0.01)
    est.record_completion("ep")
    assert est.estimate("ep", queue_position=2) >= 0.0


def test_compute_slack_no_slo_is_inf():
    entry = SLOEntry(req_id="r1", slo_type=None)
    slack, binding = compute_slack(entry, "ep",
                                   latency_predictor=_FixedLatencyPredictor(),
                                   batch_size=8)
    assert slack == float("inf")
    assert binding is None


def test_compute_slack_ttft_positive_when_ahead():
    now = time.time()
    entry = SLOEntry(req_id="r1", slo_type="ttft", input_tokens=100,
                     deadline_ttft=now + 10.0)
    slack, binding = compute_slack(entry, "ep",
                                   latency_predictor=_FixedLatencyPredictor(),
                                   batch_size=8)
    assert binding == "ttft"
    assert slack > 0  # deadline far in future, predicted TTFT small


def test_compute_slack_ttft_negative_when_late():
    now = time.time()
    entry = SLOEntry(req_id="r1", slo_type="ttft", input_tokens=100,
                     deadline_ttft=now - 1.0)  # deadline already passed
    slack, _ = compute_slack(entry, "ep",
                             latency_predictor=_FixedLatencyPredictor(),
                             batch_size=8)
    assert slack < 0


def test_compute_slack_tpot_uses_budget():
    entry = SLOEntry(req_id="r1", slo_type="tpot", input_tokens=100,
                     deadline_tpot_s=0.05)
    slack, binding = compute_slack(entry, "ep",
                                   latency_predictor=_FixedLatencyPredictor(),
                                   batch_size=8)
    assert binding == "tpot"
    # budget 0.05 - predicted 0.02 = 0.03
    assert abs(slack - 0.03) < 1e-9


def test_compute_slack_combined_picks_binding():
    now = time.time()
    entry = SLOEntry(req_id="r1", slo_type="ttft+tpot", input_tokens=100,
                     deadline_ttft=now + 10.0, deadline_tpot_s=0.01)
    slack, binding = compute_slack(entry, "ep",
                                   latency_predictor=_FixedLatencyPredictor(),
                                   batch_size=8)
    # TPOT slack (0.01 - 0.02 = -0.01) is tighter than the generous TTFT slack.
    assert binding == "tpot"
    assert slack < 0
