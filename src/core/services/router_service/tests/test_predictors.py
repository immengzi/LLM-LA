# tests/test_predictors.py
# -*- coding: utf-8 -*-
"""Unit tests for output-length predictors (router.predictors)."""
from router import predictors as P


def test_simple_predictor_char_heuristic():
    p = P.SimpleLengthPredictor()
    assert p.predict("abcd") == 2
    assert p.predict("") == 1  # floor at 1
    assert p.predict_out_tokens("abcdef") == 3


def test_hint_only_predictor():
    p = P.HintOnlyPredictor(default_tokens=256)
    assert p.predict("anything") == 256
    assert p.predict_with_hint(500, "x") == 500
    assert p.predict_with_hint(None, "abcd") == 2   # falls back to char-len
    assert p.predict_with_hint(0, "abcd") == 2      # non-positive hint ignored


def test_running_stats_mean_median_variance():
    s = P._RunningStats(max_window=100)
    for v in [10, 20, 30]:
        s.add(v)
    assert s.count == 3
    assert s.mean == 20.0
    assert s.median == 20.0
    assert s.variance > 0

    s2 = P._RunningStats()
    s2.add(2)
    s2.add(4)
    assert s2.median == 3.0  # even count -> average of middle two


def test_running_stats_window_eviction():
    s = P._RunningStats(max_window=10)  # min window is 10
    for v in range(100):
        s.add(v)
    assert s.count == 10
    assert s.mean == sum(range(90, 100)) / 10


def test_distribution_predictor_uses_median_after_min_samples():
    p = P.TaskTypeDistributionPredictor(min_samples=3, default_tokens=256)
    # Below min_samples -> default.
    p.update("r1", 100, task_type="chat")
    assert p.predict("x", task_type="chat") == 256
    p.update("r2", 200, task_type="chat")
    p.update("r3", 300, task_type="chat")
    # Now >= min_samples -> per-type median (200).
    assert p.predict("x", task_type="chat") == 200


def test_distribution_predictor_ignores_nonpositive_updates():
    p = P.TaskTypeDistributionPredictor(min_samples=1, default_tokens=256)
    p.update("r1", 0, task_type="chat")
    p.update("r2", -5, task_type="chat")
    assert p.predict("x", task_type="chat") == 256


def test_regression_predictor_fits_linear_relationship():
    p = P.InputLengthRegressionPredictor(min_samples=3, default_tokens=256)
    # y = 2x + 10
    for x in [10, 20, 30, 40]:
        p.update("r", 2 * x + 10, task_type="code", input_tokens=x)
    pred = p.predict("prompt", input_tokens=100, task_type="code")
    assert abs(pred - 210) <= 2  # ~2*100 + 10


def test_regression_predictor_default_before_min_samples():
    p = P.InputLengthRegressionPredictor(min_samples=20, default_tokens=256)
    p.update("r", 100, task_type="code", input_tokens=10)
    assert p.predict("x", input_tokens=10, task_type="code") == 256


def test_factory_selects_by_config(reset_config, monkeypatch):
    for kind, cls in [
        ("simple", P.SimpleLengthPredictor),
        ("distribution", P.TaskTypeDistributionPredictor),
        ("regression", P.InputLengthRegressionPredictor),
        ("hint_only", P.HintOnlyPredictor),
    ]:
        monkeypatch.setenv("OUTPUT_LEN_PREDICTOR", kind)
        reset_config()
        monkeypatch.setattr(P, "_predictor_instance", None)
        assert isinstance(P.get_output_length_predictor(), cls), kind
