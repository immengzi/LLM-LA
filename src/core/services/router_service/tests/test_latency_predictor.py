# tests/test_latency_predictor.py
# -*- coding: utf-8 -*-
"""Unit tests for latency predictors (router.latency_predictor)."""
import json

from router import latency_predictor as LP


def _linear_profile():
    return LP.LatencyProfile(
        compute=LP.ComputeCoeffs(alpha=0.0, beta=0.0, gamma=1e-3, delta=0.01),
        load=LP.LoadCoeffs(alpha=0.0, beta=5e-4, delta=0.0),
        decode=LP.DecodeCoeffs(alpha=0.0, beta=1e-3, gamma=0.0, delta=0.005),
        block_size_tokens=16,
    )


def test_profile_from_dict_defaults_missing_to_zero():
    prof = LP.LatencyProfile.from_dict({"compute": {"delta": 0.02}})
    assert prof.compute.delta == 0.02
    assert prof.compute.alpha == 0.0
    assert prof.load.beta == 0.0
    assert prof.block_size_tokens == 16


def test_profile_from_json(tmp_path):
    p = tmp_path / "profile.json"
    p.write_text(json.dumps({"decode": {"delta": 0.5}, "block_size_tokens": 32}))
    prof = LP.LatencyProfile.from_json(str(p))
    assert prof.decode.delta == 0.5
    assert prof.block_size_tokens == 32


def test_linear_ttft_reduced_by_cache_hits():
    pred = LP.LinearLatencyPredictor(_linear_profile())
    cold = pred.predict_ttft(input_tokens=1000, cached_tokens=0, batch_size=1)
    warm = pred.predict_ttft(input_tokens=1000, cached_tokens=500, batch_size=1)
    # More cached tokens -> less cold compute -> lower TTFT.
    assert warm < cold


def test_linear_tpot_non_negative_and_grows_with_length():
    pred = LP.LinearLatencyPredictor(_linear_profile())
    assert pred.predict_tpot(1, 0) >= 0
    small = pred.predict_tpot(8, 100)
    big = pred.predict_tpot(8, 100000)
    assert big >= small


def test_linear_e2e_is_ttft_plus_decode():
    pred = LP.LinearLatencyPredictor(_linear_profile())
    ttft = pred.predict_ttft(100, 0, 4)
    tpot = pred.predict_tpot(4, 100 + 50 // 2)
    e2e = pred.predict_e2e(100, 0, 50, 4)
    assert abs(e2e - (ttft + 50 * tpot)) < 1e-6


def test_piecewise_selects_range():
    prof = _linear_profile()
    pw = LP.PiecewiseLinearPredictor({4: prof, 16: prof, 64: prof})
    assert pw._select(2) is pw._predictors[4]
    assert pw._select(10) is pw._predictors[16]
    assert pw._select(1000) is pw._predictors[64]  # beyond max -> last range


def test_bayesian_scales_toward_observation():
    base = LP.LinearLatencyPredictor(_linear_profile())
    bp = LP.BayesianLatencyPredictor(base, forgetting_factor=0.5)
    baseline = bp.predict_ttft(1000, 0, 1)
    # Feed an observation where actual TTFT is much larger than predicted.
    obs = LP.LatencyObservation(
        input_tokens=1000, cached_tokens=0, batch_size=1,
        actual_ttft_s=baseline * 5,
    )
    bp.update(obs)
    assert bp.predict_ttft(1000, 0, 1) > baseline


def test_factory_returns_configured_predictor(reset_config, monkeypatch):
    for kind, cls in [
        ("linear", LP.LinearLatencyPredictor),
        ("bayesian", LP.BayesianLatencyPredictor),
        ("hybrid", LP.BayesianLatencyPredictor),
    ]:
        monkeypatch.setenv("LATENCY_PREDICTOR", kind)
        reset_config()
        monkeypatch.setattr(LP, "_latency_predictor_instance", None)
        assert isinstance(LP.get_latency_predictor(), cls), kind
