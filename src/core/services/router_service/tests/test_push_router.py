# tests/test_push_router.py
# -*- coding: utf-8 -*-
"""Unit tests for push-router endpoint-selection helpers."""
import random
from router.push_router import (
    _pick_min_score,
    _parse_prom_sums,
    _total_tokens_from_sums,
    _pick_lower_load,
    _kv_cost,
    _select_by_cost,
    _avg_latency_from_sums,
)

_SAMPLE = """\
# HELP vllm:prompt_tokens_total prompt tokens
# TYPE vllm:prompt_tokens_total counter
vllm:prompt_tokens_total{model_name="m"} 100
vllm:generation_tokens_total{model_name="m"} 50
vllm:prompt_tokens_total{model_name="m2"} 25
vllm:num_requests_running 3
"""


def test_pick_min_score():
    assert _pick_min_score({"a": 0.9, "b": 0.2, "c": 0.5}) == "b"
    assert _pick_min_score({"a": None, "b": 0.7}) == "b"
    assert _pick_min_score({"a": None, "b": None}) is None
    assert _pick_min_score({"a": 0.3, "b": 0.3}) == "a"


def test_parse_prom_sums_across_labels():
    sums = _parse_prom_sums(_SAMPLE, ["vllm:prompt_tokens_total"])
    assert sums["vllm:prompt_tokens_total"] == 125.0  # 100 + 25


def test_total_tokens_prompt_plus_generation():
    sums = _parse_prom_sums(
        _SAMPLE, ["vllm:prompt_tokens_total", "vllm:generation_tokens_total"]
    )
    # prompt 125 + generation 50 = 175
    assert _total_tokens_from_sums(sums) == 175.0


def test_total_tokens_single_counter_present():
    # Only prompt tokens present -> generation treated as 0.
    sums = _parse_prom_sums("vllm:prompt_tokens_total 42\n", [
        "vllm:prompt_tokens_total", "vllm:generation_tokens_total"
    ])
    assert _total_tokens_from_sums(sums) == 42.0


def test_total_tokens_absent_is_none():
    sums = _parse_prom_sums("vllm:num_requests_running 3\n", [
        "vllm:prompt_tokens_total", "vllm:generation_tokens_total"
    ])
    assert _total_tokens_from_sums(sums) is None


def test_pick_lower_load_prefers_smaller():
    assert _pick_lower_load("a", 1, "b", 2) == "a"
    assert _pick_lower_load("a", 3, "b", 2) == "b"


def test_pick_lower_load_ties_go_to_first():
    # power-of-two: on a tie, keep the first sampled endpoint (random pick).
    assert _pick_lower_load("a", 2, "b", 2) == "a"


def test_pick_lower_load_none_is_worst():
    # A failed probe (None) is treated as +inf, so the reachable peer wins.
    assert _pick_lower_load("a", None, "b", 5) == "b"
    assert _pick_lower_load("a", 5, "b", None) == "a"
    # Both probes failed -> fall back to the first sampled endpoint.
    assert _pick_lower_load("a", None, "b", None) == "a"

def test_kv_cost_overlap_reduces_prefill():
    # 10 prefill blocks, 5 cached, credit 1.0, scale 1.0, load 5 -> 5 + 5 = 10.
    assert _kv_cost(10, 5, 5.0, 1.0, 1.0) == 10.0
    # 8 cached -> adjusted 2, + load 9 = 11.
    assert _kv_cost(10, 8, 9.0, 1.0, 1.0) == 11.0
    # 2 cached -> adjusted 8, + load 10 = 18.
    assert _kv_cost(10, 2, 10.0, 1.0, 1.0) == 18.0


def test_kv_cost_clamped_nonnegative():
    # Overlap credit > 1 can over-subtract; adjusted prefill floors at 0.
    assert _kv_cost(4, 10, 3.0, 1.0, 1.0) == 3.0  # max(4-10,0)=0 -> 0 + 3


def test_kv_cost_scale_weights_prefill():
    # scale 2.0 doubles the prefill contribution: 2*(10-2) + 1 = 17.
    assert _kv_cost(10, 2, 1.0, 1.0, 2.0) == 17.0


def test_select_by_cost_argmin_deterministic():
    costs = {"a": 18.0, "b": 10.0, "c": 11.0}
    assert _select_by_cost(costs, 0.0) == "b"
    # Tie -> first inserted wins.
    assert _select_by_cost({"a": 5.0, "b": 5.0}, 0.0) == "a"
    assert _select_by_cost({}, 0.0) is None


def test_select_by_cost_softmax_favors_low_cost():
    # With a small temperature, the lowest-cost worker should dominate samples.
    costs = {"a": 100.0, "b": 0.0, "c": 100.0}
    rng = random.Random(1234)
    picks = [_select_by_cost(costs, 0.5, rng) for _ in range(200)]
    assert picks.count("b") > 180  # near-deterministic toward the cheapest

def test_avg_latency_from_sums():
    latency_sample = """\
# HELP vllm:e2e_request_latency_seconds end to end latency
# TYPE vllm:e2e_request_latency_seconds histogram
vllm:e2e_request_latency_seconds_sum{model_name="m"} 12.0
vllm:e2e_request_latency_seconds_count{model_name="m"} 4.0
vllm:e2e_request_latency_seconds_sum{model_name="m2"} 8.0
vllm:e2e_request_latency_seconds_count{model_name="m2"} 4.0
"""
    assert _avg_latency_from_sums(_parse_prom_sums(latency_sample, [
        "vllm:e2e_request_latency_seconds_sum",
        "vllm:e2e_request_latency_seconds_count",
    ])) == 2.5  # 20 / 8
    # Idle pod (count 0) -> 0.0 (most attractive).
    assert _avg_latency_from_sums({
        "vllm:e2e_request_latency_seconds_sum": 0.0,
        "vllm:e2e_request_latency_seconds_count": 0.0,
    }) == 0.0
    # Missing metric -> None.
    assert _avg_latency_from_sums({
        "vllm:e2e_request_latency_seconds_sum": None,
        "vllm:e2e_request_latency_seconds_count": None,
    }) is None
