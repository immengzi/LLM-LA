# tests/test_push_router.py
# -*- coding: utf-8 -*-
"""Unit tests for push-router endpoint-selection helpers."""
from router.push_router import (
    _pick_min_score,
    _parse_prom_sums,
    _total_tokens_from_sums,
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
