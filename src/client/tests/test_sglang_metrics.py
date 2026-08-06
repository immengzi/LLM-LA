# tests/test_sglang_metrics.py
# -*- coding: utf-8 -*-
"""Unit tests for SGLang Prometheus metric name normalization.

SGLang exposes both colon- and underscore-style series names. The external
metrics scraper must map them onto the same fields used for vLLM ticks so
dashboards and KEDA queries stay engine-agnostic. Fixture:
``fixtures/sglang_v0_5_15_metrics.txt``.
"""
from __future__ import annotations

import sys
from pathlib import Path


CLIENT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CLIENT_DIR))

from prod_external_metrics_scraper import build_tick, parse_metrics


def test_sglang_fixture_maps_colon_and_underscore_names_to_standard_fields():
    """Pinned v0.5.15 fixture → standard tick fields (running/waiting/KV/gen)."""
    fixture = Path(__file__).parent / "fixtures" / "sglang_v0_5_15_metrics.txt"
    parsed = parse_metrics(fixture.read_text(encoding="utf-8"))

    tick, _ = build_tick(parsed, None, 0.0, [0.5], "engine:8200")

    assert tick["instances"] == ["engine:8200:sglang"]
    sample = tick["samples"][0]
    assert sample["engine"] == "sglang"
    assert sample["requests_running"] == 3
    assert sample["requests_waiting"] == 4
    assert sample["kv_cache_usage_perc"] == 0.625
    assert sample["gen_tokens_per_sec"] == 128.5
