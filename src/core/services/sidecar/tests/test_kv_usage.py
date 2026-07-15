# tests/test_kv_usage.py
"""GPU KV usage scrape / parse / accessibility tests."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from sidecar.kv_usage import (
    KvUsageMonitor,
    VLLMKvUsageScraper,
    parse_kv_usage_from_text,
)


KV_V1 = """\
# HELP vllm:kv_cache_usage_perc GPU KV usage
# TYPE vllm:kv_cache_usage_perc gauge
vllm:kv_cache_usage_perc{engine="0",model_name="m"} 0.40
vllm:kv_cache_usage_perc{engine="1",model_name="m"} 0.92
lmcache:local_cache_usage{engine="0"} 12345
"""

KV_V0 = """\
# TYPE vllm:gpu_cache_usage_perc gauge
vllm:gpu_cache_usage_perc{engine="0"} 0.55
"""

KV_BOTH = """\
# TYPE vllm:kv_cache_usage_perc gauge
vllm:kv_cache_usage_perc{engine="0"} 0.33
# TYPE vllm:gpu_cache_usage_perc gauge
vllm:gpu_cache_usage_perc{engine="0"} 0.99
"""

KV_PERCENT = """\
# TYPE vllm:kv_cache_usage_perc gauge
vllm:kv_cache_usage_perc{engine="0"} 85.0
"""


def test_parse_prefers_v1_and_max_engines():
    assert parse_kv_usage_from_text(KV_V1) == pytest.approx(0.92)


def test_parse_fallback_v0():
    assert parse_kv_usage_from_text(KV_V0) == pytest.approx(0.55)


def test_parse_prefers_v1_when_both_present():
    assert parse_kv_usage_from_text(KV_BOTH) == pytest.approx(0.33)


def test_parse_normalizes_percent_scale():
    assert parse_kv_usage_from_text(KV_PERCENT) == pytest.approx(0.85)


def test_parse_missing_series():
    text = "# TYPE lmcache:local_cache_usage gauge\nlmcache:local_cache_usage 1\n"
    assert parse_kv_usage_from_text(text) is None


def test_parse_garbage():
    assert parse_kv_usage_from_text("not prometheus") is None
    assert parse_kv_usage_from_text("") is None


def test_scraper_connection_failure():
    s = VLLMKvUsageScraper("http://127.0.0.1:1", timeout_s=0.2)
    assert s.sample() is None


def test_monitor_tick_keeps_last_on_failure(monkeypatch):
    mon = KvUsageMonitor("http://127.0.0.1:8000", interval_s=1.0)
    mon._scraper = MagicMock()
    mon._scraper.sample.side_effect = [0.7, None]
    assert mon.tick() == pytest.approx(0.7)
    assert mon.tick() == pytest.approx(0.7)  # keep last good
    assert mon.get() == pytest.approx(0.7)
