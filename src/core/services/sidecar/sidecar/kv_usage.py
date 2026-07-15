# sidecar/kv_usage.py
# -*- coding: utf-8 -*-
"""Local vLLM GPU-KV usage scrape for soft divert reporting (default OFF).

Scrapes vLLM's own ``/metrics`` (Prometheus text) for
``vllm:kv_cache_usage_perc`` (prefer) or ``vllm:gpu_cache_usage_perc``
(fallback), aggregates ``max`` across ``engine`` label sets, and caches the
latest 0..1 float for ``/pull`` and ``/health``.

Reuses the same direct-local scrape style as ``slo_backpressure.VLLMTpotScraper``
(no cluster Prometheus). When disabled, no thread is started and callers see
``None`` (byte-compatible wire: omit field / leave unset).
"""
from __future__ import annotations

import threading
import time
from typing import Optional

import requests

try:
    from prometheus_client.parser import text_string_to_metric_families
except Exception:  # pragma: no cover
    text_string_to_metric_families = None  # type: ignore

# Prefer v1 name; fall back to v0.
_KV_METRIC_NAMES = ("vllm:kv_cache_usage_perc", "vllm:gpu_cache_usage_perc")


def parse_kv_usage_from_text(text: str) -> Optional[float]:
    """Parse GPU KV usage fraction from Prometheus text.

    Returns max gauge across label sets for the preferred metric name, or the
    fallback name if the preferred is absent. Ignores lmcache:* and hit-rate
    series. Returns None if neither series is present or on parse failure.
    Values outside [0, 1] are clamped; NaN/inf -> None.
    """
    if not text or text_string_to_metric_families is None:
        return None
    try:
        families = list(text_string_to_metric_families(text))
    except Exception:
        return None

    by_name = {fam.name: fam for fam in families}

    for name in _KV_METRIC_NAMES:
        fam = by_name.get(name)
        if fam is None:
            continue
        vals = []
        for s in fam.samples:
            # Gauge samples use the family name (no _sum/_count/_bucket suffix).
            if s.name != name and not s.name.startswith(name):
                continue
            # Skip histogram/summary appendages if a misnamed series appears.
            if s.name.endswith(("_sum", "_count", "_bucket", "_created")):
                continue
            try:
                v = float(s.value)
            except (TypeError, ValueError):
                continue
            if v != v or v in (float("inf"), float("-inf")):  # NaN/inf
                continue
            vals.append(v)
        if vals:
            mx = max(vals)
            if mx < 0.0:
                mx = 0.0
            if mx > 1.0:
                # Some exporters use 0..100; normalize if clearly percentage.
                if mx <= 100.0:
                    mx = mx / 100.0
                else:
                    mx = 1.0
            return mx
    return None


class VLLMKvUsageScraper:
    """GET local vLLM /metrics and return latest KV usage fraction (or None)."""

    def __init__(
        self,
        vllm_url: str,
        timeout_s: float = 2.0,
        session: Optional[requests.Session] = None,
    ):
        self._url = vllm_url.rstrip("/") + "/metrics"
        self._timeout = float(timeout_s)
        self._session = session or requests.Session()

    def sample(self) -> Optional[float]:
        try:
            r = self._session.get(self._url, timeout=self._timeout)
            r.raise_for_status()
            return parse_kv_usage_from_text(r.text)
        except Exception:
            return None


class KvUsageMonitor:
    """Background thread that refreshes a cached kv_usage float."""

    def __init__(
        self,
        vllm_url: str,
        *,
        interval_s: float = 5.0,
        timeout_s: float = 2.0,
        scraper: Optional[VLLMKvUsageScraper] = None,
    ):
        self._interval_s = max(0.2, float(interval_s))
        self._scraper = scraper or VLLMKvUsageScraper(vllm_url, timeout_s=timeout_s)
        self._lock = threading.Lock()
        self._value: Optional[float] = None
        self._ts: float = 0.0
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def get(self) -> Optional[float]:
        with self._lock:
            return self._value

    def get_with_ts(self) -> tuple[Optional[float], float]:
        with self._lock:
            return self._value, self._ts

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, name="kv-usage-monitor", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        t = self._thread
        if t is not None:
            t.join(timeout=self._interval_s + 1.0)
        self._thread = None

    def tick(self) -> Optional[float]:
        """One scrape (testable without the background thread)."""
        v = self._scraper.sample()
        with self._lock:
            # Keep last good sample on transient failure (stale handled by router).
            if v is not None:
                self._value = v
                self._ts = time.time()
            return self._value

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception:
                pass
            self._stop.wait(timeout=self._interval_s)


# Process-wide optional monitor bound from main().
_monitor: Optional[KvUsageMonitor] = None


def bind_kv_usage_monitor(mon: Optional[KvUsageMonitor]) -> None:
    global _monitor
    _monitor = mon


def get_cached_kv_usage() -> Optional[float]:
    """Return latest scraped KV usage, or None if reporting disabled / no data."""
    mon = _monitor
    if mon is None:
        return None
    return mon.get()
