#!/usr/bin/env python3
"""
external_metrics_scraper.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Scrape a raw vLLM Prometheus ``/metrics`` endpoint (an EXTERNAL server we do
not control / cannot put behind our Prometheus) and compute the same family of
metrics that prod_latency_collector.py pulls via PromQL — with TTFT front and
center.

Unlike prod_latency_collector.py (which queries a Prometheus server through
/api/v1/query), this talks to the model server's raw /metrics exposition text
directly, parses the histograms/counters, and derives rates + percentiles by
diffing consecutive scrapes (the same thing ``rate()`` / ``histogram_quantile``
do server-side).

Output is intentionally CONSISTENT with prod_latency_collector.py so the same
downstream analysis code works on both:

    * Runs land in the standard experiments layout ``<experiments_root>/<N>/``.
    * ``config.json``    - scraper settings + start time
    * ``metrics.jsonl``  - one tick per line, SAME envelope as the collector's
                           metrics.jsonl: ``{ts, mode, instances, samples:[...]}``
                           where each sample is keyed by a composite ``instance``
                           (``host:port:<engine>``, matching the collector's
                           ``instance+engine`` compound key).
    * ``run_summary.json`` - written on shutdown with tick count and wall time.

Each sample carries every vLLM field the collector emits (requests_running,
kv_cache_usage_perc, *_per_sec rates, prefix / external-prefix cache hit rates,
ttft/tpot/e2e averages, ...) PLUS extra per-histogram percentiles
(``ttft_p50``/``p90``/``p95``/``p99`` etc.) and ``*_count`` that only the raw
scrape can produce. Sidecar/router fields don't exist on an external vLLM
endpoint, so they're simply absent.

Note: request-body capture (the ``request_body`` field available on the client
and prod_latency_collector paths) is NOT applicable here. A vLLM ``/metrics``
endpoint exposes only aggregate histograms/counters -- there is no per-request
data of any kind -- so this scraper cannot and does not emit request bodies.

Usage:
    # live, every 5s, print a TTFT-focused table; results in a fresh experiment dir
    python external_metrics_scraper.py --url http://7.150.2.142:8077/metrics

    # custom experiments root & interval
    python external_metrics_scraper.py \
        --url http://7.150.2.142:8077/metrics \
        --experiments-root /home/data/saeid/experiments --interval 5

    # single snapshot (cumulative/all-time stats only, no windowed rates)
    python external_metrics_scraper.py --url http://7.150.2.142:8077/metrics --once

    # legacy: append ticks to a flat JSONL file instead of an experiment dir
    python external_metrics_scraper.py --url http://7.150.2.142:8077/metrics \
        --out /home/data/saeid/experiments/external_metrics.jsonl

The first tick (or --once) reports CUMULATIVE (all-time) averages/percentiles
and marks the row ``bootstrap: true`` because windowed rates need two samples.
Subsequent ticks are WINDOWED over the poll interval.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests

# ============================================================
# Target metrics (mirror prod_latency_collector._METRICS_CATALOG,
# restricted to what a vLLM /metrics endpoint actually exposes).
# ============================================================

GAUGES = [
    "vllm:num_requests_running",
    "vllm:num_requests_waiting",
    # v0 exposes gpu_cache_usage_perc; v1 exposes kv_cache_usage_perc. Take
    # whichever is present.
    "vllm:gpu_cache_usage_perc",
    "vllm:kv_cache_usage_perc",
    "sglang:num_running_reqs",
    "sglang:num_queue_reqs",
    "sglang:token_usage",
    "sglang:gen_throughput",
]

COUNTERS = [
    "vllm:request_success_total",
    "vllm:num_preemptions_total",
    "vllm:generation_tokens_total",
    "vllm:prompt_tokens_total",
    "vllm:prefix_cache_hits_total",
    "vllm:prefix_cache_queries_total",
    "vllm:external_prefix_cache_hits_total",
    "vllm:external_prefix_cache_queries_total",
    "vllm:prompt_tokens_cached_total",
]

# (base_name, percentile_prefix, avg_field). Percentiles + count use the prefix
# (e.g. "ttft_p95", "ttft_count"); the average uses avg_field so it matches the
# collector's field names exactly (note e2e -> "e2e_latency_seconds_avg").
HISTOGRAMS = [
    ("vllm:time_to_first_token_seconds", "ttft", "ttft_seconds_avg"),
    ("vllm:time_per_output_token_seconds", "tpot", "tpot_seconds_avg"),
    ("vllm:e2e_request_latency_seconds", "e2e", "e2e_latency_seconds_avg"),
    ("vllm:request_queue_time_seconds", "queue_time", "queue_time_seconds_avg"),
    ("vllm:request_prefill_time_seconds", "prefill_time", "prefill_time_seconds_avg"),
    ("vllm:request_decode_time_seconds", "decode_time", "decode_time_seconds_avg"),
]

# ------------------------------------------------------------
# LMCache (P2P host-staging) metrics. Exposed on the SAME vLLM /metrics
# endpoint with the ``lmcache:`` prefix when LMCache metric logging is
# enabled; simply absent (no series) on plain vLLM or when disabled, in
# which case every derived field below is None. Answers the question the
# vLLM (L0/GPU) prefix-cache metrics can't: when the GPU cache evicts a
# prefix, does LMCache's CPU host-staging + P2P tier serve it back, or
# also collapse (forcing a full recompute -> TTFT spike)?
# ------------------------------------------------------------
LMCACHE_GAUGES = [
    "lmcache:lookup_hit_rate",
    "lmcache:retrieve_hit_rate",
    "lmcache:local_cache_usage",
    "lmcache:remote_cache_usage",
    "lmcache:local_storage_usage",
    "lmcache:active_memory_objs_count",
    "lmcache:pinned_memory_objs_count",
    "lmcache:local_cpu_hot_cache_count",
    "lmcache:lmcache_is_healthy",
    "lmcache:kv_msg_queue_size",
    "lmcache:remote_put_task_num",
    "lmcache:storage_events_ongoing_count",
    "lmcache:scheduler_unfinished_requests_count",
]

LMCACHE_COUNTERS = [
    "lmcache:num_retrieve_requests",
    "lmcache:num_store_requests",
    "lmcache:num_lookup_requests",
    "lmcache:num_requested_tokens",
    "lmcache:num_hit_tokens",
    "lmcache:num_stored_tokens",
    "lmcache:num_lookup_tokens",
    "lmcache:num_lookup_hits",
    "lmcache:num_vllm_hit_tokens",
    "lmcache:local_cpu_evict_count",
    "lmcache:local_cpu_evict_keys_count",
    "lmcache:local_cpu_evict_failed_count",
    "lmcache:forced_unpin_count",
    "lmcache:num_slow_retrieval_by_time",
    "lmcache:num_slow_retrieval_by_speed",
    "lmcache:num_p2p_requests",
    "lmcache:num_p2p_transferred_tokens",
]

LMCACHE_HISTOGRAMS = [
    ("lmcache:time_to_retrieve", "lmc_time_to_retrieve", "lmc_time_to_retrieve_avg"),
    ("lmcache:time_to_store", "lmc_time_to_store", "lmc_time_to_store_avg"),
    ("lmcache:retrieve_speed", "lmc_retrieve_speed", "lmc_retrieve_speed_avg"),
    ("lmcache:store_speed", "lmc_store_speed", "lmc_store_speed_avg"),
    ("lmcache:p2p_time_to_transfer", "lmc_p2p_time_to_transfer", "lmc_p2p_time_to_transfer_avg"),
    ("lmcache:p2p_transfer_speed", "lmc_p2p_transfer_speed", "lmc_p2p_transfer_speed_avg"),
]

GAUGES = GAUGES + LMCACHE_GAUGES
COUNTERS = COUNTERS + LMCACHE_COUNTERS
HISTOGRAMS = HISTOGRAMS + LMCACHE_HISTOGRAMS

DEFAULT_QUANTILES = [0.50, 0.90, 0.95, 0.99]

# Per-second fields that get a per-minute companion (per_min = per_sec * 60).
# Kept identical to the collector's list so both tools emit the same keys;
# fields not present on a given row (e.g. router/sidecar on bare vLLM) are
# simply skipped. Covers both request rates (rps -> rpm) and token throughput
# (incoming prefill / outgoing gen).
_RPM_FIELDS = [
    ("request_success_per_sec", "request_success_per_min"),
    ("router_admission_rps",    "router_admission_rpm"),
    ("router_outgoing_rps",     "router_outgoing_rpm"),
    ("sidecar_received_rps",    "sidecar_received_rpm"),
    ("sidecar_completed_rps",   "sidecar_completed_rpm"),
    ("derived_rps",             "derived_rpm"),
    # token throughput: incoming = prompt/prefill, outgoing = generated/decode
    ("prefill_tokens_per_sec",  "prefill_tokens_per_min"),
    ("gen_tokens_per_sec",      "gen_tokens_per_min"),
]

_INF = float("inf")

_METRIC_ALIASES = {
    "sglang_num_running_reqs": "sglang:num_running_reqs",
    "sglang_num_queue_reqs": "sglang:num_queue_reqs",
    "sglang_token_usage": "sglang:token_usage",
    "sglang_gen_throughput": "sglang:gen_throughput",
}


# ============================================================
# Prometheus exposition-format parser
# ============================================================

_LABEL_RE = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')


def _parse_value(tok: str) -> float:
    t = tok.strip()
    if t in ("+Inf", "Inf"):
        return _INF
    if t == "-Inf":
        return -_INF
    if t == "NaN":
        return math.nan
    try:
        return float(t)
    except ValueError:
        return math.nan


def _parse_labels(inside: str) -> Dict[str, str]:
    return {m.group(1): m.group(2) for m in _LABEL_RE.finditer(inside)}


def parse_metrics(text: str) -> Dict[str, List[Tuple[Dict[str, str], float]]]:
    """Parse Prometheus text exposition into {metric_name: [(labels, value), ...]}."""
    out: Dict[str, List[Tuple[Dict[str, str], float]]] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "{" in line:
            name = line[: line.index("{")]
            rest = line[line.index("{") + 1 :]
            try:
                label_part, val_part = rest.rsplit("}", 1)
            except ValueError:
                continue
            labels = _parse_labels(label_part)
            val_tok = val_part.strip().split()
            if not val_tok:
                continue
            value = _parse_value(val_tok[0])
        else:
            parts = line.split()
            if len(parts) < 2:
                continue
            name, labels, value = parts[0], {}, _parse_value(parts[1])
        name = _METRIC_ALIASES.get(name, name)
        if name.startswith("sglang:") and not labels.get("engine"):
            labels["engine"] = "sglang"
        out.setdefault(name, []).append((labels, value))
    return out


# ============================================================
# Series grouping (by engine + model_name, like instance+engine)
# ============================================================

def _series_key(labels: Dict[str, str]) -> Tuple[str, str]:
    return (labels.get("model_name", ""), labels.get("engine", ""))


def _group_scalar(parsed, name: str) -> Dict[Tuple[str, str], float]:
    """Last-wins per (model, engine). Use for gauges (one series per engine)."""
    out: Dict[Tuple[str, str], float] = {}
    for labels, value in parsed.get(name, []):
        out[_series_key(labels)] = value
    return out


def _group_counter(parsed, name: str) -> Dict[Tuple[str, str], float]:
    """Sum per (model, engine). vLLM counters may split by extra labels
    (e.g. request_success_total by finished_reason), so aggregate them."""
    out: Dict[Tuple[str, str], float] = {}
    for labels, value in parsed.get(name, []):
        if math.isnan(value):
            continue
        key = _series_key(labels)
        out[key] = out.get(key, 0.0) + value
    return out


def _group_histogram(parsed, base: str) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """Return {series_key: {"buckets": {le: cum_count}, "sum": x, "count": n}}."""
    groups: Dict[Tuple[str, str], Dict[str, Any]] = {}

    def _ensure(key):
        return groups.setdefault(key, {"buckets": {}, "sum": None, "count": None})

    for labels, value in parsed.get(base + "_bucket", []):
        key = _series_key(labels)
        le = _parse_value(labels.get("le", "NaN"))
        if not math.isnan(le):
            _ensure(key)["buckets"][le] = value
    for labels, value in parsed.get(base + "_sum", []):
        _ensure(_series_key(labels))["sum"] = value
    for labels, value in parsed.get(base + "_count", []):
        _ensure(_series_key(labels))["count"] = value
    return groups


# ============================================================
# Histogram math (Prometheus-compatible)
# ============================================================

def hist_quantile(q: float, buckets: Dict[float, float]) -> Optional[float]:
    """Prometheus histogram_quantile over cumulative bucket counts {le: cum}."""
    if not buckets:
        return None
    items = sorted(buckets.items(), key=lambda kv: kv[0])
    total = items[-1][1]
    if total is None or total <= 0 or math.isnan(total):
        return None
    rank = q * total
    prev_le = 0.0
    prev_c = 0.0
    for le, c in items:
        if c is None or math.isnan(c):
            continue
        if c >= rank:
            if math.isinf(le):
                # Can't interpolate into +Inf; return highest finite bound.
                return prev_le
            bucket_count = c - prev_c
            if bucket_count <= 0:
                return le
            return prev_le + (le - prev_le) * ((rank - prev_c) / bucket_count)
        if not math.isinf(le):
            prev_le = le
        prev_c = c
    return prev_le


def _delta_buckets(cur: Dict[float, float], prev: Dict[float, float]) -> Dict[float, float]:
    out: Dict[float, float] = {}
    for le, c in cur.items():
        p = prev.get(le, 0.0)
        d = c - p
        # counter reset guard
        out[le] = d if d >= 0 else c
    return out


def hist_stats(
    cur: Dict[str, Any],
    prev: Optional[Dict[str, Any]],
    quantiles: List[float],
) -> Tuple[Dict[str, Optional[float]], bool]:
    """Return (stats, windowed). Windowed uses deltas; else cumulative."""
    cur_sum, cur_count = cur.get("sum"), cur.get("count")
    windowed = False
    stats: Dict[str, Optional[float]] = {}

    if prev is not None and prev.get("count") is not None and cur_count is not None:
        d_count = cur_count - prev["count"]
        if d_count > 0:
            windowed = True
            d_sum = (cur_sum or 0.0) - (prev.get("sum") or 0.0)
            stats["avg"] = d_sum / d_count if d_count else None
            stats["count"] = d_count
            dbuckets = _delta_buckets(cur.get("buckets", {}), prev.get("buckets", {}))
            for q in quantiles:
                stats[f"p{int(q * 100)}"] = hist_quantile(q, dbuckets)
            return stats, windowed

    # cumulative fallback
    if cur_count and cur_count > 0:
        stats["avg"] = (cur_sum or 0.0) / cur_count
    else:
        stats["avg"] = None
    stats["count"] = cur_count
    for q in quantiles:
        stats[f"p{int(q * 100)}"] = hist_quantile(q, cur.get("buckets", {}))
    return stats, windowed


def _rate(cur: Optional[float], prev: Optional[float], dt: float) -> Optional[float]:
    if cur is None or prev is None or dt <= 0:
        return None
    d = cur - prev
    if d < 0:
        d = cur  # counter reset
    return d / dt


# ============================================================
# Experiment directory helpers (same logic as prod_latency_collector.py)
# ============================================================

def _next_experiment_dir(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    existing_ids: List[int] = []
    for p in root.iterdir():
        if p.is_dir() and p.name.isdigit():
            try:
                existing_ids.append(int(p.name))
            except ValueError:
                continue
    next_id = max(existing_ids) + 1 if existing_ids else 1
    exp_dir = root / str(next_id)
    exp_dir.mkdir(parents=True, exist_ok=False)
    return exp_dir


def _instance_from_url(url: str) -> str:
    """Derive the ``instance`` label (host:port) from the /metrics URL, matching
    the ``instance`` Prometheus would attach when scraping this target."""
    netloc = urlparse(url).netloc
    return netloc or url


# ============================================================
# Snapshot -> per-engine records
# ============================================================

def build_tick(
    parsed: Dict[str, List[Tuple[Dict[str, str], float]]],
    prev_raw: Optional[Dict[str, Any]],
    dt: float,
    quantiles: List[float],
    instance_base: str,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Return (tick_dict, raw_snapshot_for_next_diff).

    The tick uses the SAME envelope as prod_latency_collector's metrics.jsonl:
    ``{ts, mode, instances, samples}`` with a composite ``instance`` key
    (``host:port:<engine>``). ``window_s``/``windowed`` are extra provenance.
    """
    # gauges
    gauges = {name: _group_scalar(parsed, name) for name in GAUGES}
    counters = {name: _group_counter(parsed, name) for name in COUNTERS}
    hists = {base: _group_histogram(parsed, base) for base, _prefix, _avg in HISTOGRAMS}

    # raw snapshot we persist for the next diff
    raw_snapshot = {
        "counters": counters,
        "hists": hists,
        "gauges": gauges,
    }
    prev_counters = (prev_raw or {}).get("counters", {})
    prev_hists = (prev_raw or {}).get("hists", {})
    prev_gauges = (prev_raw or {}).get("gauges", {})

    # union of all series keys
    keys = set()
    for m in gauges.values():
        keys |= set(m.keys())
    for m in counters.values():
        keys |= set(m.keys())
    for hg in hists.values():
        keys |= set(hg.keys())

    samples: List[Dict[str, Any]] = []
    any_windowed = False

    for key in sorted(keys):
        model_name, engine = key
        instance = f"{instance_base}:{engine}" if engine else instance_base
        rec: Dict[str, Any] = {
            "instance": instance,
            "model_name": model_name,
            "engine": engine,
        }

        # gauges
        rr = gauges["vllm:num_requests_running"].get(key)
        if rr is None:
            rr = gauges["sglang:num_running_reqs"].get(key)
        rw = gauges["vllm:num_requests_waiting"].get(key)
        if rw is None:
            rw = gauges["sglang:num_queue_reqs"].get(key)
        kv = gauges["vllm:kv_cache_usage_perc"].get(key)
        if kv is None:
            kv = gauges["vllm:gpu_cache_usage_perc"].get(key)
        if kv is None:
            kv = gauges["sglang:token_usage"].get(key)
        rec["requests_running"] = rr
        rec["requests_waiting"] = rw
        rec["kv_cache_usage_perc"] = kv

        # counter rates
        def crate(name):
            return _rate(counters[name].get(key), prev_counters.get(name, {}).get(key), dt)

        rec["gen_tokens_per_sec"] = crate("vllm:generation_tokens_total")
        if rec["gen_tokens_per_sec"] is None:
            rec["gen_tokens_per_sec"] = gauges["sglang:gen_throughput"].get(key)
        rec["prefill_tokens_per_sec"] = crate("vllm:prompt_tokens_total")
        # Cumulative (all-time) token counters, so incoming/outgoing token
        # throughput can be re-windowed retrospectively (not just the rate above).
        rec["generation_tokens_total"] = counters["vllm:generation_tokens_total"].get(key)
        rec["prompt_tokens_total"] = counters["vllm:prompt_tokens_total"].get(key)
        rec["request_success_per_sec"] = crate("vllm:request_success_total")
        # Cumulative (all-time) finished-request counter, summed across the
        # finished_reason label by _group_counter. Matches the internal
        # collector's request_success_total field.
        rec["request_success_total"] = counters["vllm:request_success_total"].get(key)
        rec["preemptions_per_sec"] = crate("vllm:num_preemptions_total")
        rec["prompt_tokens_cached_per_sec"] = crate("vllm:prompt_tokens_cached_total")

        pc_hits = crate("vllm:prefix_cache_hits_total")
        pc_q = crate("vllm:prefix_cache_queries_total")
        rec["prefix_cache_hits_per_sec"] = pc_hits
        rec["prefix_cache_queries_per_sec"] = pc_q
        rec["prefix_cache_hit_rate"] = (
            pc_hits / pc_q if (pc_hits is not None and pc_q and pc_q > 0) else None
        )

        ext_hits = crate("vllm:external_prefix_cache_hits_total")
        ext_q = crate("vllm:external_prefix_cache_queries_total")
        rec["ext_prefix_cache_hits_per_sec"] = ext_hits
        rec["ext_prefix_cache_queries_per_sec"] = ext_q
        rec["ext_prefix_cache_hit_rate"] = (
            ext_hits / ext_q if (ext_hits is not None and ext_q and ext_q > 0) else None
        )

        # --- LMCache (P2P host-staging); absent (all None) on plain vLLM ---
        # point-in-time gauges
        rec["lmc_lookup_hit_rate_gauge"] = gauges["lmcache:lookup_hit_rate"].get(key)
        rec["lmc_retrieve_hit_rate_gauge"] = gauges["lmcache:retrieve_hit_rate"].get(key)
        rec["lmc_local_cache_usage_bytes"] = gauges["lmcache:local_cache_usage"].get(key)
        rec["lmc_remote_cache_usage_bytes"] = gauges["lmcache:remote_cache_usage"].get(key)
        rec["lmc_local_storage_usage_bytes"] = gauges["lmcache:local_storage_usage"].get(key)
        rec["lmc_active_memory_objs"] = gauges["lmcache:active_memory_objs_count"].get(key)
        rec["lmc_pinned_memory_objs"] = gauges["lmcache:pinned_memory_objs_count"].get(key)
        rec["lmc_local_cpu_hot_cache_count"] = gauges["lmcache:local_cpu_hot_cache_count"].get(key)
        rec["lmc_is_healthy"] = gauges["lmcache:lmcache_is_healthy"].get(key)
        rec["lmc_kv_msg_queue_size"] = gauges["lmcache:kv_msg_queue_size"].get(key)
        rec["lmc_remote_put_task_num"] = gauges["lmcache:remote_put_task_num"].get(key)
        rec["lmc_storage_events_ongoing"] = gauges["lmcache:storage_events_ongoing_count"].get(key)
        rec["lmc_scheduler_unfinished_requests"] = gauges["lmcache:scheduler_unfinished_requests_count"].get(key)
        # counter rates
        rec["lmc_retrieve_requests_per_sec"] = crate("lmcache:num_retrieve_requests")
        rec["lmc_store_requests_per_sec"] = crate("lmcache:num_store_requests")
        rec["lmc_lookup_requests_per_sec"] = crate("lmcache:num_lookup_requests")
        lmc_req_tok = crate("lmcache:num_requested_tokens")
        lmc_hit_tok = crate("lmcache:num_hit_tokens")
        lmc_lookup_tok = crate("lmcache:num_lookup_tokens")
        lmc_lookup_hit = crate("lmcache:num_lookup_hits")
        rec["lmc_requested_tokens_per_sec"] = lmc_req_tok
        rec["lmc_hit_tokens_per_sec"] = lmc_hit_tok
        rec["lmc_stored_tokens_per_sec"] = crate("lmcache:num_stored_tokens")
        rec["lmc_lookup_tokens_per_sec"] = lmc_lookup_tok
        rec["lmc_lookup_hit_tokens_per_sec"] = lmc_lookup_hit
        rec["lmc_vllm_hit_tokens_per_sec"] = crate("lmcache:num_vllm_hit_tokens")
        rec["lmc_cpu_evict_per_sec"] = crate("lmcache:local_cpu_evict_count")
        rec["lmc_cpu_evict_keys_per_sec"] = crate("lmcache:local_cpu_evict_keys_count")
        rec["lmc_cpu_evict_failed_per_sec"] = crate("lmcache:local_cpu_evict_failed_count")
        rec["lmc_forced_unpin_per_sec"] = crate("lmcache:forced_unpin_count")
        rec["lmc_slow_retrieval_by_time_per_sec"] = crate("lmcache:num_slow_retrieval_by_time")
        rec["lmc_slow_retrieval_by_speed_per_sec"] = crate("lmcache:num_slow_retrieval_by_speed")
        rec["lmc_p2p_requests_per_sec"] = crate("lmcache:num_p2p_requests")
        rec["lmc_p2p_transferred_tokens_per_sec"] = crate("lmcache:num_p2p_transferred_tokens")
        # windowed token-level hit rates (the real "LMCache tier caught it" signal)
        rec["lmc_lookup_hit_rate"] = (
            lmc_lookup_hit / lmc_lookup_tok
            if (lmc_lookup_hit is not None and lmc_lookup_tok and lmc_lookup_tok > 0) else None
        )
        rec["lmc_retrieve_hit_rate"] = (
            lmc_hit_tok / lmc_req_tok
            if (lmc_hit_tok is not None and lmc_req_tok and lmc_req_tok > 0) else None
        )
        # cumulative counters, so LMCache hit rate / P2P volume can be re-windowed
        rec["lmc_num_lookup_hits_total"] = counters["lmcache:num_lookup_hits"].get(key)
        rec["lmc_num_lookup_tokens_total"] = counters["lmcache:num_lookup_tokens"].get(key)
        rec["lmc_num_hit_tokens_total"] = counters["lmcache:num_hit_tokens"].get(key)
        rec["lmc_num_requested_tokens_total"] = counters["lmcache:num_requested_tokens"].get(key)
        rec["lmc_num_p2p_transferred_tokens_total"] = counters["lmcache:num_p2p_transferred_tokens"].get(key)

        # Flow-balance incoming estimate. A bare vLLM has no router-admission
        # counter, so we infer arrivals from conservation of requests in the
        # engine: arrivals = departures + d(N)/dt, where departures is the
        # success rate and N = running + waiting. None until we have a windowed
        # pair of snapshots.
        derived_rps = None
        if rr is not None and rw is not None and dt > 0 and prev_raw is not None:
            prev_rr = prev_gauges.get("vllm:num_requests_running", {}).get(key)
            prev_rw = prev_gauges.get("vllm:num_requests_waiting", {}).get(key)
            succ = rec.get("request_success_per_sec")
            if prev_rr is not None and prev_rw is not None and succ is not None:
                net_queue_growth = ((rr + rw) - (prev_rr + prev_rw)) / dt
                derived_rps = succ + net_queue_growth
                rec["net_queue_growth_per_sec"] = net_queue_growth
        rec["derived_rps"] = derived_rps

        # Per-minute companions for every request-rate field (rpm = rps * 60,
        # same 30s window, just a unit conversion for convenience/reporting).
        for _rps_field, _rpm_field in _RPM_FIELDS:
            _v = rec.get(_rps_field)
            if _rps_field in rec:
                rec[_rpm_field] = (_v * 60.0) if _v is not None else None

        # histograms (avg + percentiles), TTFT first
        for base, prefix, avg_field in HISTOGRAMS:
            cur_g = hists[base].get(key)
            if cur_g is None:
                continue
            prev_g = prev_hists.get(base, {}).get(key)
            stats, windowed = hist_stats(cur_g, prev_g, quantiles)
            any_windowed = any_windowed or windowed
            rec[avg_field] = stats.get("avg")
            rec[f"{prefix}_count"] = stats.get("count")
            for q in quantiles:
                rec[f"{prefix}_p{int(q * 100)}"] = stats.get(f"p{int(q * 100)}")

        rec["bootstrap"] = not any_windowed
        samples.append(rec)

    tick = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "mode": "collector",
        "scrape_mode": "raw-metrics",
        "window_s": round(dt, 3) if dt > 0 else None,
        "windowed": any_windowed,
        "instances": [s["instance"] for s in samples],
        "samples": samples,
    }
    return tick, raw_snapshot


# ============================================================
# Pretty console output (TTFT-focused)
# ============================================================

def _fmt(v: Optional[float], nd: int = 3) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "-"
    return f"{v:.{nd}f}"


def print_tick(tick: Dict[str, Any], quantiles: List[float]) -> None:
    ts = tick["ts"].split("T")[1][:8] if "T" in tick["ts"] else tick["ts"]
    mode = "WINDOW" if tick["windowed"] else "CUMUL "
    win = f"{tick['window_s']}s" if tick.get("window_s") else "-"
    print(f"=== {ts}  [{mode}] window={win} ===")
    qcols = " ".join(f"ttft_p{int(q*100)}" for q in quantiles)
    print(
        f"{'instance':<24} {'run':>4} {'wait':>4} {'ttft_avg':>9} "
        + " ".join(f"{c:>9}" for c in qcols.split())
        + f" {'tpot_avg':>9} {'e2e_avg':>9} {'gen_tok/s':>10} {'succ/s':>7} {'drps':>7}"
    )
    for s in tick["samples"]:
        inst = str(s.get("instance", "?"))[:24]
        row = (
            f"{inst:<24} {_fmt(s.get('requests_running'),0):>4} "
            f"{_fmt(s.get('requests_waiting'),0):>4} "
            f"{_fmt(s.get('ttft_seconds_avg')):>9} "
        )
        for q in quantiles:
            row += f"{_fmt(s.get(f'ttft_p{int(q*100)}')):>9} "
        row += (
            f"{_fmt(s.get('tpot_seconds_avg')):>9} "
            f"{_fmt(s.get('e2e_latency_seconds_avg')):>9} "
            f"{_fmt(s.get('gen_tokens_per_sec'),1):>10} "
            f"{_fmt(s.get('request_success_per_sec'),2):>7} "
            f"{_fmt(s.get('derived_rps'),2):>7}"
        )
        print(row)
    print()


# ============================================================
# Main loop
# ============================================================

def fetch(url: str, timeout: float) -> str:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.text


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Scrape an external vLLM /metrics endpoint for TTFT & friends, "
                    "writing prod_latency_collector-compatible experiment output."
    )
    ap.add_argument("--url", default="http://7.150.2.142:8077/metrics",
                    help="External vLLM /metrics URL (default: %(default)s)")
    ap.add_argument("--experiments-root", default="/home/data/saeid/experiments",
                    help="Root dir for experiment outputs (default: %(default)s)")
    ap.add_argument("--interval", type=float, default=30.0,
                    help="Seconds between scrapes; this is also the rate window "
                         "(deltas are computed between consecutive scrapes). "
                         "30s matches the collector's 30s rate window for "
                         "apples-to-apples comparison (default: 30)")
    ap.add_argument("--once", action="store_true",
                    help="Single cumulative snapshot, then exit")
    ap.add_argument("--duration", type=float, default=0.0,
                    help="Stop after N seconds (0 = run until Ctrl+C)")
    ap.add_argument("--out", default=None,
                    help="Legacy: append ticks to this flat JSONL path instead of "
                         "creating an experiment dir")
    ap.add_argument("--timeout", type=float, default=10.0,
                    help="HTTP timeout seconds (default: 10)")
    ap.add_argument("--quantiles", default="0.5,0.9,0.95,0.99",
                    help="Comma-separated quantiles (default: %(default)s)")
    ap.add_argument("--quiet", action="store_true",
                    help="Do not print the console table (JSONL only)")
    args = ap.parse_args()

    quantiles = []
    for tok in args.quantiles.split(","):
        tok = tok.strip()
        if tok:
            quantiles.append(float(tok))
    quantiles.sort()

    instance_base = _instance_from_url(args.url)
    t_start = time.time()

    # Output target: experiment dir (default, collector-consistent) or legacy flat file.
    exp_dir: Optional[Path] = None
    if args.out:
        metrics_path = Path(args.out)
        metrics_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        exp_dir = _next_experiment_dir(Path(args.experiments_root))
        print(f"[scraper] experiment dir: {exp_dir}")
        config_out = {
            "created_at_unix": t_start,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "collector": "external_metrics_scraper",
            "scrape_mode": "raw-metrics",
            "target_url": args.url,
            "instance": instance_base,
            "interval_s": args.interval,
            "quantiles": quantiles,
            "client_config": {
                "backend": "external-scrape",
                "target_url": args.url,
            },
        }
        with (exp_dir / "config.json").open("w") as f:
            json.dump(config_out, f, indent=2, sort_keys=True)
        metrics_path = exp_dir / "metrics.jsonl"

    out_fh = open(metrics_path, "a", encoding="utf-8")

    shutdown = threading.Event()

    def _sig(signum, frame):
        shutdown.set()

    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)

    print(f"[scraper] target: {args.url}  (instance={instance_base})")
    print(f"[scraper] appending JSONL -> {metrics_path}")

    prev_raw: Optional[Dict[str, Any]] = None
    prev_t: Optional[float] = None
    tick_count = 0

    while not shutdown.is_set():
        now = time.time()
        try:
            text = fetch(args.url, args.timeout)
        except Exception as e:
            print(f"[scraper] fetch failed: {e}", file=sys.stderr)
            if args.once:
                break
            shutdown.wait(args.interval)
            continue

        parsed = parse_metrics(text)
        dt = (now - prev_t) if prev_t is not None else 0.0
        tick, prev_raw = build_tick(parsed, prev_raw, dt, quantiles, instance_base)
        prev_t = now

        out_fh.write(json.dumps(tick, default=str) + "\n")
        out_fh.flush()
        tick_count += 1
        if not args.quiet:
            print_tick(tick, quantiles)

        if args.once:
            break
        if args.duration and (time.time() - t_start) >= args.duration:
            break
        shutdown.wait(args.interval)

    out_fh.close()
    dt_wall = time.time() - t_start

    if exp_dir is not None:
        run_summary = {
            "collector": "external_metrics_scraper",
            "backend": "external-scrape",
            "scrape_mode": "raw-metrics",
            "target_url": args.url,
            "instance": instance_base,
            "tick_count": tick_count,
            "wall_time_s": round(dt_wall, 3),
            "interval_s": args.interval,
            "quantiles": quantiles,
        }
        try:
            with (exp_dir / "run_summary.json").open("w") as f:
                json.dump(run_summary, f, indent=2, sort_keys=True)
        except Exception as e:
            print(f"[scraper] WARN: failed to write run_summary.json: {e}")
        print(f"[scraper] done. {tick_count} ticks in {dt_wall:.1f}s")
        print(f"[scraper] results: {exp_dir}")
    else:
        print(f"[scraper] done. {tick_count} ticks in {dt_wall:.1f}s -> {metrics_path}")


if __name__ == "__main__":
    main()
