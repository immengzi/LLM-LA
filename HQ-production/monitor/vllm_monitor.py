#!/usr/bin/env python3
"""
vllm_monitor.py — Continuous background monitor for vLLM /metrics endpoint.

Storage format is fully consistent with prom_utils.py and metrics_prom.py:

  JSONL record (metrics.jsonl):
    {"ts": "<iso8601>", "mode": "<mode>", "samples": [{"instance": "<host>", ...fields...}]}

  Field types per category:
    gauge      -> plain float       (e.g. requests_running = 4.0)
    counter    -> plain float/s     (rate over poll interval, e.g. gen_tokens_per_sec = 312.4)
    histogram  -> plain float       (lifetime mean = sum/count, e.g. ttft_seconds_avg = 0.342)
                  PLUS extra p99 field: ttft_seconds_avg__p99 = 0.51
                  The _avg field is a plain float matching prom_utils/metrics_prom semantics.

  Error ticks:
    {"ts": "...", "mode": "...", "samples": [], "error": "...", "consecutive_failures": N}

Usage
-----
    # Foreground:
    python vllm_monitor.py --host http://<hostip> --interval 10

    # Background daemon (nohup):
    nohup python vllm_monitor.py --host http://<hostip> --interval 10 \\
        --log-dir ./vllm_logs > /dev/null 2>&1 &

    # Background via systemd:
    systemctl --user start vllm_monitor

Environment variables (all overridable by CLI flags):
    VLLM_HOST      — base URL of the vLLM server  (default: http://localhost:8000)
    VLLM_INTERVAL  — poll interval in seconds      (default: 15)
    VLLM_LOG_DIR   — directory for log files       (default: ./vllm_logs)

Latency field naming (matches prom_utils/metrics_prom hist_avg fields):
    e2e_latency_seconds_avg      vllm:e2e_request_latency_seconds
    ttft_seconds_avg             vllm:time_to_first_token_seconds
    tpot_seconds_avg             vllm:time_per_output_token_seconds  (or request_time_per_output_token_seconds)
    queue_time_seconds_avg       vllm:request_queue_time_seconds
    prefill_time_seconds_avg     vllm:request_prefill_time_seconds
    decode_time_seconds_avg      vllm:request_decode_time_seconds
    inference_time_seconds_avg   vllm:request_inference_time_seconds
    request_prompt_tokens_avg    vllm:request_prompt_tokens
    request_generation_tokens_avg vllm:request_generation_tokens
    request_max_generation_tokens_avg vllm:request_max_num_generation_tokens

  Each histogram field FOO_avg is a plain float (the cumulative mean).
  A companion FOO_avg__p99 float is also written (vllm_monitor extension,
  not in prom_utils — downstream can ignore it if not needed).
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import signal
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import urllib.request
import urllib.error

# ─────────────────────────────────────────────────────────────────────────────
# Version
# ─────────────────────────────────────────────────────────────────────────────

VERSION = "1.7.0"

# ─────────────────────────────────────────────────────────────────────────────
# Metric catalog
#
# Each entry: (prometheus_metric_name, field_name, metric_type)
#
# field_name    — key written into the sample dict.
#                 For counters this is the RATE field name matching
#                 prom_utils/metrics_prom counter_rate fields.
#                 For histograms this is the _avg field name matching
#                 prom_utils/metrics_prom hist_avg fields.
#
# metric_type   — gauge | counter | histogram
#
# Alias pairs are listed consecutively; the collector uses the first one
# actually present in the scraped /metrics output.
# ─────────────────────────────────────────────────────────────────────────────

METRIC_TARGETS: List[Tuple[str, str, str]] = [

    # ── KV cache (gauges → plain float 0-1) ───────────────────────────────
    ("vllm:kv_cache_usage_perc",                    "gpu_kv_cache_usage_frac",              "gauge"),
    # Primary name confirmed present in your vLLM build.
    ("vllm:gpu_cache_usage_perc",                   "gpu_kv_cache_usage_frac",              "gauge"),
    # Fallback alias (older vLLM builds).

    ("vllm:cpu_cache_usage_perc",                   "cpu_kv_cache_usage_frac",              "gauge"),

    # ── Prefix cache (raw counters stored as cumulative float) ────────────
    # These have no equivalent in prom_utils/metrics_prom; kept as extras.
    ("vllm:prefix_cache_queries",                   "prefix_cache_queries",                 "counter_cumulative"),
    # Local GPU HBM prefix cache lookup queries.
    ("vllm:prefix_cache_hits",                      "prefix_cache_hits",                    "counter_cumulative"),
    # Local GPU HBM prefix cache hits.

    # ── External prefix cache (KV Connector: OffloadingConnector / LMCache) ──
    # Present only when a KV Connector is configured; silently absent otherwise.
    ("vllm:external_prefix_cache_queries_total",    "external_prefix_cache_queries",        "counter_cumulative"),
    # Cumulative queries to external KV store (CPU offload / remote cache).
    ("vllm:external_prefix_cache_hits_total",       "external_prefix_cache_hits",           "counter_cumulative"),
    # Cumulative hits from external KV store.

    # ── Request queue state (gauges → plain float) ────────────────────────
    ("vllm:num_requests_running",                   "requests_running",                     "gauge"),
    ("vllm:num_requests_waiting",                   "requests_waiting",                     "gauge"),

    # ── Preemptions (counter → rate float/s) ─────────────────────────────
    # field name matches metrics_prom counter_rate field
    ("vllm:num_preemptions_total",                  "preemptions_per_sec",                  "counter_rate"),

    # ── Latency histograms (→ plain float = cumulative mean) ──────────────
    # field names match prom_utils / metrics_prom hist_avg fields exactly
    ("vllm:e2e_request_latency_seconds",            "e2e_latency_seconds_avg",              "histogram"),
    ("vllm:time_to_first_token_seconds",            "ttft_seconds_avg",                     "histogram"),

    # TPOT: two names seen across vLLM builds; first match wins
    ("vllm:time_per_output_token_seconds",          "tpot_seconds_avg",                     "histogram"),
    ("vllm:request_time_per_output_token_seconds",  "tpot_seconds_avg",                     "histogram"),

    ("vllm:request_queue_time_seconds",             "queue_time_seconds_avg",               "histogram"),
    ("vllm:request_prefill_time_seconds",           "prefill_time_seconds_avg",             "histogram"),
    ("vllm:request_decode_time_seconds",            "decode_time_seconds_avg",              "histogram"),
    ("vllm:request_inference_time_seconds",         "inference_time_seconds_avg",           "histogram"),
    ("vllm:request_prompt_tokens",                  "request_prompt_tokens_avg",            "histogram"),
    ("vllm:request_generation_tokens",              "request_generation_tokens_avg",        "histogram"),
    ("vllm:request_max_num_generation_tokens",      "request_max_generation_tokens_avg",    "histogram"),

    # ── Token throughput (counter → rate float/s) ─────────────────────────
    # field names match metrics_prom counter_rate fields
    ("vllm:prompt_tokens_total",                    "prefill_tokens_per_sec",               "counter_rate"),
    ("vllm:prompt_tokens",                          "prefill_tokens_per_sec",               "counter_rate"),
    # v0 alias.

    ("vllm:generation_tokens_total",                "gen_tokens_per_sec",                   "counter_rate"),
    ("vllm:generation_tokens",                      "gen_tokens_per_sec",                   "counter_rate"),
    # v0 alias.

    # ── Request outcomes (counter → rate float/s) ─────────────────────────
    # field names match metrics_prom counter_rate fields
    ("vllm:request_success_total",                  "request_success_per_sec",              "counter_rate"),
    ("vllm:request_success",                        "request_success_per_sec",              "counter_rate"),
    # v0 alias.

    ("vllm:request_failure_total",                  "request_failure_per_sec",              "counter_rate"),

    # ── Speculative decoding (counter → rate float/s) ─────────────────────
    # field names match metrics_prom counter_rate fields
    ("vllm:spec_decode_num_accepted_tokens_total",  "spec_tokens_accepted_per_sec",         "counter_rate"),
    ("vllm:spec_decode_num_draft_tokens_total",     "spec_tokens_draft_per_sec",            "counter_rate"),
    ("vllm:spec_decode_num_emitted_tokens_total",   "spec_tokens_emitted_per_sec",          "counter_rate"),
]

# ─────────────────────────────────────────────────────────────────────────────
# JsonlLogger
# Matches the open/write/close interface of metrics_prom.py JsonlLogger,
# and the write-only interface of prom_utils.py JsonlLogger.
# ─────────────────────────────────────────────────────────────────────────────

class JsonlLogger:
    def __init__(self, path: str):
        self._path = path
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._fh = None

    def open(self) -> None:
        self._fh = open(self._path, "a", encoding="utf-8")

    def write(self, obj: Dict[str, Any]) -> None:
        line = json.dumps(obj, ensure_ascii=False, default=str)
        if self._fh is not None:
            self._fh.write(line + "\n")
            self._fh.flush()
        else:
            # fallback: open-write-close (safe if open() not called)
            with open(self._path, "a", encoding="utf-8") as f:
                f.write(line + "\n")

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None


# ─────────────────────────────────────────────────────────────────────────────
# Prometheus text parser  (zero external dependencies)
# ─────────────────────────────────────────────────────────────────────────────

_SAMPLE_RE = re.compile(
    r'^(?P<metric>[a-zA-Z_:][a-zA-Z0-9_:]*)(?P<labels>\{[^}]*\})?\s+'
    r'(?P<value>[^\s]+)(?:\s+(?P<ts>\d+))?$'
)
_LABEL_RE = re.compile(r'(\w+)="([^"]*)"')


def parse_prometheus_text(text: str) -> Dict[str, list]:
    result: Dict[str, list] = defaultdict(list)
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = _SAMPLE_RE.match(line)
        if not m:
            continue
        metric = m.group("metric")
        labels_str = m.group("labels") or ""
        raw_val = m.group("value")
        try:
            value = float(raw_val)
        except ValueError:
            continue
        if math.isnan(value):
            value = float("nan")
        labels = dict(_LABEL_RE.findall(labels_str))
        result[metric].append((labels, value))
    return result


def _sum_samples(samples: list) -> Optional[float]:
    if not samples:
        return None
    vals = [v for _, v in samples if not math.isnan(v)]
    return sum(vals) if vals else None


def get_histogram_mean_p99(base_name: str, metrics: Dict[str, list]) -> Tuple[Optional[float], Optional[float]]:
    """
    Returns (mean, p99) from cumulative histogram _sum/_count/_bucket.
    mean = sum / count  (cumulative lifetime average, analogous to prom rate(_sum)/rate(_count))
    p99  = interpolated from bucket boundaries
    """
    total_count = _sum_samples(metrics.get(base_name + "_count", []))
    total_sum   = _sum_samples(metrics.get(base_name + "_sum", []))

    mean = None
    if total_count and total_count > 0 and total_sum is not None:
        mean = total_sum / total_count

    p99 = None
    bucket_samples = metrics.get(base_name + "_bucket", [])
    if bucket_samples and total_count and total_count > 0:
        le_map: Dict[str, float] = defaultdict(float)
        for labels, v in bucket_samples:
            le_map[labels.get("le", "+Inf")] += v

        sorted_buckets = []
        for le_str, cum in le_map.items():
            try:
                le_f = float(le_str) if le_str != "+Inf" else float("inf")
            except ValueError:
                continue
            sorted_buckets.append((le_f, cum))
        sorted_buckets.sort(key=lambda x: x[0])

        target = 0.99 * total_count
        for i, (le_f, cum) in enumerate(sorted_buckets):
            if cum >= target:
                if i == 0:
                    p99 = le_f
                else:
                    prev_le, prev_cum = sorted_buckets[i - 1]
                    if cum > prev_cum:
                        frac = (target - prev_cum) / (cum - prev_cum)
                        p99 = prev_le + frac * (le_f - prev_le)
                    else:
                        p99 = le_f
                break

    return mean, p99


# ─────────────────────────────────────────────────────────────────────────────
# Metric collector
# ─────────────────────────────────────────────────────────────────────────────

class VllmMetricsCollector:
    def __init__(self, host: str, timeout: int = 10):
        self.host = host.rstrip("/")
        self.metrics_url = self.host + "/metrics"
        self.timeout = timeout
        # Stores previous raw cumulative counter values for rate computation.
        # Key: prometheus metric name (not field name).
        self._prev_counters: Dict[str, float] = {}
        self._prev_ts: Optional[float] = None

    def fetch_raw(self) -> str:
        req = urllib.request.Request(self.metrics_url)
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            return resp.read().decode("utf-8")

    def collect(self) -> Dict[str, Any]:
        """
        Fetch /metrics and return a flat sample dict.

        Field semantics — consistent with prom_utils.py and metrics_prom.py:
          gauge fields       -> plain float
          counter_rate fields -> plain float/s  (delta / dt)
          histogram _avg fields -> plain float  (cumulative mean = sum/count)
          histogram _avg__p99 fields -> plain float  (p99 estimate; vllm_monitor extension)
          counter_cumulative fields -> plain float  (raw cumulative; prefix cache extras)
          derived fields     -> plain float  (hit_rate, free_frac, acceptance_rate)
        """
        now_ts = time.time()
        raw = self.fetch_raw()
        metrics = parse_prometheus_text(raw)
        dt = (now_ts - self._prev_ts) if self._prev_ts else None

        fields: Dict[str, Any] = {}
        # Track which field_names have been written (alias dedup).
        seen_fields: Dict[str, bool] = {}
        # Accumulate raw counter values keyed by prometheus metric name for next-poll delta.
        new_counters: Dict[str, float] = {}

        for metric_name, field_name, metric_type in METRIC_TARGETS:

            if metric_type == "gauge":
                if field_name in seen_fields:
                    continue
                val = _sum_samples(metrics.get(metric_name, []))
                if val is not None:
                    fields[field_name] = val
                    seen_fields[field_name] = True

            elif metric_type == "histogram":
                if field_name in seen_fields:
                    continue
                mean, p99 = get_histogram_mean_p99(metric_name, metrics)
                if mean is not None:
                    # Store mean as plain float — matches prom_utils hist_avg field type.
                    fields[field_name] = mean
                    # Store p99 as a companion field (extension; not in prom_utils).
                    if p99 is not None and not math.isinf(p99):
                        fields[field_name + "__p99"] = p99
                    seen_fields[field_name] = True

            elif metric_type == "counter_rate":
                # Read raw cumulative counter; derive rate as delta/dt.
                # Matches semantics of prom_utils rate() queries.
                if field_name in seen_fields:
                    continue
                raw_val = None
                for candidate in (metric_name,
                                   metric_name + "_total",
                                   metric_name.removesuffix("_total")):
                    v = _sum_samples(metrics.get(candidate, []))
                    if v is not None:
                        raw_val = v
                        new_counters[metric_name] = v
                        break
                if raw_val is not None and dt and dt > 0:
                    prev = self._prev_counters.get(metric_name)
                    if prev is not None:
                        fields[field_name] = round(max(0.0, raw_val - prev) / dt, 4)
                        seen_fields[field_name] = True
                    # On first poll (no prev): field is absent this tick, consistent with
                    # Prometheus rate() returning no data before 2 scrapes.

            elif metric_type == "counter_cumulative":
                # Store raw cumulative value (prefix cache extras not in prom_utils).
                if field_name in seen_fields:
                    continue
                raw_val = None
                for candidate in (metric_name,
                                   metric_name + "_total",
                                   metric_name.removesuffix("_total")):
                    v = _sum_samples(metrics.get(candidate, []))
                    if v is not None:
                        raw_val = v
                        new_counters[metric_name] = v
                        break
                if raw_val is not None:
                    fields[field_name] = raw_val
                    seen_fields[field_name] = True

        # ── Derived fields ──────────────────────────────────────────────────

        # GPU KV cache free fraction.
        gpu_usage = fields.get("gpu_kv_cache_usage_frac")
        if gpu_usage is not None:
            fields["gpu_kv_cache_free_frac"] = round(1.0 - gpu_usage, 4)

        # Prefix cache hit rate (interval rate from cumulative counters).
        q     = fields.get("prefix_cache_queries")
        h     = fields.get("prefix_cache_hits")
        q_prev = self._prev_counters.get("vllm:prefix_cache_queries")
        h_prev = self._prev_counters.get("vllm:prefix_cache_hits")
        if q is not None and h is not None and q_prev is not None and h_prev is not None:
            dq = q - q_prev
            dh = h - h_prev
            fields["prefix_cache_hit_rate"] = round(dh / dq, 4) if dq > 0 else None
        elif q is not None and q > 0 and h is not None:
            fields["prefix_cache_hit_rate_cumulative"] = round(h / q, 4)

        # External prefix cache hit rate (KV Connector: OffloadingConnector / LMCache).
        eq      = fields.get("external_prefix_cache_queries")
        eh      = fields.get("external_prefix_cache_hits")
        eq_prev = self._prev_counters.get("vllm:external_prefix_cache_queries_total")
        eh_prev = self._prev_counters.get("vllm:external_prefix_cache_hits_total")
        if eq is not None and eh is not None and eq_prev is not None and eh_prev is not None:
            deq = eq - eq_prev
            deh = eh - eh_prev
            fields["external_prefix_cache_hit_rate"] = round(deh / deq, 4) if deq > 0 else None
        elif eq is not None and eq > 0 and eh is not None:
            fields["external_prefix_cache_hit_rate_cumulative"] = round(eh / eq, 4)

        # Speculative decoding acceptance rate (accepted_per_sec / draft_per_sec).
        acc  = fields.get("spec_tokens_accepted_per_sec")
        dft  = fields.get("spec_tokens_draft_per_sec")
        if acc is not None and dft is not None and dft > 0:
            fields["spec_decode_acceptance_rate"] = round(acc / dft, 4)

        # ── Update counter state for next poll ──────────────────────────────
        self._prev_counters.update(new_counters)
        self._prev_ts = now_ts

        return fields


# ─────────────────────────────────────────────────────────────────────────────
# Pretty summary printer
# ─────────────────────────────────────────────────────────────────────────────

def _fmt(v: Any, pct: bool = False, unit: str = "") -> str:
    if v is None:
        return "N/A"
    if isinstance(v, float) and math.isnan(v):
        return "NaN"
    if pct:
        return f"{v * 100:.1f}%"
    if isinstance(v, float):
        return f"{v:.4f}{unit}"
    return str(v)


def _lat_line(fields: Dict, key: str) -> str:
    mean = fields.get(key)
    p99  = fields.get(key + "__p99")
    if mean is None:
        return "N/A"
    s = f"avg={_fmt(mean, unit='s')}"
    if p99 is not None:
        s += f"  p99={_fmt(p99, unit='s')}"
    return s


def print_summary(ts: str, fields: Dict) -> None:
    lines = [
        f"\n{'─'*66}",
        f"  vLLM Monitor  |  {ts}",
        f"{'─'*66}",

        "  +- KV Cache / Prefix Cache -------------------------------------+",
        f"  |  GPU KV cache usage         {_fmt(fields.get('gpu_kv_cache_usage_frac'), pct=True):>8}",
        f"  |  GPU KV cache free          {_fmt(fields.get('gpu_kv_cache_free_frac'), pct=True):>8}",
        f"  |  CPU KV cache usage         {_fmt(fields.get('cpu_kv_cache_usage_frac'), pct=True):>8}",
        f"  |  Prefix hit rate (interval) {_fmt(fields.get('prefix_cache_hit_rate'), pct=True):>8}",
        f"  |  Prefix hit rate (lifetime) {_fmt(fields.get('prefix_cache_hit_rate_cumulative'), pct=True):>8}",
        f"  |  Prefix queries (total)     {_fmt(fields.get('prefix_cache_queries')):>8}",
        f"  |  Prefix hits    (total)     {_fmt(fields.get('prefix_cache_hits')):>8}",
        f"  |  Ext hit rate (interval)    {_fmt(fields.get('external_prefix_cache_hit_rate'), pct=True):>8}",
        f"  |  Ext hit rate (lifetime)    {_fmt(fields.get('external_prefix_cache_hit_rate_cumulative'), pct=True):>8}",
        f"  |  Ext queries  (total)       {_fmt(fields.get('external_prefix_cache_queries')):>8}",
        f"  |  Ext hits     (total)       {_fmt(fields.get('external_prefix_cache_hits')):>8}",
        "  +---------------------------------------------------------------+",

        "  +- Request Queue -----------------------------------------------+",
        f"  |  Running               {_fmt(fields.get('requests_running')):>10}",
        f"  |  Waiting               {_fmt(fields.get('requests_waiting')):>10}",
        f"  |  Preemptions/s         {_fmt(fields.get('preemptions_per_sec')):>10}",
        "  +---------------------------------------------------------------+",

        "  +- Latency (cumulative avg / p99) ------------------------------+",
        f"  |  E2E latency        {_lat_line(fields, 'e2e_latency_seconds_avg')}",
        f"  |  TTFT               {_lat_line(fields, 'ttft_seconds_avg')}",
        f"  |    Queue time       {_lat_line(fields, 'queue_time_seconds_avg')}",
        f"  |    Prefill time     {_lat_line(fields, 'prefill_time_seconds_avg')}",
        f"  |  Inference time     {_lat_line(fields, 'inference_time_seconds_avg')}",
        f"  |  Decode total       {_lat_line(fields, 'decode_time_seconds_avg')}",
        f"  |  TPOT               {_lat_line(fields, 'tpot_seconds_avg')}",
        "  +---------------------------------------------------------------+",

        "  +- Token Stats (rates match prom_utils counter_rate) -----------+",
        f"  |  Prompt tokens/req  avg={_fmt(fields.get('request_prompt_tokens_avg'), unit=' tok')}",
        f"  |  Gen tokens/req     avg={_fmt(fields.get('request_generation_tokens_avg'), unit=' tok')}",
        f"  |  Max gen budget     avg={_fmt(fields.get('request_max_generation_tokens_avg'), unit=' tok')}",
        f"  |  Prefill tok/s      {_fmt(fields.get('prefill_tokens_per_sec')):>10}",
        f"  |  Gen tok/s          {_fmt(fields.get('gen_tokens_per_sec')):>10}",
        f"  |  Successes/s        {_fmt(fields.get('request_success_per_sec')):>10}",
        f"  |  Failures/s         {_fmt(fields.get('request_failure_per_sec')):>10}",
        "  +---------------------------------------------------------------+",

        "  +- Speculative Decoding (N/A if not enabled) -------------------+",
        f"  |  Acceptance rate    {_fmt(fields.get('spec_decode_acceptance_rate'), pct=True):>10}",
        f"  |  Accepted tok/s     {_fmt(fields.get('spec_tokens_accepted_per_sec')):>10}",
        f"  |  Draft tok/s        {_fmt(fields.get('spec_tokens_draft_per_sec')):>10}",
        f"  |  Emitted tok/s      {_fmt(fields.get('spec_tokens_emitted_per_sec')):>10}",
        "  +---------------------------------------------------------------+",
        "",
    ]
    print("\n".join(lines), flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# Graceful shutdown
# ─────────────────────────────────────────────────────────────────────────────

_running = True

def _handle_signal(signum, frame):
    global _running
    _running = False


# ─────────────────────────────────────────────────────────────────────────────
# Main loop
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Continuous vLLM /metrics monitor — prom_utils/metrics_prom compatible JSONL")
    parser.add_argument(
        "--host", default=os.environ.get("VLLM_HOST", "http://localhost:8000"),
        help="vLLM server base URL  (env: VLLM_HOST)")
    parser.add_argument(
        "--interval", type=float,
        default=float(os.environ.get("VLLM_INTERVAL", "15")),
        help="Poll interval in seconds  (env: VLLM_INTERVAL, default: 15)")
    parser.add_argument(
        "--log-dir", type=Path,
        default=Path(os.environ.get("VLLM_LOG_DIR", "./vllm_logs")),
        help="Directory for log files  (env: VLLM_LOG_DIR, default: ./vllm_logs)")
    parser.add_argument(
        "--mode", default="monitor",
        help="Mode label in each JSONL record (default: monitor)")
    parser.add_argument(
        "--timeout", type=int, default=10,
        help="HTTP request timeout in seconds (default: 10)")
    parser.add_argument(
        "--no-stdout", action="store_true",
        help="Suppress live summary on stdout (useful when daemonising)")
    parser.add_argument(
        "--summary-every", type=int, default=1,
        help="Print live summary every N polls (default: 1)")
    args = parser.parse_args()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    args.log_dir.mkdir(parents=True, exist_ok=True)

    jsonl = JsonlLogger(str(args.log_dir / "metrics.jsonl"))
    jsonl.open()

    ops_log_path = args.log_dir / "vllm_monitor.log"

    def ops_log(msg: str) -> None:
        ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        line = f"{ts}  {msg}"
        print(line, flush=True)
        with open(ops_log_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    ops_log("=" * 66)
    ops_log(f"vLLM Monitor v{VERSION} starting")
    ops_log(f"  Target  : {args.host}/metrics")
    ops_log(f"  Interval: {args.interval}s")
    ops_log(f"  Mode    : {args.mode}")
    ops_log(f"  Log dir : {args.log_dir.resolve()}")
    ops_log("=" * 66)

    collector = VllmMetricsCollector(args.host, timeout=args.timeout)
    poll_count = 0
    consecutive_failures = 0
    MAX_BACKOFF = 120

    try:
        while _running:
            loop_start = time.monotonic()
            ts = datetime.now(tz=timezone.utc).isoformat()
            try:
                fields = collector.collect()
                consecutive_failures = 0

                # JSONL record — matches prom_utils.py and metrics_prom.py exactly:
                # {"ts": "...", "mode": "...", "samples": [{"instance": "...", ...fields...}]}
                sample = {"instance": collector.host}
                sample.update(fields)
                jsonl.write({"ts": ts, "mode": args.mode, "samples": [sample]})

                poll_count += 1
                if not args.no_stdout and (poll_count % args.summary_every == 0):
                    print_summary(ts, fields)

            except urllib.error.URLError as exc:
                consecutive_failures += 1
                backoff = min(args.interval * consecutive_failures, MAX_BACKOFF)
                ops_log(f"ERROR  Fetch failed (attempt {consecutive_failures}): {exc} "
                        f"— retrying in {backoff:.0f}s")
                jsonl.write({
                    "ts":                   ts,
                    "mode":                 args.mode,
                    "samples":              [],
                    "error":                str(exc),
                    "consecutive_failures": consecutive_failures,
                })
                time.sleep(backoff)
                continue

            except Exception as exc:
                ops_log(f"ERROR  Unexpected: {exc}")

            elapsed = time.monotonic() - loop_start
            if _running:
                time.sleep(max(0.0, args.interval - elapsed))

    finally:
        jsonl.close()
        ops_log("vLLM monitor stopped.")


if __name__ == "__main__":
    main()
