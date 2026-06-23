#!/usr/bin/env python3
"""
prod_latency_collector.py
~~~~~~~~~~~~~~~~~~~~~~~~~
Continuously polls the router's /latency_log endpoint and persists
per-request latency records into the standard experiments directory
structure (same layout as load_runner / main.py).

Usage:
    python prod_latency_collector.py --router-url http://10.50.156.65:30080

    # Custom experiments root & poll interval
    python prod_latency_collector.py \
        --router-url http://10.50.156.65:30080 \
        --experiments-root /home/data/saeid/experiments \
        --poll-interval 5

    # Also scrape Prometheus histograms periodically
    python prod_latency_collector.py \
        --router-url http://10.50.156.65:30080 \
        --prometheus-url http://10.50.156.65:31190

Output (in <experiments_root>/<N>/):
    config.json           - collector settings + start time
    logs.json             - NDJSON, one line per request (same schema as load_runner)
    run_summary.json      - written on shutdown with total counts and wall time
    metrics.jsonl         - optional: periodic Prometheus histogram snapshots
    endpoint_tokens.json  - per-endpoint token rollup (written on shutdown)
"""

from __future__ import annotations

import argparse
import collections
import json
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import requests


# ============================================================
# Experiment directory helpers (same logic as experiment_io.py)
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


class NdjsonLogger:
    """Thread-safe NDJSON writer with automatic file rotation."""

    _MAX_BYTES = 100 * 1024 * 1024  # 100 MB per file

    def __init__(self, path: Path):
        self._base_path = path
        self._dir = path.parent
        self._lock = threading.Lock()
        self._fh = open(path, "a", encoding="utf-8")
        self._count = 0
        self._file_bytes = 0
        self._file_idx = 0

    def _rotate(self) -> None:
        self._fh.close()
        self._file_idx += 1
        rotated = self._dir / f"logs_{self._file_idx}.json"
        self._fh = open(rotated, "a", encoding="utf-8")
        self._file_bytes = 0

    def append(self, record: dict) -> None:
        line = json.dumps(record, ensure_ascii=False, default=str)
        encoded = (line + "\n").encode("utf-8")
        with self._lock:
            self._fh.write(line + "\n")
            self._fh.flush()
            self._count += 1
            self._file_bytes += len(encoded)
            if self._file_bytes >= self._MAX_BYTES:
                self._rotate()

    @property
    def count(self) -> int:
        with self._lock:
            return self._count

    def close(self) -> None:
        with self._lock:
            self._fh.close()


# ============================================================
# Prometheus metrics scraper (via PromQL API)
# Same format as metrics_prom.py: {ts, mode, instances, samples}
# ============================================================

_RATE_WINDOW = "30s"

# (field_name, promql, label_key)
# label_key determines which Prometheus label maps to "instance" in output
_METRICS_CATALOG = [
    # --- vLLM gauges ---
    ("requests_running",    "vllm:num_requests_running",    "instance"),
    ("requests_waiting",    "vllm:num_requests_waiting",    "instance"),
    ("kv_cache_usage_perc", "vllm:gpu_cache_usage_perc",    "instance"),
    # --- vLLM counter rates ---
    ("request_success_per_sec",  "rate(vllm:request_success_total[{w}])",  "instance"),
    ("preemptions_per_sec",      "rate(vllm:num_preemptions_total[{w}])",  "instance"),
    ("gen_tokens_per_sec",       "rate(vllm:generation_tokens_total[{w}])",       "instance"),
    ("prefill_tokens_per_sec",   "rate(vllm:prompt_tokens_total[{w}])",           "instance"),
    ("prefix_cache_hits_per_sec",   "rate(vllm:prefix_cache_hits_total[{w}])",    "instance"),
    ("prefix_cache_queries_per_sec","rate(vllm:prefix_cache_queries_total[{w}])", "instance"),
    ("ext_prefix_cache_hits_per_sec",   "rate(vllm:external_prefix_cache_hits_total[{w}])",    "instance"),
    ("ext_prefix_cache_queries_per_sec","rate(vllm:external_prefix_cache_queries_total[{w}])", "instance"),
    ("prompt_tokens_cached_per_sec",    "rate(vllm:prompt_tokens_cached_total[{w}])",          "instance"),
    # --- vLLM histogram avgs ---
    ("ttft_seconds_avg",       "rate(vllm:time_to_first_token_seconds_sum[{w}]) / rate(vllm:time_to_first_token_seconds_count[{w}])",   "instance"),
    ("tpot_seconds_avg",       "rate(vllm:time_per_output_token_seconds_sum[{w}]) / rate(vllm:time_per_output_token_seconds_count[{w}])", "instance"),
    ("e2e_latency_seconds_avg","rate(vllm:e2e_request_latency_seconds_sum[{w}]) / rate(vllm:e2e_request_latency_seconds_count[{w}])",     "instance"),
    # --- Sidecar ---
    ("sidecar_queue_length",   "sidecar_queue_length",              "endpoint"),
    ("sidecar_received_rps",   "rate(sidecar_received_requests_total[{w}])", "endpoint"),
    ("sidecar_completed_rps",  "rate(sidecar_completed_requests_total[{w}])","endpoint"),
    ("sidecar_workers_total",  "sidecar_workers_total",             "endpoint"),
    ("sidecar_workers_busy",   "sidecar_workers_busy",              "endpoint"),
    ("sidecar_python_threads", "sidecar_python_threads",            "pod"),
    # --- Router ---
    ("router_queue_length",    "router_central_queue_length",       "instance"),
    ("router_admission_rps",   "rate(router_admission_requests_total[{w}])", "instance"),
    ("router_outgoing_rps",    "rate(router_dispatch_requests_total[{w}])",  "instance"),
    # --- Router latency histograms (new, per-model p50/p95) ---
    ("router_ttft_p50",     "histogram_quantile(0.50, rate(router_request_ttft_seconds_bucket[{w}]))",     "model"),
    ("router_ttft_p95",     "histogram_quantile(0.95, rate(router_request_ttft_seconds_bucket[{w}]))",     "model"),
    ("router_tpot_avg_p50", "histogram_quantile(0.50, rate(router_request_tpot_avg_seconds_bucket[{w}]))", "model"),
    ("router_tpot_avg_p95", "histogram_quantile(0.95, rate(router_request_tpot_avg_seconds_bucket[{w}]))", "model"),
    ("router_e2e_p50",      "histogram_quantile(0.50, rate(router_request_e2e_seconds_bucket[{w}]))",      "model"),
    ("router_e2e_p95",      "histogram_quantile(0.95, rate(router_request_e2e_seconds_bucket[{w}]))",      "model"),
]


def _prom_query(base_url: str, promql: str) -> List[Dict[str, Any]]:
    url = f"{base_url.rstrip('/')}/api/v1/query"
    r = requests.get(url, params={"query": promql}, timeout=10)
    r.raise_for_status()
    payload = r.json()
    if payload.get("status") != "success":
        return []
    return payload.get("data", {}).get("result", []) or []


def _vec_to_map(results: List[Dict[str, Any]], label_key: str) -> Dict[str, float]:
    """Convert a Prometheus result vector into {label_value: float}."""
    out: Dict[str, float] = {}
    for r in results:
        metric = r.get("metric", {})
        key = metric.get(label_key, "")
        if not key:
            continue
        val = r.get("value", [None, None])
        try:
            out[key] = float(val[1])
        except (TypeError, ValueError, IndexError):
            continue
    return out


def _scrape_prometheus(prom_url: str) -> Optional[Dict[str, Any]]:
    """
    Query Prometheus API and produce a tick in the same format as
    metrics_prom.py: {ts, mode, instances, samples: [{instance, pod, ...}, ...]}.
    """
    per_inst: Dict[str, Dict[str, Any]] = {}
    instances_ordered: List[str] = []

    def _ensure(inst: str) -> Dict[str, Any]:
        if inst not in per_inst:
            per_inst[inst] = {"instance": inst}
            instances_ordered.append(inst)
        return per_inst[inst]

    has_data = False

    for field, promql_template, label_key in _METRICS_CATALOG:
        promql = promql_template.replace("{w}", _RATE_WINDOW)
        try:
            results = _prom_query(prom_url, promql)
            m = _vec_to_map(results, label_key)
        except Exception:
            continue

        for label_val, value in m.items():
            has_data = True
            rec = _ensure(label_val)
            rec[field] = value

    if not has_data:
        return None

    # Broadcast aggregated router scalars into vLLM rows (same as metrics_prom.py)
    router_q = None
    router_adm = None
    router_out = None
    for inst, rec in per_inst.items():
        if "router_queue_length" in rec:
            rq = rec.get("router_queue_length")
            if rq is not None:
                router_q = max(router_q, rq) if router_q is not None else rq
        if "router_admission_rps" in rec:
            ra = rec.get("router_admission_rps")
            if ra is not None:
                router_adm = (router_adm or 0) + ra
        if "router_outgoing_rps" in rec:
            ro = rec.get("router_outgoing_rps")
            if ro is not None:
                router_out = (router_out or 0) + ro

    for inst, rec in per_inst.items():
        if "requests_running" in rec:
            if router_q is not None:
                rec.setdefault("router_queue_length", router_q)
            if router_adm is not None:
                rec.setdefault("router_admission_rps", router_adm)
            if router_out is not None:
                rec.setdefault("router_outgoing_rps", router_out)

    # Derive prefix_cache_hit_rate
    for rec in per_inst.values():
        queries = rec.get("prefix_cache_queries_per_sec")
        hits = rec.get("prefix_cache_hits_per_sec")
        if queries and queries > 0 and hits is not None:
            rec["prefix_cache_hit_rate"] = hits / queries
        else:
            rec["prefix_cache_hit_rate"] = None

        ext_queries = rec.get("ext_prefix_cache_queries_per_sec")
        ext_hits = rec.get("ext_prefix_cache_hits_per_sec")
        if ext_queries and ext_queries > 0 and ext_hits is not None:
            rec["ext_prefix_cache_hit_rate"] = ext_hits / ext_queries
        else:
            rec["ext_prefix_cache_hit_rate"] = None

    return {
        "ts": datetime.now(timezone.utc).isoformat(),
        "mode": "collector",
        "instances": instances_ordered,
        "samples": [per_inst[k] for k in instances_ordered],
    }


# ============================================================
# Per-endpoint token rollup (mirrors trace_utils.summarize_endpoint_tokens)
# ============================================================

def _summarize_endpoint_tokens(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    eps: Dict[str, Dict[str, int]] = {}
    total_prompt = 0
    total_completion = 0
    total_reqs = 0

    for r in records:
        eid = r.get("endpoint_id") or r.get("endpoint") or "_unknown_"
        pt = int(r.get("prompt_tokens", 0))
        ct = int(r.get("completion_tokens", 0))

        if eid not in eps:
            eps[eid] = {"requests": 0, "prefill_tokens": 0, "decode_tokens": 0}
        eps[eid]["requests"] += 1
        eps[eid]["prefill_tokens"] += pt
        eps[eid]["decode_tokens"] += ct

        total_prompt += pt
        total_completion += ct
        total_reqs += 1

    per_endpoint = []
    for ep, s in sorted(eps.items()):
        per_endpoint.append({
            "endpoint": ep,
            "requests": s["requests"],
            "prefill_tokens": s["prefill_tokens"],
            "decode_tokens": s["decode_tokens"],
            "total_tokens": s["prefill_tokens"] + s["decode_tokens"],
        })

    return {
        "total_records": total_reqs,
        "total_prefill_tokens": total_prompt,
        "total_decode_tokens": total_completion,
        "total_requests": total_reqs,
        "per_endpoint": per_endpoint,
    }


# ============================================================
# Main collector loop
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Continuously collect per-request latency from the router /latency_log endpoint."
    )
    parser.add_argument("--router-url", default="http://10.50.156.65:30080",
                        help="Router base URL (default: http://10.50.156.65:30080)")
    parser.add_argument("--experiments-root", default="/home/data/saeid/experiments",
                        help="Root directory for experiment outputs (default: /home/data/saeid/experiments)")
    parser.add_argument("--poll-interval", type=float, default=5.0,
                        help="Seconds between polls (default: 5)")
    parser.add_argument("--prometheus-url", default="http://10.50.156.65:31190",
                        help="Prometheus /metrics URL (default: http://10.50.156.65:31190)")
    parser.add_argument("--prom-interval", type=float, default=30.0,
                        help="Seconds between Prometheus scrapes (default: 30)")
    parser.add_argument("--batch-size", type=int, default=2000,
                        help="Number of records to fetch per poll (default: 2000)")
    args = parser.parse_args()

    router_url = args.router_url.rstrip("/")
    latency_url = f"{router_url}/latency_log?last={args.batch_size}"

    # Create experiment directory
    exp_dir = _next_experiment_dir(Path(args.experiments_root))
    print(f"[collector] experiment dir: {exp_dir}")

    # Write config.json
    t_start = time.time()
    config_out = {
        "created_at_unix": t_start,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "collector": "prod_latency_collector",
        "router_url": router_url,
        "poll_interval_s": args.poll_interval,
        "prometheus_url": args.prometheus_url,
        "batch_size": args.batch_size,
        "client_config": {
            "backend": "prod-collector",
            "router_url": router_url,
        },
    }
    with (exp_dir / "config.json").open("w") as f:
        json.dump(config_out, f, indent=2, sort_keys=True)

    # Open logs.json
    logger = NdjsonLogger(exp_dir / "logs.json")

    # Optional metrics.jsonl
    metrics_fh = None
    if args.prometheus_url:
        metrics_fh = open(exp_dir / "metrics.jsonl", "a", encoding="utf-8")

    # Bounded dedup window -- keeps last 10k keys to cap memory
    _DEDUP_MAX = 10_000
    seen_rids: collections.OrderedDict = collections.OrderedDict()
    all_records: List[Dict[str, Any]] = []
    shutdown = threading.Event()
    last_prom_scrape = 0.0

    def _handle_signal(signum, frame):
        print(f"\n[collector] caught signal {signum}, shutting down...")
        shutdown.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    print(f"[collector] polling {latency_url} every {args.poll_interval}s")
    print(f"[collector] press Ctrl+C to stop and write summary")

    poll_count = 0
    while not shutdown.is_set():
        # --- Poll /latency_log ---
        try:
            resp = requests.get(latency_url, timeout=10)
            resp.raise_for_status()
            entries: List[Dict[str, Any]] = resp.json()
        except Exception as e:
            print(f"[collector] poll error: {e}")
            shutdown.wait(args.poll_interval)
            continue

        new_count = 0
        for entry in entries:
            rid = entry.get("rid", "")
            ts = entry.get("ts", 0)
            dedup_key = f"{rid}:{ts}"
            if dedup_key in seen_rids:
                continue
            seen_rids[dedup_key] = None
            if len(seen_rids) > _DEDUP_MAX:
                seen_rids.popitem(last=False)

            e2e_s = (entry.get("e2e_ms") or 0) / 1000.0
            t_start = entry.get("t_start")
            t0 = t_start if t_start is not None else ts
            record = {
                "idx": logger.count,
                "req_id": rid,
                "t0_wall": t0,
                "t1_wall": ts,
                "end_to_end_s": e2e_s,
                "model_latency_s": e2e_s,
                "finish_reason": entry.get("finish_reason", "stop"),
                "endpoint_id": entry.get("endpoint"),
                "streaming": entry.get("stream", False),
                "prompt_tokens": entry.get("prompt_tokens", 0),
                "completion_tokens": entry.get("completion_tokens", 0),
            }

            ttft_ms = entry.get("ttft_ms")
            if ttft_ms is not None:
                record["ttft_s"] = ttft_ms / 1000.0

            tpot_ms = entry.get("tpot_avg_ms")
            if tpot_ms is not None:
                record["tpot_avg_s"] = tpot_ms / 1000.0

            model = entry.get("model")
            if model:
                record["model"] = model

            logger.append(record)
            all_records.append(record)
            new_count += 1

        poll_count += 1
        if new_count > 0:
            print(f"[collector] poll #{poll_count}: +{new_count} new records "
                  f"(total: {logger.count})")

        # --- Optional Prometheus scrape ---
        now = time.time()
        if metrics_fh and args.prometheus_url and (now - last_prom_scrape) >= args.prom_interval:
            snapshot = _scrape_prometheus(args.prometheus_url)
            if snapshot:
                metrics_fh.write(json.dumps(snapshot, default=str) + "\n")
                metrics_fh.flush()
            last_prom_scrape = now

        shutdown.wait(args.poll_interval)

    # ---- Shutdown: write summary files ----
    logger.close()
    if metrics_fh:
        metrics_fh.close()

    dt_wall = time.time() - t_start

    # endpoint_tokens.json
    try:
        token_summary = _summarize_endpoint_tokens(all_records)
        with (exp_dir / "endpoint_tokens.json").open("w") as f:
            json.dump(token_summary, f, indent=2, sort_keys=True)
    except Exception as e:
        print(f"[collector] WARN: failed to write endpoint_tokens.json: {e}")

    # run_summary.json
    run_summary = {
        "total_requests": logger.count,
        "backend": "prod-collector",
        "load_runner_duration_s": round(dt_wall, 3),
        "wall_time_s": round(dt_wall, 3),
        "transport_mode": "latency_log_poll",
        "router_url": router_url,
        "poll_count": poll_count,
        "poll_interval_s": args.poll_interval,
        "prometheus_url": args.prometheus_url,
    }
    try:
        with (exp_dir / "run_summary.json").open("w") as f:
            json.dump(run_summary, f, indent=2, sort_keys=True)
    except Exception as e:
        print(f"[collector] WARN: failed to write run_summary.json: {e}")

    print(f"[collector] done. {logger.count} records in {dt_wall:.1f}s")
    print(f"[collector] results: {exp_dir}")


if __name__ == "__main__":
    main()
