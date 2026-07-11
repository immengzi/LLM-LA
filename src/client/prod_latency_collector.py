#!/usr/bin/env python3
"""
prod_latency_collector.py
~~~~~~~~~~~~~~~~~~~~~~~~~
Continuously polls the router's /latency_log endpoint and persists
per-request latency records into the standard experiments directory
structure (same layout as load_runner / main.py).

Usage:
    python src/client/prod_latency_collector.py --router-url http://10.50.156.65:30080

    # Custom experiments root & poll interval
    python src/client/prod_latency_collector.py \
        --router-url http://10.50.156.65:30080 \
        --experiments-root /home/data/saeid/experiments \
        --poll-interval 5

    # Also scrape Prometheus histograms periodically
    python src/client/prod_latency_collector.py \
        --router-url http://10.50.156.65:30080 \
        --prometheus-url http://10.50.156.65:31190

Output (in <experiments_root>/<N>/):
    config.json           - collector settings + start time
    logs.json             - NDJSON, one line per request (same schema as load_runner);
                            streamed LIVE during the run (standard primary artifact)
    router_logs.json      - router /latency_log truth; mirrored from logs.json at
                            shutdown for backward-compat (client runs ship both)
    run_summary.json      - written on shutdown with total counts and wall time
    metrics.jsonl         - optional: periodic Prometheus histogram snapshots
    endpoint_tokens.json  - per-endpoint token rollup (written on shutdown)
"""

from __future__ import annotations

import argparse
import json
import math
import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import requests

# Default experiments root: honor EXPERIMENTS_ROOT, else the client-relative
# experiments/ dir (same location the sweep writes to). The old hard-coded
# /home/data/... default is unwritable on most hosts.
_DEFAULT_EXPERIMENTS_ROOT = os.environ.get("EXPERIMENTS_ROOT") or str(
    Path(__file__).resolve().parent / "experiments"
)

from router_log_collector import (
    RouterLogCollector,
    join_logs_with_router,
    summarize_routing,
)


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


# ============================================================
# Prometheus metrics scraper (via PromQL API)
# Same format as metrics_prom.py: {ts, mode, instances, samples}
# ============================================================

_RATE_WINDOW = "30s"

# (field_name, promql, label_key)
# label_key determines which Prometheus label maps to "instance" in output
_METRICS_CATALOG = [
    # --- vLLM gauges (keyed by instance+engine for DP visibility) ---
    ("requests_running",    "vllm:num_requests_running",    "instance+engine"),
    ("requests_waiting",    "vllm:num_requests_waiting",    "instance+engine"),
    ("kv_cache_usage_perc", "vllm:gpu_cache_usage_perc",    "instance+engine"),
    # --- vLLM counter rates ---
    ("request_success_per_sec",  "rate(vllm:request_success_total[{w}])",  "instance+engine"),
    # Cumulative (all-time) finished-request counter, summed across the
    # finished_reason label in _vec_to_map so it matches the external scraper's
    # absolute request count (not just the windowed rate above).
    ("request_success_total",    "vllm:request_success_total",             "instance+engine"),
    ("preemptions_per_sec",      "rate(vllm:num_preemptions_total[{w}])",  "instance+engine"),
    ("gen_tokens_per_sec",       "rate(vllm:generation_tokens_total[{w}])",       "instance+engine"),
    ("prefill_tokens_per_sec",   "rate(vllm:prompt_tokens_total[{w}])",           "instance+engine"),
    ("prefix_cache_hits_per_sec",   "rate(vllm:prefix_cache_hits_total[{w}])",    "instance+engine"),
    ("prefix_cache_queries_per_sec","rate(vllm:prefix_cache_queries_total[{w}])", "instance+engine"),
    ("ext_prefix_cache_hits_per_sec",   "rate(vllm:external_prefix_cache_hits_total[{w}])",    "instance+engine"),
    ("ext_prefix_cache_queries_per_sec","rate(vllm:external_prefix_cache_queries_total[{w}])", "instance+engine"),
    ("prompt_tokens_cached_per_sec",    "rate(vllm:prompt_tokens_cached_total[{w}])",          "instance+engine"),
    # --- vLLM histogram avgs ---
    ("ttft_seconds_avg",       "rate(vllm:time_to_first_token_seconds_sum[{w}]) / rate(vllm:time_to_first_token_seconds_count[{w}])",   "instance+engine"),
    ("tpot_seconds_avg",       "rate(vllm:time_per_output_token_seconds_sum[{w}]) / rate(vllm:time_per_output_token_seconds_count[{w}])", "instance+engine"),
    ("e2e_latency_seconds_avg","rate(vllm:e2e_request_latency_seconds_sum[{w}]) / rate(vllm:e2e_request_latency_seconds_count[{w}])",     "instance+engine"),
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


import re as _re

def _inject_ns_filter(promql: str, ns_filter: str) -> str:
    """Inject a namespace label selector into metric selectors in a PromQL expression.

    Only injects before ``[`` (rate windows) or on standalone metric names.
    Function names like ``rate``, ``histogram_quantile`` are left untouched.
    """
    result = _re.sub(
        r'([a-zA-Z_:][a-zA-Z0-9_:]*)\[',
        lambda m: f'{m.group(1)}{{{ns_filter}}}[',
        promql,
    )
    if '[' not in promql and '(' not in promql:
        result = f'{promql}{{{ns_filter}}}'
    return result


def _prom_query(base_url: str, promql: str) -> List[Dict[str, Any]]:
    url = f"{base_url.rstrip('/')}/api/v1/query"
    r = requests.get(url, params={"query": promql}, timeout=10)
    r.raise_for_status()
    payload = r.json()
    if payload.get("status") != "success":
        return []
    return payload.get("data", {}).get("result", []) or []


def _vec_to_map(results: List[Dict[str, Any]], label_key: str) -> Dict[str, float]:
    """Convert a Prometheus result vector into {label_value: float}.

    label_key can be a single label name (e.g. "instance") or a
    '+'-separated compound key (e.g. "instance+engine") to produce
    composite keys like "7.150.0.37:8200:e1" when multiple series
    share the same primary label (typical for DP engines).
    """
    parts = label_key.split("+")
    out: Dict[str, float] = {}
    for r in results:
        metric = r.get("metric", {})
        primary = metric.get(parts[0], "")
        if not primary:
            continue
        if len(parts) > 1:
            suffix = ":".join(metric.get(p, "") for p in parts[1:])
            key = f"{primary}:{suffix}" if suffix else primary
        else:
            key = primary
        val = r.get("value", [None, None])
        try:
            v = float(val[1])
        except (TypeError, ValueError, IndexError):
            continue
        # Sum series that collapse to the same key (e.g. counters split by an
        # extra label like finished_reason on request_success_total). Metrics
        # with one series per key are unaffected. A lone NaN (e.g. a 0/0 average)
        # is preserved, but a real value wins over / adds past a NaN.
        if key in out:
            if math.isnan(out[key]):
                out[key] = v
            elif not math.isnan(v):
                out[key] += v
        else:
            out[key] = v
    return out


def _scrape_prometheus(prom_url: str, namespace: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Query Prometheus API and produce a tick in the same format as
    metrics_prom.py: {ts, mode, instances, samples: [{instance, pod, ...}, ...]}.

    If *namespace* is set, a ``{namespace="..."}`` selector is injected
    into every PromQL query so only metrics from that K8s namespace are
    returned.
    """
    per_inst: Dict[str, Dict[str, Any]] = {}
    instances_ordered: List[str] = []

    def _ensure(inst: str) -> Dict[str, Any]:
        if inst not in per_inst:
            per_inst[inst] = {"instance": inst}
            instances_ordered.append(inst)
        return per_inst[inst]

    ns_filter = ""
    if namespace:
        ns_filter = f'namespace="{namespace}"'

    has_data = False

    for field, promql_template, label_key in _METRICS_CATALOG:
        promql = promql_template.replace("{w}", _RATE_WINDOW)
        if ns_filter:
            promql = _inject_ns_filter(promql, ns_filter)
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


def _augment_derived_rps(
    snapshot: Dict[str, Any],
    prev_nq: Dict[str, tuple],
    now_ts: float,
) -> None:
    """
    Add a flow-balance ``derived_rps`` to each vLLM sample, in-place.

    This mirrors the estimate in prod_external_metrics_scraper.py so the two
    tools produce a comparable incoming-rate series. It is *independent* of the
    router-side ``router_admission_rps`` (which is kept as-is): here we infer
    arrivals purely from engine conservation:

        arrivals = departures + d(N)/dt,   N = running + waiting

    where departures is ``request_success_per_sec``. ``prev_nq`` carries the
    previous (N, ts) per instance across scrapes; the first tick yields None.
    """
    for rec in snapshot.get("samples", []):
        inst = rec.get("instance")
        rr = rec.get("requests_running")
        rw = rec.get("requests_waiting")
        succ = rec.get("request_success_per_sec")

        n_now = (rr + rw) if (rr is not None and rw is not None) else None
        derived = None
        prev = prev_nq.get(inst) if inst is not None else None
        if n_now is not None and succ is not None and prev is not None:
            n_prev, t_prev = prev
            dt = now_ts - t_prev
            if dt > 0:
                net_queue_growth = (n_now - n_prev) / dt
                derived = succ + net_queue_growth
                rec["net_queue_growth_per_sec"] = net_queue_growth
        rec["derived_rps"] = derived

        if inst is not None and n_now is not None:
            prev_nq[inst] = (n_now, now_ts)


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
    parser.add_argument("--experiments-root", default=_DEFAULT_EXPERIMENTS_ROOT,
                        help=f"Root directory for experiment outputs (default: {_DEFAULT_EXPERIMENTS_ROOT}; "
                             f"override with $EXPERIMENTS_ROOT or this flag)")
    parser.add_argument("--poll-interval", type=float, default=5.0,
                        help="Seconds between polls (default: 5)")
    parser.add_argument("--prometheus-url", default="http://10.50.156.65:31190",
                        help="Prometheus /metrics URL (default: http://10.50.156.65:31190)")
    parser.add_argument("--prom-interval", type=float, default=30.0,
                        help="Seconds between Prometheus scrapes (default: 30)")
    parser.add_argument("--batch-size", type=int, default=2000,
                        help="Number of records to fetch per poll (default: 2000)")
    parser.add_argument("--namespace", default=None,
                        help="K8s namespace filter for Prometheus queries (e.g. 'vllm')")
    parser.add_argument("--capture-pod-logs", dest="capture_pod_logs",
                        action="store_true", default=True,
                        help="Stream all container logs into <exp_dir>/vllm-logs/ (default: on)")
    parser.add_argument("--no-capture-pod-logs", dest="capture_pod_logs",
                        action="store_false",
                        help="Disable container log capture")
    parser.add_argument("--pod-log-namespace", default=None,
                        help="Namespace to capture container logs from (default: --namespace or 'vllm')")
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

    # Shared router /latency_log collector streamed straight into logs.json -- the
    # standard, primary artifact (same filename/schema as load_runner). In pure-
    # observation mode logs.json IS the router truth (endpoint + prefix/KV fields),
    # so it exists LIVE during the run, just like client sweeps. router_logs.json
    # is mirrored at shutdown for backward-compat (client runs ship both).
    logs_path = exp_dir / "logs.json"
    router_logs_path = exp_dir / "router_logs.json"
    collector = RouterLogCollector(
        router_url=router_url,
        out_path=logs_path,
        poll_interval_s=args.poll_interval,
        batch_size=args.batch_size,
    )

    # Optional metrics.jsonl
    metrics_fh = None
    if args.prometheus_url:
        metrics_fh = open(exp_dir / "metrics.jsonl", "a", encoding="utf-8")

    shutdown = threading.Event()
    last_prom_scrape = 0.0
    prev_nq: Dict[str, tuple] = {}  # instance -> (running+waiting, ts) for derived_rps

    def _handle_signal(signum, frame):
        print(f"\n[collector] caught signal {signum}, shutting down...")
        shutdown.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    collector.start()
    print(f"[collector] polling {latency_url} every {args.poll_interval}s")
    print(f"[collector] press Ctrl+C to stop and write summary")

    # Container log capture (vLLM/router/sidecar) -> <exp_dir>/vllm-logs/
    pod_log_streamer = None
    if args.capture_pod_logs:
        try:
            from pod_log_streamer import PodLogStreamer

            pod_log_ns = args.pod_log_namespace or args.namespace or "vllm"
            pod_log_streamer = PodLogStreamer(
                out_dir=exp_dir / "vllm-logs",
                namespace=pod_log_ns,
            )
            if not pod_log_streamer.start():
                pod_log_streamer = None
            else:
                print(f"[collector] capturing container logs -> {exp_dir / 'vllm-logs'}")
        except Exception as e:
            print(f"[collector] WARN: failed to start pod log capture: {e}")
            pod_log_streamer = None

    poll_count = 0
    while not shutdown.is_set():
        # --- Optional Prometheus scrape (latency polling runs in the collector) ---
        now = time.time()
        if metrics_fh and args.prometheus_url and (now - last_prom_scrape) >= args.prom_interval:
            snapshot = _scrape_prometheus(args.prometheus_url, namespace=args.namespace)
            if snapshot:
                _augment_derived_rps(snapshot, prev_nq, now)
                metrics_fh.write(json.dumps(snapshot, default=str) + "\n")
                metrics_fh.flush()
            last_prom_scrape = now

        poll_count += 1
        shutdown.wait(args.poll_interval)

    # ---- Shutdown: write summary files ----
    collector.stop()
    if pod_log_streamer is not None:
        try:
            pod_log_streamer.stop()
        except Exception as e:
            print(f"[collector] WARN: failed to stop pod log capture: {e}")
    if metrics_fh:
        metrics_fh.close()

    dt_wall = time.time() - t_start

    # logs.json is the live router truth (pure-observation mode). Mirror it to
    # router_logs.json for backward-compat (client runs ship both), then run the
    # authoritative join (idempotent: it just re-attaches the router fields), so
    # external runs match client runs.
    all_records: List[Dict[str, Any]] = []
    try:
        with logs_path.open("r", encoding="utf-8") as fin, \
                router_logs_path.open("w", encoding="utf-8") as fout:
            for line in fin:
                line = line.rstrip("\n")
                if not line.strip():
                    continue
                fout.write(line + "\n")
                try:
                    all_records.append(json.loads(line))
                except Exception:
                    continue
    except FileNotFoundError:
        logs_path.write_text("", encoding="utf-8")
        router_logs_path.write_text("", encoding="utf-8")

    routing_summary = None
    try:
        join_logs_with_router(logs_path, router_logs_path)
        routing_summary = summarize_routing(logs_path)
    except Exception as e:
        print(f"[collector] WARN: routing join/summary failed: {e}")

    # endpoint_tokens.json
    try:
        token_summary = _summarize_endpoint_tokens(all_records)
        with (exp_dir / "endpoint_tokens.json").open("w") as f:
            json.dump(token_summary, f, indent=2, sort_keys=True)
    except Exception as e:
        print(f"[collector] WARN: failed to write endpoint_tokens.json: {e}")

    # run_summary.json
    run_summary = {
        "total_requests": collector.count,
        "backend": "prod-collector",
        "load_runner_duration_s": round(dt_wall, 3),
        "wall_time_s": round(dt_wall, 3),
        "transport_mode": "latency_log_poll",
        "router_url": router_url,
        "poll_count": poll_count,
        "poll_interval_s": args.poll_interval,
        "prometheus_url": args.prometheus_url,
    }
    if routing_summary is not None:
        run_summary["routing"] = routing_summary
    try:
        with (exp_dir / "run_summary.json").open("w") as f:
            json.dump(run_summary, f, indent=2, sort_keys=True)
    except Exception as e:
        print(f"[collector] WARN: failed to write run_summary.json: {e}")

    print(f"[collector] done. {collector.count} records in {dt_wall:.1f}s")
    print(f"[collector] results: {exp_dir}")


if __name__ == "__main__":
    main()
