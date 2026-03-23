#!/usr/bin/env python3
"""
vllm_analyze.py — Parse metrics.jsonl and save one PNG per chart.

Output structure:
    <out_dir>/experiment_<EXPERIMENT_ID>/
        all/
            01_requests.png
            02_prefix_hit_rate.png
            03_external_prefix_hit_rate.png
            04_context_length_distribution.png
            05_token_throughput.png
            06_latency.png
        2026-03-20/
            01_requests.png
            ...
        2026-03-21/
            01_requests.png
            ...

Usage:
    python3 vllm_analyze.py vllm_logs/metrics.jsonl
    python3 vllm_analyze.py vllm_logs/metrics.jsonl --out-dir ./plots
    python3 vllm_analyze.py vllm_logs/metrics.jsonl --last-minutes 60
    python3 vllm_analyze.py vllm_logs/metrics.jsonl --no-daily
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mticker

EXPERIMENT_ID = "1"

# ─────────────────────────────────────────────────────────────────────────────
# Load
# ─────────────────────────────────────────────────────────────────────────────

def load_records(path: Path, last_minutes: Optional[int] = None) -> List[Dict[str, Any]]:
    records = []
    cutoff = None
    if last_minutes:
        cutoff = datetime.now(tz=timezone.utc) - timedelta(minutes=last_minutes)

    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not rec.get("samples"):
                continue
            ts_str = rec.get("ts", "")
            try:
                ts = datetime.fromisoformat(ts_str)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                ts_local = ts.astimezone().replace(tzinfo=None)
            except Exception:
                continue
            if cutoff and ts < cutoff:
                continue
            records.append({"ts": ts_local, "fields": rec["samples"][0]})

    records.sort(key=lambda r: r["ts"])
    return records


def group_records_by_day(records: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Group records by local date string YYYY-MM-DD derived from the 'ts' field."""
    groups: Dict[str, List] = defaultdict(list)
    for r in records:
        day_key = r["ts"].strftime("%Y-%m-%d")
        groups[day_key].append(r)
    return dict(sorted(groups.items()))


def _safe(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        f = float(v)
        return None if math.isnan(f) else f
    except Exception:
        return None


def extract(records, key, scale=1.0):
    ts, vals = [], []
    for r in records:
        v = _safe(r["fields"].get(key))
        if v is not None:
            ts.append(r["ts"])
            vals.append(v * scale)
    return ts, vals


def extract_pct(records, key):
    return extract(records, key, scale=100.0)


# ─────────────────────────────────────────────────────────────────────────────
# Style
# ─────────────────────────────────────────────────────────────────────────────

def setup_ax(ax, title, ylabel):
    ax.set_title(title, fontsize=12, pad=10)
    ax.set_ylabel(ylabel, fontsize=10)
    ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.5)
    ax.xaxis.set_major_formatter(mdates.AutoDateFormatter(mdates.AutoDateLocator()))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=20, ha="right", fontsize=8)
    ax.tick_params(axis="y", labelsize=8)


def save(fig, path):
    fig.tight_layout()
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> {path}")


def no_data(ax, msg="No data available"):
    ax.text(0.5, 0.5, msg, ha="center", va="center",
            transform=ax.transAxes, fontsize=11, color="gray")
    ax.set_xticks([])
    ax.set_yticks([])


def _make_title(base: str, day_label: str) -> str:
    return f"{base}  [{day_label}]" if day_label else base


# ─────────────────────────────────────────────────────────────────────────────
# Charts
# ─────────────────────────────────────────────────────────────────────────────

def plot_requests(records, out_dir, day_label: str = ""):
    fig, axes = plt.subplots(3, 1, figsize=(10, 11), sharex=True)
    fig.suptitle(_make_title("Active Requests Over Time", day_label), fontsize=13, y=1.01)

    ts_r, r = extract(records, "requests_running")
    ts_w, w = extract(records, "requests_waiting")

    # ── Subplot 1: Running + Waiting combined ────────────────────────────────
    ax = axes[0]
    setup_ax(ax, "Running + Waiting", "requests")
    if ts_r:
        ax.plot(ts_r, r, label="Running", color="steelblue", linewidth=1.5)
        ax.fill_between(ts_r, r, alpha=0.15, color="steelblue")
    if ts_w:
        ax.plot(ts_w, w, label="Waiting", color="coral", linewidth=1.5)
    if not ts_r and not ts_w:
        no_data(ax)
    else:
        ax.set_ylim(bottom=0)
        ax.legend(fontsize=9)

    # ── Subplot 2: Running only ──────────────────────────────────────────────
    ax = axes[1]
    setup_ax(ax, "Running", "requests")
    if ts_r:
        ax.plot(ts_r, r, color="steelblue", linewidth=1.5)
        ax.fill_between(ts_r, r, alpha=0.15, color="steelblue")
        ax.set_ylim(bottom=0)
        ax.annotate(f"peak {max(r):.0f}  avg {sum(r)/len(r):.1f}",
                    xy=(0.01, 0.97), xycoords="axes fraction",
                    va="top", fontsize=8, color="steelblue")
    else:
        no_data(ax)

    # ── Subplot 3: Waiting only ──────────────────────────────────────────────
    ax = axes[2]
    setup_ax(ax, "Waiting", "requests")
    if ts_w:
        ax.plot(ts_w, w, color="coral", linewidth=1.5)
        ax.fill_between(ts_w, w, alpha=0.15, color="coral")
        ax.set_ylim(bottom=0)
        ax.annotate(f"peak {max(w):.0f}  avg {sum(w)/len(w):.1f}",
                    xy=(0.01, 0.97), xycoords="axes fraction",
                    va="top", fontsize=8, color="coral")
    else:
        no_data(ax)

    save(fig, out_dir / "01_requests.png")


def plot_prefix_hit_rate(records, out_dir, day_label: str = ""):
    fig, ax = plt.subplots(figsize=(10, 4))
    setup_ax(ax, _make_title("Prefix Cache Hit Rate  (local GPU HBM)", day_label), "hit rate %")

    ts_i, vi = extract_pct(records, "prefix_cache_hit_rate")

    if ts_i:
        ax.plot(ts_i, vi, color="steelblue", linewidth=1.5)
        ax.fill_between(ts_i, vi, alpha=0.12, color="steelblue")

    if not ts_i:
        no_data(ax, "No data — no requests during this period")
    else:
        ax.set_ylim(0, 105)
        ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f%%"))
        if vi:
            ax.annotate(f"avg {sum(vi)/len(vi):.1f}%  peak {max(vi):.1f}%",
                        xy=(0.01, 0.97), xycoords="axes fraction",
                        va="top", fontsize=8, color="steelblue")

    save(fig, out_dir / "02_prefix_hit_rate.png")


def plot_ext_prefix_hit_rate(records, out_dir, day_label: str = ""):
    fig, ax = plt.subplots(figsize=(10, 4))
    setup_ax(ax, _make_title("External Prefix Cache Hit Rate  (KV Connector)", day_label), "hit rate %")

    ts_i, vi = extract_pct(records, "external_prefix_cache_hit_rate")
    ts_c, vc = extract_pct(records, "external_prefix_cache_hit_rate_cumulative")

    if ts_i:
        ax.plot(ts_i, vi, label="Interval", color="mediumpurple", linewidth=1.5)
        ax.fill_between(ts_i, vi, alpha=0.12, color="mediumpurple")
    if ts_c:
        ax.plot(ts_c, vc, label="Cumulative", color="gray",
                linewidth=1.2, linestyle="--")

    if not ts_i and not ts_c:
        no_data(ax, "No data — external prefix cache not active")
    else:
        ax.set_ylim(0, 105)
        ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f%%"))
        ax.legend(fontsize=9)
        all_v = vi + vc
        if all_v:
            ax.annotate(f"avg {sum(all_v)/len(all_v):.1f}%  peak {max(all_v):.1f}%",
                        xy=(0.01, 0.97), xycoords="axes fraction",
                        va="top", fontsize=8, color="mediumpurple")

    save(fig, out_dir / "03_external_prefix_hit_rate.png")


def _parse_buckets(bucket_dict):
    items = []
    for le_str, cum in bucket_dict.items():
        try:
            le_f = float(le_str) if le_str != "+Inf" else float("inf")
        except ValueError:
            continue
        if le_f == float("inf"):
            continue
        items.append((le_f, le_str, float(cum)))
    items.sort(key=lambda x: x[0])
    return items


def _get_window_buckets(records):
    first_buckets = None
    last_buckets  = None
    for r in records:
        b = r["fields"].get("request_prompt_tokens__buckets")
        if b and isinstance(b, dict):
            if first_buckets is None:
                first_buckets = b
            last_buckets = b

    if last_buckets is None:
        return None, None, None

    last_items = _parse_buckets(last_buckets)
    if not last_items:
        return None, None, None

    if first_buckets is not None and first_buckets is not last_buckets:
        first_items = _parse_buckets(first_buckets)
        first_map   = {le_str: cum for _, le_str, cum in first_items}
    else:
        first_map = {}

    les, labels, per_bucket = [], [], []
    prev_cum_last  = 0.0
    prev_cum_first = 0.0
    for le_f, le_str, cum_last in last_items:
        cum_first = first_map.get(le_str, 0.0)
        count = max(0.0, (cum_last - prev_cum_last) - (cum_first - prev_cum_first))
        les.append(le_f)
        labels.append(le_str)
        per_bucket.append(count)
        prev_cum_last  = cum_last
        prev_cum_first = cum_first

    return les, labels, per_bucket


def plot_context_length(records, out_dir, day_label: str = ""):
    fig, ax = plt.subplots(figsize=(10, 4))
    base_title = "Request Prompt Token Distribution  (cumulative histogram buckets, per-request counts)"
    ax.set_title(_make_title(base_title, day_label), fontsize=12, pad=10)
    ax.set_xlabel("prompt tokens (bucket upper bound)", fontsize=10)
    ax.set_ylabel("request count", fontsize=10)
    ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.5, axis="y")
    ax.tick_params(labelsize=8)

    les, labels, per_bucket = _get_window_buckets(records)

    if les is None:
        no_data(ax,
            "No bucket data\n"
            "Requires vllm_monitor.py >= 1.7.0 with bucket storage enabled.\n"
            "Re-run vllm_monitor.py and collect new data.")
    else:
        les_f    = [l for l, c in zip(les, per_bucket) if c > 0]
        labels_f = [lb for lb, c in zip(labels, per_bucket) if c > 0]
        counts_f = [c for c in per_bucket if c > 0]

        x = range(len(les_f))
        bars = ax.bar(x, counts_f, color="steelblue", edgecolor="white",
                      linewidth=0.4, alpha=0.85)

        ax.set_xticks(list(x))
        ax.set_xticklabels(labels_f, rotation=45, ha="right", fontsize=7)

        total = sum(counts_f)
        cumsum = 0
        for i, c in enumerate(counts_f):
            cumsum += c
            if cumsum >= total / 2:
                bars[i].set_color("coral")
                bars[i].set_alpha(0.9)
                ax.annotate("median bucket", xy=(i, counts_f[i]),
                            xytext=(i + 0.3, counts_f[i] * 1.05),
                            fontsize=7, color="coral")
                break

        avg_v = None
        for r in reversed(records):
            v = _safe(r["fields"].get("request_prompt_tokens_avg"))
            if v is not None:
                avg_v = v
                break

        stats = f"total requests={total:.0f}"
        if avg_v is not None:
            stats += f"  avg={avg_v:.0f} tok"
        ax.annotate(stats, xy=(0.99, 0.97), xycoords="axes fraction",
                    ha="right", va="top", fontsize=8, color="dimgray")

    fig.tight_layout()
    fig.savefig(out_dir / "04_context_length_distribution.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> {out_dir / '04_context_length_distribution.png'}")


def plot_throughput(records, out_dir, day_label: str = ""):
    fig, ax = plt.subplots(figsize=(10, 4))
    setup_ax(ax, _make_title("Token Throughput", day_label), "tok/s")

    ts_g, vg = extract(records, "gen_tokens_per_sec")
    ts_p, vp = extract(records, "prefill_tokens_per_sec")

    if ts_g:
        ax.plot(ts_g, vg, label="Gen tok/s", color="steelblue", linewidth=1.5)
        ax.fill_between(ts_g, vg, alpha=0.12, color="steelblue")
    if ts_p:
        ax.plot(ts_p, vp, label="Prefill tok/s", color="darkorange", linewidth=1.5)

    if not ts_g and not ts_p:
        no_data(ax)
    else:
        ax.set_ylim(bottom=0)
        ax.legend(fontsize=9)
        if vg:
            ax.annotate(f"avg gen {sum(vg)/len(vg):.0f}  peak {max(vg):.0f}",
                        xy=(0.01, 0.97), xycoords="axes fraction",
                        va="top", fontsize=8, color="steelblue")

    save(fig, out_dir / "05_token_throughput.png")


def plot_latency(records, out_dir, day_label: str = ""):
    fig, ax = plt.subplots(figsize=(10, 4))
    setup_ax(ax, _make_title("Latency — TTFT & TPOT  (cumulative avg)", day_label), "ms")

    ts_t, vt = extract(records, "ttft_seconds_avg", scale=1000)
    ts_p, vp = extract(records, "tpot_seconds_avg", scale=1000)

    if ts_t:
        ax.plot(ts_t, vt, label="TTFT (ms)", color="steelblue", linewidth=1.5)
    if ts_p:
        ax.plot(ts_p, vp, label="TPOT (ms)", color="darkorange", linewidth=1.5)

    if not ts_t and not ts_p:
        no_data(ax)
    else:
        ax.set_ylim(bottom=0)
        ax.legend(fontsize=9)
        parts = []
        if vt: parts.append(f"avg TTFT {sum(vt)/len(vt):.1f}ms")
        if vp: parts.append(f"avg TPOT {sum(vp)/len(vp):.2f}ms")
        if parts:
            ax.annotate("  |  ".join(parts),
                        xy=(0.01, 0.97), xycoords="axes fraction",
                        va="top", fontsize=8, color="dimgray")

    save(fig, out_dir / "06_latency.png")


def render_all_charts(records, out_dir, day_label: str = ""):
    """Render all 6 charts into out_dir, tagging titles with day_label if given."""
    out_dir.mkdir(parents=True, exist_ok=True)
    plot_requests(records, out_dir, day_label)
    plot_prefix_hit_rate(records, out_dir, day_label)
    plot_ext_prefix_hit_rate(records, out_dir, day_label)
    plot_context_length(records, out_dir, day_label)
    plot_throughput(records, out_dir, day_label)
    plot_latency(records, out_dir, day_label)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Save one PNG per vLLM metric chart from metrics.jsonl")
    parser.add_argument("log", type=Path, help="Path to metrics.jsonl")
    parser.add_argument("--out-dir", type=Path, default=None,
                        help="Output directory (default: same dir as jsonl)")
    parser.add_argument("--last-minutes", type=int, default=None,
                        help="Only use last N minutes of data")
    parser.add_argument("--no-daily", action="store_true",
                        help="Skip per-day breakdown, only write the 'all' folder")
    args = parser.parse_args()

    if not args.log.exists():
        print(f"File not found: {args.log}", file=sys.stderr)
        sys.exit(1)

    base_dir = args.out_dir or args.log.parent
    exp_dir  = base_dir / f"experiment_{EXPERIMENT_ID}"
    exp_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading {args.log} ...", flush=True)
    records = load_records(args.log, last_minutes=args.last_minutes)
    if not records:
        print("No valid records found.", file=sys.stderr)
        sys.exit(1)

    print(f"  {len(records)} records  "
          f"|  {records[0]['ts'].strftime('%Y-%m-%d %H:%M:%S')}"
          f"  ->  {records[-1]['ts'].strftime('%Y-%m-%d %H:%M:%S')}")

    # ── All-data charts ──────────────────────────────────────────────────────
    all_dir = exp_dir / "all"
    print(f"\n[all] Writing charts to {all_dir}/")
    render_all_charts(records, all_dir, day_label="")

    # ── Per-day charts ───────────────────────────────────────────────────────
    if not args.no_daily:
        day_groups = group_records_by_day(records)
        print(f"\nFound {len(day_groups)} day(s): {', '.join(day_groups.keys())}")
        for day_key, day_records in day_groups.items():
            day_dir = exp_dir / day_key
            print(f"\n[{day_key}] {len(day_records)} records  "
                  f"|  Writing charts to {day_dir}/")
            render_all_charts(day_records, day_dir, day_label=day_key)

    print("\nDone.")


if __name__ == "__main__":
    main()
