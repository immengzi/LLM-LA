#!/usr/bin/env python3
"""
vllm_analyze.py — Parse metrics.jsonl and save one PNG per chart.

Output files (in same directory as the jsonl, or --out-dir):
    01_requests.png
    02_prefix_hit_rate.png
    03_external_prefix_hit_rate.png
    04_context_length_distribution.png
    05_token_throughput.png
    06_latency.png

Usage:
    python3 vllm_analyze.py vllm_logs/metrics.jsonl
    python3 vllm_analyze.py vllm_logs/metrics.jsonl --out-dir ./plots
    python3 vllm_analyze.py vllm_logs/metrics.jsonl --last-minutes 60
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mticker


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


# ─────────────────────────────────────────────────────────────────────────────
# Charts
# ─────────────────────────────────────────────────────────────────────────────

def plot_requests(records, out_dir):
    fig, ax = plt.subplots(figsize=(10, 4))
    setup_ax(ax, "Active Requests Over Time", "Number of requests")

    ts_r, r = extract(records, "requests_running")
    ts_w, w = extract(records, "requests_waiting")

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
        # stats annotation

    save(fig, out_dir / "01_requests.png")


def plot_prefix_hit_rate(records, out_dir):
    fig, ax = plt.subplots(figsize=(10, 4))
    setup_ax(ax, "Prefix Cache Hit Rate  (local NPU HBM)", "hit rate %")

    ts_i, vi = extract_pct(records, "prefix_cache_hit_rate")
    # ts_c, vc = extract_pct(records, "prefix_cache_hit_rate_cumulative")

    if ts_i:
        ax.plot(ts_i, vi, label="Interval", color="steelblue", linewidth=1.5)
        ax.fill_between(ts_i, vi, alpha=0.12, color="steelblue")
    # if ts_c:
    #     ax.plot(ts_c, vc, label="Cumulative", color="gray",
    #             linewidth=1.2, linestyle="--")

    if not ts_i:
        no_data(ax, "No data — no requests during this period")
    else:
        ax.set_ylim(0, 105)
        ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f%%"))
        ax.legend(fontsize=9)
        if vi:
            ax.annotate(f"avg {sum(vi)/len(vi):.1f}%  peak {max(vi):.1f}%",
                        xy=(0.01, 0.97), xycoords="axes fraction",
                        va="top", fontsize=8, color="steelblue")

    save(fig, out_dir / "02_prefix_hit_rate.png")


# def plot_ext_prefix_hit_rate(records, out_dir):
#     fig, ax = plt.subplots(figsize=(10, 4))
#     setup_ax(ax, "External Prefix Cache Hit Rate  (KV Connector)", "hit rate %")

#     ts_i, vi = extract_pct(records, "external_prefix_cache_hit_rate")
#     ts_c, vc = extract_pct(records, "external_prefix_cache_hit_rate_cumulative")

#     if ts_i:
#         ax.plot(ts_i, vi, label="Interval", color="mediumpurple", linewidth=1.5)
#         ax.fill_between(ts_i, vi, alpha=0.12, color="mediumpurple")
#     if ts_c:
#         ax.plot(ts_c, vc, label="Cumulative", color="gray",
#                 linewidth=1.2, linestyle="--")

#     if not ts_i and not ts_c:
#         no_data(ax, "No data — external prefix cache not active")
#     else:
#         ax.set_ylim(0, 105)
#         ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f%%"))
#         ax.legend(fontsize=9)
#         all_v = vi + vc
#         if all_v:
#             ax.annotate(f"avg {sum(all_v)/len(all_v):.1f}%  peak {max(all_v):.1f}%",
#                         xy=(0.01, 0.97), xycoords="axes fraction",
#                         va="top", fontsize=8, color="mediumpurple")

#     save(fig, out_dir / "03_external_prefix_hit_rate.png")

def plot_ext_prefix_hit_rate(records, out_dir):
    fig, ax = plt.subplots(figsize=(10, 4))
    setup_ax(ax, "External Prefix Cache Hit Rate", "hit rate %")

    ts_i, vi = extract_pct(records, "external_prefix_cache_hit_rate")

    if ts_i:
        ax.plot(ts_i, vi, label="Interval", color="mediumpurple", linewidth=1.5)
        ax.fill_between(ts_i, vi, alpha=0.12, color="mediumpurple")

    if not ts_i:
        no_data(ax, "No data — external prefix cache not active")
    else:
        ax.set_ylim(0, 105)
        ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f%%"))
        ax.legend(fontsize=9)
        if vi:
            ax.annotate(f"avg {sum(vi)/len(vi):.1f}%  peak {max(vi):.1f}%",
                        xy=(0.01, 0.97), xycoords="axes fraction",
                        va="top", fontsize=8, color="mediumpurple")

    save(fig, out_dir / "03_external_prefix_hit_rate.png")

def plot_context_length(records, out_dir):
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.set_title("Average Cumulated Request Context Length Distribution  (Cumulated prompt tokens averaged per poll, poll every 5s)",
                 fontsize=12, pad=10)
    ax.set_xlabel("prompt tokens", fontsize=10)
    ax.set_ylabel("count", fontsize=10)
    ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.5, axis="y")
    ax.tick_params(labelsize=8)

    _, vals = extract(records, "request_prompt_tokens_avg")

    if not vals:
        no_data(ax, "No data — request_prompt_tokens_avg not available")
    else:
        n_bins = min(30, max(10, len(vals) // 4))
        ax.hist(vals, bins=n_bins, color="steelblue", edgecolor="white",
                linewidth=0.4, alpha=0.8)
        avg_v = sum(vals) / len(vals)
        ax.axvline(avg_v, color="coral", linewidth=1.5, linestyle="--",
                   label=f"avg {avg_v:.0f}")
        ax.legend(fontsize=9)

    fig.tight_layout()
    fig.savefig(out_dir / "04_context_length_distribution.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> {out_dir / '04_context_length_distribution.png'}")


def plot_throughput(records, out_dir):
    fig, ax = plt.subplots(figsize=(10, 4))
    setup_ax(ax, "Token Throughput", "tok/s")

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


def plot_latency(records, out_dir):
    fig, ax = plt.subplots(figsize=(10, 4))
    setup_ax(ax, "Latency — TTFT & TPOT  (cumulative avg)", "ms")

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
    args = parser.parse_args()

    if not args.log.exists():
        print(f"File not found: {args.log}", file=sys.stderr)
        sys.exit(1)

    out_dir = args.out_dir or args.log.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading {args.log} ...", flush=True)
    records = load_records(args.log, last_minutes=args.last_minutes)
    if not records:
        print("No valid records found.", file=sys.stderr)
        sys.exit(1)

    print(f"  {len(records)} records  "
          f"|  {records[0]['ts'].strftime('%Y-%m-%d %H:%M:%S')}"
          f"  ->  {records[-1]['ts'].strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Writing charts to {out_dir}/")

    plot_requests(records, out_dir)
    plot_prefix_hit_rate(records, out_dir)
    plot_ext_prefix_hit_rate(records, out_dir)
    plot_context_length(records, out_dir)
    plot_throughput(records, out_dir)
    plot_latency(records, out_dir)

    print("Done.")


if __name__ == "__main__":
    main()