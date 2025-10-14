#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import json
import math
import re
import sys
import pathlib
from typing import Dict, List, Tuple, Optional

import click
import matplotlib.pyplot as plt


def human(x: float) -> str:
    return f"{x:.3f}" if math.isfinite(x) else "nan"


def percentile(sorted_vals: List[float], q: float) -> float:
    """q in [0,1], simple nearest-rank interpolation."""
    if not sorted_vals:
        return float("nan")
    if q <= 0:
        return sorted_vals[0]
    if q >= 1:
        return sorted_vals[-1]
    idx = int(round(q * (len(sorted_vals) - 1)))
    return sorted_vals[idx]


def load_latencies(lat_csv: pathlib.Path) -> List[float]:
    lats: List[float] = []
    with lat_csv.open("r", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            try:
                status = int(str(row.get("status", "0")))
                if status == 200:
                    lats.append(float(str(row.get("latency_s", "nan"))))
            except Exception:
                # skip bad rows
                continue
    return lats


def collect_batches(exp_dir: pathlib.Path) -> List[Tuple[int, List[float]]]:
    batches: List[Tuple[int, List[float]]] = []
    for sub in sorted(exp_dir.iterdir()):
        if not sub.is_dir():
            continue
        m = re.fullmatch(r"batch_(\d+)", sub.name)
        if not m:
            continue
        b = int(m.group(1))
        lat_csv = sub / "latencies.csv"
        if lat_csv.exists():
            lats = load_latencies(lat_csv)
            if lats:
                batches.append((b, lats))
    return sorted(batches, key=lambda t: t[0])


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.argument("experiment_id", type=int)
@click.option(
    "--root",
    default="/home/saeid/llm-lb/profiling",
    show_default=True,
    help="Root folder that contains profiling runs (1, 2, 3, ...).",
)
@click.option(
    "--candles-outfile",
    default=None,
    help="Output image path for candle/box plot. Defaults to <root>/<id>/latency_candles.png",
)
@click.option(
    "--avg-outfile",
    default=None,
    help="Output image path for average plot. Defaults to <root>/<id>/latency_averages.png",
)
def main(
    experiment_id: int,
    root: str,
    candles_outfile: Optional[str],
    avg_outfile: Optional[str],
):
    """
    Draw a candle/box plot and a simple average-latency plot per batch for the given profiling experiment ID.
    Also prints which batch has the highest latency variance and saves variance stats to JSON.
    """
    exp_dir = pathlib.Path(root) / str(experiment_id)
    if not exp_dir.exists():
        click.echo(f"ERROR: experiment folder not found: {exp_dir}", err=True)
        sys.exit(1)

    data = collect_batches(exp_dir)
    if not data:
        click.echo(
            f"ERROR: no batch_* folders with latencies.csv found in {exp_dir}", err=True
        )
        sys.exit(2)

    # Compute per-batch stats
    stats: Dict[int, Dict[str, float]] = {}
    for b, lats in data:
        l_sorted = sorted(lats)
        n = len(l_sorted)
        mean = sum(l_sorted) / n
        var = (
            sum((x - mean) ** 2 for x in l_sorted) / (n - 1) if n > 1 else float("nan")
        )
        p50 = percentile(l_sorted, 0.5)
        p95 = percentile(l_sorted, 0.95)
        stats[b] = {
            "count": float(n),
            "mean_s": float(mean),
            "variance_s2": float(var),
            "p50_s": float(p50),
            "p95_s": float(p95),
        }

    # Rank by variance (desc)
    ranking = sorted(
        stats.items(),
        key=lambda kv: (
            -(kv[1]["variance_s2"] if math.isfinite(kv[1]["variance_s2"]) else -1e99),
            kv[0],
        ),
    )
    top_b, top_stat = ranking[0]
    click.echo("\nVariance by batch (desc):")
    for b, st in ranking:
        click.echo(
            f"  batch {b:>3}: var={human(st['variance_s2'])} s^2, count={int(st['count'])}, "
            f"p50={human(st['p50_s'])}s, p95={human(st['p95_s'])}s, mean={human(st['mean_s'])}s"
        )
    click.echo(
        f"\nHighest variance → batch {top_b} (var={human(top_stat['variance_s2'])} s^2)"
    )

    # --- Candle/box plot ---
    labels = [str(b) for b, _ in data]
    series = [lats for _, lats in data]

    plt.figure()
    plt.boxplot(series, labels=labels, showfliers=False)
    plt.xlabel("Batch size (--max-num-seqs)")
    plt.ylabel("Latency (s)")
    plt.title(
        f"Latency Candle Plot · Experiment {experiment_id} · Highest Var: batch {top_b}"
    )
    plt.grid(True, axis="y", alpha=0.3)
    plt.tight_layout()
    out_candles = (
        pathlib.Path(candles_outfile)
        if candles_outfile
        else (exp_dir / "latency_candles.png")
    )
    plt.savefig(out_candles, dpi=150)
    plt.close()

    # --- Average-only plot ---
    batches_sorted = [b for b, _ in data]
    means = [stats[b]["mean_s"] for b in batches_sorted]

    plt.figure()
    plt.plot(batches_sorted, means, marker="o")
    plt.xlabel("Batch size (--max-num-seqs)")
    plt.ylabel("Average latency (s)")
    plt.title(f"Average Latency vs Batch Size · Experiment {experiment_id}")
    plt.grid(True, axis="both", alpha=0.3)
    plt.tight_layout()
    out_avg = (
        pathlib.Path(avg_outfile) if avg_outfile else (exp_dir / "latency_averages.png")
    )
    plt.savefig(out_avg, dpi=150)
    plt.close()

    # Save stats JSON next to the images
    stats_json_path = exp_dir / "variance_by_batch.json"
    pretty_stats = {
        str(b): {
            "count": int(stats[b]["count"]),
            "mean_s": round(float(stats[b]["mean_s"]), 6),
            "variance_s2": (
                round(float(stats[b]["variance_s2"]), 6)
                if math.isfinite(stats[b]["variance_s2"])
                else "nan"
            ),
            "p50_s": round(float(stats[b]["p50_s"]), 6),
            "p95_s": round(float(stats[b]["p95_s"]), 6),
        }
        for b in batches_sorted
    }
    with stats_json_path.open("w", encoding="utf-8") as f:
        json.dump(
            {
                "experiment_id": experiment_id,
                "root": str(pathlib.Path(root)),
                "images": {
                    "candles": str(out_candles),
                    "averages": str(out_avg),
                },
                "batches": pretty_stats,
                "highest_variance_batch": str(top_b),
            },
            f,
            indent=2,
        )

    click.echo(f"\nSaved candle plot  → {out_candles}")
    click.echo(f"Saved average plot → {out_avg}")
    click.echo(f"Saved stats        → {stats_json_path}")


if __name__ == "__main__":
    main()
