#!/usr/bin/env python3
"""Summarize Locust CSV stats into a LiteLLM-style markdown table.

Reads results/<run>/locust_stats.csv (written by --csv) and prints:

| Type | Name | Median | 95%ile | 99%ile | Average | Current RPS |
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


def _f(row: Dict[str, str], *keys: str, default: str = "") -> str:
    for k in keys:
        if k in row and row[k] not in (None, ""):
            return str(row[k])
    return default


def _num(x: str) -> Optional[float]:
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def load_stats(csv_path: Path) -> List[Dict[str, str]]:
    with csv_path.open(newline="") as f:
        return list(csv.DictReader(f))


def pick_rows(rows: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for row in rows:
        name = _f(row, "Name", "name")
        typ = _f(row, "Type", "type")
        key = f"{typ}|{name}"
        if name in ("/chat/completions", "Aggregated") or "Overhead" in name:
            out[key] = row
        if name == "Aggregated" and typ in ("", "None", "Aggregated"):
            out["Aggregated"] = row
    return out


def _fmt(x: str) -> str:
    n = _num(x)
    if n is None:
        return x
    if abs(n - round(n)) < 1e-9:
        return str(int(round(n)))
    return f"{n:.2f}"


def row_cells(row: Dict[str, str]) -> Dict[str, Any]:
    # Locust CSV columns vary slightly by version.
    median = _f(row, "Median Response Time", "Median response time", "50%")
    p95 = _f(row, "95%", "95%ile", "Ninety Fifth Response Time")
    p99 = _f(row, "99%", "99%ile", "Ninety Ninth Response Time")
    avg = _f(row, "Average Response Time", "Average response time")
    rps = _f(row, "Requests/s", "Current RPS", "Request Count")
    return {
        "median": _fmt(median),
        "p95": _fmt(p95),
        "p99": _fmt(p99),
        "avg": _fmt(avg),
        "rps": _fmt(rps),
        "type": _f(row, "Type", "type"),
        "name": _f(row, "Name", "name"),
    }


def render_md(meta: Dict[str, Any], cells: List[Dict[str, Any]]) -> str:
    lines = [
        f"# {meta.get('path', 'run')} — {meta.get('instances', '?')} instances",
        "",
        f"- Users: `{meta.get('users', '?')}`  Spawn rate: `{meta.get('spawn_rate', '?')}`  "
        f"Run time: `{meta.get('run_time', '?')}`",
        f"- Model: `{meta.get('model', '?')}`  Mock latency ms: `{meta.get('mock_latency_ms', '0')}`",
        "",
        "| Type | Name | Median (ms) | 95%ile (ms) | 99%ile (ms) | Average (ms) | Current RPS |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for c in cells:
        lines.append(
            f"| {c['type'] or 'POST'} | {c['name']} | {c['median']} | {c['p95']} | "
            f"{c['p99']} | {c['avg']} | {c['rps']} |"
        )
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("out_dir", type=Path, help="results/<path>_<N>inst directory")
    args = ap.parse_args()
    out_dir: Path = args.out_dir

    meta_path = out_dir / "run_meta.json"
    meta: Dict[str, Any] = {}
    if meta_path.exists():
        meta = json.loads(meta_path.read_text())

    stats_path = out_dir / "locust_stats.csv"
    if not stats_path.exists():
        # Locust may write locust_stats.csv when --csv prefix is .../locust
        candidates = list(out_dir.glob("*_stats.csv")) + list(out_dir.glob("locust_stats.csv"))
        if not candidates:
            print(f"No Locust stats CSV in {out_dir}", file=sys.stderr)
            return 1
        stats_path = candidates[0]

    rows = load_stats(stats_path)
    ordered: List[Dict[str, Any]] = []
    seen = set()
    for row in rows:
        name = _f(row, "Name", "name")
        typ = _f(row, "Type", "type")
        if name not in ("/chat/completions", "Aggregated") and "Overhead" not in name:
            continue
        key = (typ, name)
        if key in seen:
            continue
        seen.add(key)
        ordered.append(row_cells(row))
    # Stable order: POST chat, Custom overhead, Aggregated
    def _sort_key(c: Dict[str, Any]) -> tuple:
        name = c["name"]
        if name == "/chat/completions":
            return (0, c["type"])
        if "Overhead" in name:
            return (1, c["type"])
        if name == "Aggregated":
            return (2, c["type"])
        return (3, name)

    ordered.sort(key=_sort_key)

    md = render_md(meta, ordered)
    (out_dir / "summary.md").write_text(md)
    summary_json = {
        "meta": meta,
        "rows": ordered,
        "stats_csv": str(stats_path),
    }
    (out_dir / "summary.json").write_text(json.dumps(summary_json, indent=2) + "\n")
    print(md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
