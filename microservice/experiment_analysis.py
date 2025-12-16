# experiment_analysis.py
# Utilities for loading experiment logs and computing per-request latency metrics.
#
# Usage from a notebook, for example:
#
#   from experiment_analysis import load_experiment_latencies
#   df = load_experiment_latencies(exp_id=3)
#   df.head()
#
# This expects the directory layout produced by experiment_io.init_experiment():
#
#   ./experiments/
#       1/
#         config.json
#         logs.json   <-- NDJSON (one JSON object per line)
#       2/
#         ...
#
# Each line in logs.json is the per-request record emitted by load_runner.py.

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Dict, Any, Optional, Union, List
import json

import pandas as pd

from trace_utils import compute_trace_metrics


JsonDict = Dict[str, Any]
PathLike = Union[str, Path]


def _read_ndjson(path: Path) -> Iterable[JsonDict]:
    """
    Stream-read an NDJSON/JSONL file (one JSON object per line).

    Skips blank lines and logs with JSON parse failures by raising an error
    (you probably want to fix broken logs rather than silently ignore them).
    """
    with path.open("r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as e:
                raise RuntimeError(f"Failed to parse {path} at line {lineno}: {e}") from e


def _resolve_experiment_dir(
    exp_id: Optional[Union[int, str]] = None,
    exp_dir: Optional[PathLike] = None,
    experiments_root: PathLike = "experiments",
) -> Path:
    """
    Resolve the experiment directory.

    You can either:
      - pass a numeric/string experiment ID (e.g. 3 -> ./experiments/3), or
      - pass an explicit exp_dir path.

    Args:
        exp_id: Experiment ID (directory name under experiments_root).
        exp_dir: Explicit path to the experiment directory.
        experiments_root: Root folder containing experiment subdirectories.

    Returns:
        Path to the experiment directory.

    Raises:
        ValueError if both exp_id and exp_dir are None.
        FileNotFoundError if the resolved directory does not exist.
    """
    if exp_dir is not None and exp_id is not None:
        raise ValueError("Pass either exp_id or exp_dir, not both.")

    if exp_dir is not None:
        p = Path(exp_dir)
    else:
        if exp_id is None:
            raise ValueError("Must provide either exp_id or exp_dir.")
        p = Path(experiments_root) / str(exp_id)

    if not p.is_dir():
        raise FileNotFoundError(f"Experiment directory not found: {p}")

    return p


def load_experiment_latencies(
    *,
    exp_id: Optional[Union[int, str]] = None,
    exp_dir: Optional[PathLike] = None,
    experiments_root: PathLike = "experiments",
    include_failed: bool = False,
) -> pd.DataFrame:
    """
    Load logs.json for a given experiment and compute per-request latency metrics.

    This function:
      - locates the experiment directory (via exp_id or exp_dir),
      - reads logs.json (NDJSON) line-by-line,
      - for each record with a 'trace' object:
          - recomputes derived metrics using compute_trace_metrics(trace),
      - returns a pandas DataFrame with one row per record.

    Columns include:
      - basic fields from the log record (idx, req_id, wait_wall_s, model_latency_s, ...),
      - 'send_failed' flag (bool, default False),
      - one column per derived metric from compute_trace_metrics:
          - end_to_end_s, client_to_router_s, router_queue_s, ...
        (only present if the underlying timestamps exist in the trace).

    Args:
        exp_id: Experiment ID (e.g. 3 -> experiments/3/logs.json).
        exp_dir: Explicit path to an experiment directory (overrides exp_id).
        experiments_root: Root directory used when resolving exp_id.
        include_failed: If False, drop rows where send_failed is True.

    Returns:
        pandas.DataFrame with one row per logged record.
    """
    exp_path = _resolve_experiment_dir(exp_id=exp_id, exp_dir=exp_dir, experiments_root=experiments_root)
    logs_path = exp_path / "logs.json"

    if not logs_path.is_file():
        raise FileNotFoundError(f"logs.json not found in experiment directory: {logs_path}")

    rows: List[JsonDict] = []

    for rec in _read_ndjson(logs_path):
        # Normalise basic flags
        send_failed = bool(rec.get("send_failed", False))

        # Optionally skip failed sends (no /enqueue, no trace).
        if send_failed and not include_failed:
            continue

        # Start the row with a shallow copy of the record so we keep the basic fields.
        row: JsonDict = {
            "idx": rec.get("idx"),
            "req_id": rec.get("req_id"),
            "send_failed": send_failed,
            "wait_wall_s": rec.get("wait_wall_s"),
            "model_latency_s": rec.get("model_latency_s"),
            "finish_reason": rec.get("finish_reason"),
            # You can add more here if you care (prompt length, output length, etc.)
        }

        trace = rec.get("trace")
        if isinstance(trace, dict):
            # Recompute derived metrics from raw timestamps to keep a single source of truth.
            metrics = compute_trace_metrics(trace)

            # Merge into the row; keys are like 'end_to_end_s', 'router_queue_s', ...
            for k, v in metrics.items():
                row[k] = v

        rows.append(row)

    if not rows:
        # Return an empty DataFrame with no rows but some expected columns.
        return pd.DataFrame(
            columns=[
                "idx",
                "req_id",
                "send_failed",
                "wait_wall_s",
                "model_latency_s",
                "finish_reason",
                # metrics columns will appear as needed when data is present
            ]
        )

    df = pd.DataFrame(rows)

    # Make sure some obvious fields are typed sensibly
    if "idx" in df.columns:
        df["idx"] = pd.to_numeric(df["idx"], errors="coerce").astype("Int64")
    if "send_failed" in df.columns:
        df["send_failed"] = df["send_failed"].astype(bool)

    return df
