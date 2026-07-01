# experiment_analysis.py
# Utilities for loading experiment logs and computing per-request latency metrics.
# + minimal helpers for loading Prometheus samples recorded in metrics.jsonl.
#
# This version optionally corrects cross-node timestamps using:
#   - experiments/<id>/config.json            -> time_offsets_ns (node -> offset_ns)
#   - experiments/<id>/pod_node_mapping_events.jsonl -> pod->node snapshots over time
#
# Enable with:
#   load_experiment_latencies(..., apply_time_offset_correction=True)

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Dict, Any, Optional, Union, List, Tuple
import json
import copy

import pandas as pd

from trace_utils import compute_trace_metrics


JsonDict = Dict[str, Any]
PathLike = Union[str, Path]


# =========================
# IO helpers
# =========================

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


# =========================
# Time-offset correction helpers
# =========================

def _load_time_offsets_ns(config_json_path: Path) -> Dict[str, int]:
    """
    Load node time offsets from config.json:
      time_offsets_ns: { node_name: offset_ns }

    offset_ns semantics (as used in your measurement script):
      offset_ns = remote_epoch_ns - local_midpoint_ns

    So to convert a remote timestamp into the master's timebase:
      master_est_s = remote_s - (offset_ns / 1e9)
    """
    try:
        obj = json.loads(config_json_path.read_text(encoding="utf-8"))
        m = obj.get("time_offsets_ns", {}) or {}
        out: Dict[str, int] = {}
        for k, v in m.items():
            try:
                out[str(k)] = int(v)
            except Exception:
                continue
        return out
    except Exception:
        return {}


def _load_podmap_event_snapshots(podmap_jsonl_path: Path) -> List[Tuple[float, Dict[str, str]]]:
    """
    Read pod_node_mapping_events.jsonl and extract a time-ordered list of snapshots:

      [(ts_unix, {pod_name: node_name, ...}), ...]

    We key by 'ts_unix' from the event logger entry (not the pod's start_time).
    """
    snaps: List[Tuple[float, Dict[str, str]]] = []
    if not podmap_jsonl_path.is_file():
        return snaps

    for rec in _read_ndjson(podmap_jsonl_path):
        try:
            ts = float(rec.get("ts_unix", 0.0) or 0.0)
            snap = rec.get("snapshot", {}) or {}
            pods = snap.get("pods", []) or []
            mapping: Dict[str, str] = {}
            for p in pods:
                pod = p.get("pod")
                node = p.get("node")
                if pod and node:
                    mapping[str(pod)] = str(node)
            if mapping:
                snaps.append((ts, mapping))
        except Exception:
            continue

    snaps.sort(key=lambda x: x[0])
    return snaps


def _pod_to_node_at_time(
    snapshots: List[Tuple[float, Dict[str, str]]],
    t_unix: float,
) -> Dict[str, str]:
    """
    Pick the latest snapshot at or before t_unix.
    If none exist before time, fall back to the earliest snapshot (best-effort).
    """
    if not snapshots:
        return {}

    # binary-ish scan (snapshots are small; linear is fine)
    best: Optional[Dict[str, str]] = None
    for ts, mapping in snapshots:
        if ts <= t_unix:
            best = mapping
        else:
            break

    if best is None:
        best = snapshots[0][1]
    return best


def _apply_time_offset_correction_to_trace(
    trace: Dict[str, Any],
    *,
    serving_node: Optional[str],
    time_offsets_ns: Dict[str, int],
) -> Tuple[Dict[str, Any], Optional[float]]:
    """
    Return (corrected_trace, applied_offset_s).

    Only adjusts timestamps that are *expected* to come from the sidecar/vLLM node clock.

    Assumption:
      - client/router timestamps are already on the master timebase
      - sidecar/vLLM timestamps are on the serving node's clock (the node hosting the vLLM pod)
    """
    if not serving_node or serving_node not in time_offsets_ns:
        return trace, None

    offset_s = time_offsets_ns[serving_node] / 1e9

    # Work on a copy (never mutate raw logs in-place).
    out = copy.deepcopy(trace)

    # Keys that are produced on the serving node side (sidecar + vLLM)
    # If you add new timestamps later, extend this list/prefix logic.
    def is_remote_key(k: str) -> bool:
        k = str(k)
        if not k.startswith("t_"):
            return False
        # Sidecar-side timestamps
        if "sidecar" in k:
            return True
        # vLLM timestamps
        if k.startswith("t_vllm_") or "vllm" in k:
            return True
        return False

    for k, v in list(out.items()):
        if not is_remote_key(k):
            continue
        if isinstance(v, (int, float)):
            out[k] = float(v) - float(offset_s)

    return out, float(offset_s)


# =========================
# Main latency loader
# =========================

def load_experiment_latencies(
    *,
    exp_id: Optional[Union[int, str]] = None,
    exp_dir: Optional[PathLike] = None,
    experiments_root: PathLike = "experiments",
    include_failed: bool = False,
    apply_time_offset_correction: bool = False,
) -> pd.DataFrame:
    """
    Load logs.json for a given experiment and compute per-request latency metrics.

    If apply_time_offset_correction=True:
      - loads time_offsets_ns from config.json
      - loads pod->node snapshots from pod_node_mapping_events.jsonl
      - finds serving pod from trace['endpoint']
      - finds serving node via snapshot closest to request time
      - corrects sidecar/vLLM timestamps into the master's timebase before computing metrics

    Returns a DataFrame with:
      - base fields
      - derived trace metrics columns (client_to_router_s, router_queue_s, ...)
      - extra helpful columns (serving_pod, serving_node, applied_time_offset_s)
    """
    exp_path = _resolve_experiment_dir(exp_id=exp_id, exp_dir=exp_dir, experiments_root=experiments_root)
    logs_path = exp_path / "logs.json"
    config_path = exp_path / "config.json"
    podmap_path = exp_path / "pod_node_mapping_events.jsonl"

    if not logs_path.is_file():
        raise FileNotFoundError(f"logs.json not found in experiment directory: {logs_path}")

    time_offsets_ns: Dict[str, int] = {}
    podmap_snaps: List[Tuple[float, Dict[str, str]]] = []

    if apply_time_offset_correction:
        time_offsets_ns = _load_time_offsets_ns(config_path) if config_path.is_file() else {}
        podmap_snaps = _load_podmap_event_snapshots(podmap_path) if podmap_path.is_file() else []

    rows: List[JsonDict] = []

    for rec in _read_ndjson(logs_path):
        send_failed = bool(rec.get("send_failed", False))
        if send_failed and not include_failed:
            continue

        trace = rec.get("trace") if isinstance(rec.get("trace"), dict) else None

        # Base row
        row: JsonDict = {
            "idx": rec.get("idx"),
            "req_id": rec.get("req_id"),
            "send_failed": send_failed,
            "end_to_end_s": rec.get("end_to_end_s"),
            "model_latency_s": rec.get("model_latency_s"),
            "finish_reason": rec.get("finish_reason"),
            "prompt_tokens": rec.get("prompt_tokens"),
            "completion_tokens": rec.get("completion_tokens"),
            "total_tokens": rec.get("total_tokens"),
        }

        # Streaming metrics (written by load_runner for stream=true requests)
        if rec.get("ttft_s") is not None:
            row["ttft_s"] = float(rec["ttft_s"])
        if rec.get("tpot_avg_s") is not None:
            row["tpot_avg_s"] = float(rec["tpot_avg_s"])
        if rec.get("streaming_chunks") is not None:
            row["streaming_chunks"] = int(rec["streaming_chunks"])
        if rec.get("streaming"):
            row["streaming"] = True
        if rec.get("endpoint_id") is not None:
            row["endpoint_id"] = rec["endpoint_id"]
        if rec.get("target_model") is not None:
            row["target_model"] = rec["target_model"]

        serving_pod = None
        serving_node = None
        applied_offset_s = None

        if trace is not None:
            serving_pod = trace.get("endpoint")

            # Choose "request time" on master clock to pick the closest podmap snapshot
            # Prefer t0_wall (client wall) if present; else fall back to t_arrive_router.
            t_ref = rec.get("t0_wall", None)
            if not isinstance(t_ref, (int, float)):
                t_ref = trace.get("t_arrive_router", None)
            if not isinstance(t_ref, (int, float)):
                # last resort: now-ish, but better to just not map
                t_ref = None

            if apply_time_offset_correction and t_ref is not None and podmap_snaps and isinstance(serving_pod, str):
                pod_to_node = _pod_to_node_at_time(podmap_snaps, float(t_ref))
                serving_node = pod_to_node.get(serving_pod)

                # Correct trace timestamps into master timebase
                if time_offsets_ns:
                    trace_corr, applied_offset_s = _apply_time_offset_correction_to_trace(
                        trace,
                        serving_node=serving_node,
                        time_offsets_ns=time_offsets_ns,
                    )
                else:
                    trace_corr = trace
            else:
                trace_corr = trace

            # Save mapping columns (helpful for downstream analysis)
            row["serving_pod"] = serving_pod
            row["serving_node"] = serving_node
            row["applied_time_offset_s"] = applied_offset_s

            # Compute derived metrics from (possibly corrected) raw timestamps
            metrics = compute_trace_metrics(trace_corr)
            for k, v in metrics.items():
                row[k] = v

        rows.append(row)

    if not rows:
        return pd.DataFrame(
            columns=[
                "idx",
                "req_id",
                "send_failed",
                "end_to_end_s",
                "model_latency_s",
                "finish_reason",
                "serving_pod",
                "serving_node",
                "applied_time_offset_s",
            ]
        )

    df = pd.DataFrame(rows)

    # sensible typing
    if "idx" in df.columns:
        df["idx"] = pd.to_numeric(df["idx"], errors="coerce").astype("Int64")
    if "send_failed" in df.columns:
        df["send_failed"] = df["send_failed"].astype(bool)
    if "applied_time_offset_s" in df.columns:
        df["applied_time_offset_s"] = pd.to_numeric(df["applied_time_offset_s"], errors="coerce")

    return df


# ============================================================
# Minimal Prometheus metrics loader (metrics.jsonl)
# ============================================================

def load_experiment_prom_samples(
    *,
    exp_id: Optional[Union[int, str]] = None,
    exp_dir: Optional[PathLike] = None,
    experiments_root: PathLike = "experiments",
) -> pd.DataFrame:
    """
    Load metrics.jsonl (written by metrics_prom.py) and flatten into a DataFrame.

    Each JSONL tick typically looks like:
      {"ts": "...", "mode": "...", "instances": [...], "samples": [ {instance/pod + fields...}, ... ]}

    This function returns one row per (tick, sample), i.e. per instance per tick.
    """
    exp_path = _resolve_experiment_dir(exp_id=exp_id, exp_dir=exp_dir, experiments_root=experiments_root)
    metrics_path = exp_path / "metrics.jsonl"

    if not metrics_path.is_file():
        return pd.DataFrame()

    flat: List[JsonDict] = []

    for rec in _read_ndjson(metrics_path):
        # Skip error ticks written by metrics_prom.py
        if rec.get("type") == "metrics_error":
            continue

        ts = rec.get("ts")
        samples = rec.get("samples")
        if not isinstance(samples, list) or not samples:
            continue

        for s in samples:
            if not isinstance(s, dict):
                continue
            row: JsonDict = {"ts": ts}
            row.update(s)
            flat.append(row)

    if not flat:
        return pd.DataFrame()

    df = pd.DataFrame(flat)

    if "ts" in df.columns:
        df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")

    return df


def experiments_from_series(series_ids, series_size=4, start_exp_id=1):
    experiments = []
    for s in sorted(series_ids):
        if s < 0:
            raise ValueError("series_ids must be non-negative")
        start = start_exp_id + s * series_size
        experiments.extend(range(start, start + series_size))
    return experiments


def experiments_from_series(start_exp_id, end_exp_id):
    """
    Return a list of experiment IDs from start_exp_id to end_exp_id (inclusive).
    """
    if start_exp_id > end_exp_id:
        raise ValueError("start_exp_id must be <= end_exp_id")
    return list(range(start_exp_id, end_exp_id + 1))