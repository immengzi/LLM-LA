# utils_analysis.py
import os
import json
import re
import pandas as pd
import numpy as np
from typing import List, Tuple, Dict, Any
import matplotlib.pyplot as plt

# ---------- Readers ----------


def read_jsonl(path: str) -> pd.DataFrame:
    with open(path, "r", encoding="utf-8") as f:
        return pd.DataFrame([json.loads(line) for line in f])


def read_queue(path: str) -> pd.DataFrame:
    with open(path, "r", encoding="utf-8") as f:
        text = f.read().strip()
    # Try JSONL first
    try:
        return pd.DataFrame([json.loads(line) for line in text.splitlines()])
    except Exception:
        # Fallback single JSON (object or list)
        obj = json.loads(text)
        if isinstance(obj, list):
            return pd.DataFrame(obj)
        return pd.DataFrame([obj])


def read_results(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ---------- Core loaders ----------


def load_experiment(
    method: str, run_id: str, base_dir: str = "results"
) -> Dict[str, Any]:
    """Load metrics.jsonl, output.jsonl, optional queue.json, and results.json for one run."""
    run_path = os.path.join(base_dir, method, str(run_id))
    exp: Dict[str, Any] = {}

    metrics_path = os.path.join(run_path, "metrics.jsonl")
    if os.path.exists(metrics_path):
        exp["metrics"] = read_jsonl(metrics_path)

    output_path = os.path.join(run_path, "output.jsonl")
    if os.path.exists(output_path):
        exp["output"] = read_jsonl(output_path)

    queue_path = os.path.join(run_path, "queue.json")
    if os.path.exists(queue_path):
        exp["queue"] = read_queue(queue_path)

    results_path = os.path.join(run_path, "results.json")
    if os.path.exists(results_path):
        exp["results"] = read_results(results_path)

    # Parse and sort by timestamp if present
    for key in ("metrics", "output", "queue"):
        df = exp.get(key)
        if isinstance(df, pd.DataFrame) and not df.empty and "ts" in df.columns:
            df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
            exp[key] = df.sort_values("ts").reset_index(drop=True)

    return exp


# ---------- Transformations ----------


def normalize_metrics_samples(
    metrics_df: pd.DataFrame, method: str, run_id: str
) -> pd.DataFrame:
    """Explode metrics.samples list into per-instance rows with context columns."""
    if metrics_df is None or metrics_df.empty or "samples" not in metrics_df.columns:
        return pd.DataFrame()
    rows = []
    for _, row in metrics_df.iterrows():
        ts = row.get("ts")
        mode = row.get("mode")
        samples = row.get("samples") or []
        for s in samples:
            item = {"method": method, "run_id": run_id, "ts": ts, "mode": mode}
            if isinstance(s, dict):
                item.update(s)
            rows.append(item)
    out = pd.DataFrame(rows)
    if "ts" in out.columns:
        out["ts"] = pd.to_datetime(out["ts"], errors="coerce")
    if not out.empty:
        sort_cols = ["ts"] + [c for c in ("instance",) if c in out.columns]
        out = out.sort_values(sort_cols).reset_index(drop=True)
    return out


# ---------- Helpers for multiple selected experiments ----------


def make_runs_index(
    experiments: List[Tuple[str, str]], base_dir: str = "results"
) -> pd.DataFrame:
    """Build a minimal runs index from explicit (method, run_id) pairs."""
    rows = []
    for m, r in experiments:
        rows.append(
            {
                "method": m,
                "run_id": str(r),
                "metrics_path": os.path.join(base_dir, m, str(r), "metrics.jsonl"),
                "output_path": os.path.join(base_dir, m, str(r), "output.jsonl"),
                "queue_path": os.path.join(base_dir, m, str(r), "queue.json"),
                "results_path": os.path.join(base_dir, m, str(r), "results.json"),
                "base_dir": base_dir,
            }
        )
    return pd.DataFrame(rows)


def collect_results_summary(runs_index: pd.DataFrame) -> pd.DataFrame:
    """Flatten results.json across selected experiments."""
    records = []
    for _, row in runs_index.iterrows():
        rp = row.get("results_path")
        if rp and os.path.exists(rp):
            try:
                data = read_results(rp)
                rec = {"method": row["method"], "run_id": row["run_id"]}
                rec.update(data)
                records.append(rec)
            except Exception as e:
                print("Error reading:", rp, e)
    return pd.DataFrame(records)


def collect_all_metrics(runs_index: pd.DataFrame) -> pd.DataFrame:
    """Concatenate normalized per-instance metrics across selected experiments."""
    frames = []
    for _, row in runs_index.iterrows():
        mp = row.get("metrics_path")
        if mp and os.path.exists(mp):
            df = read_jsonl(mp)
            df_norm = normalize_metrics_samples(df, row["method"], row["run_id"])
            frames.append(df_norm)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def collect_all_outputs(runs_index: pd.DataFrame) -> pd.DataFrame:
    """Concatenate output.jsonl across selected experiments."""
    frames = []
    for _, row in runs_index.iterrows():
        op = row.get("output_path")
        if op and os.path.exists(op):
            df = read_jsonl(op)
            df["method"] = row["method"]
            df["run_id"] = row["run_id"]
            if "ts" in df.columns:
                df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
            frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ---------- Flexible selectors for aggregated metrics ----------


def _coerce_iter(x):
    if x is None:
        return None
    if isinstance(x, (list, tuple, set, pd.Series)):
        return list(x)
    return [x]


def select_metrics(
    metrics_all: pd.DataFrame,
    endpoints=None,  # "10.1.62.151:8200" or ["10.1.62.151:8200", ...]
    modes=None,  # "rr-batching" or ["rr-batching", "pull-batching"]
    experiments=None,  # "17" or ["17","29"] or [("rr-batching","17"), ...]
    columns=None,  # ["ts","instance","gen_tokens_per_sec",...]
) -> pd.DataFrame:
    """
    Filter rows by endpoint instance(s), mode(s), and experiment id(s),
    and optionally project selected columns. Returns a copy.
    """
    if metrics_all is None or metrics_all.empty:
        return pd.DataFrame()

    df = metrics_all.copy()

    # Filter by endpoints (instance)
    endpoints = _coerce_iter(endpoints)
    if endpoints:
        if "instance" not in df.columns:
            raise KeyError(
                "Column 'instance' not present in metrics_all. Did you normalize metrics.samples?"
            )
        df = df[df["instance"].isin(endpoints)]

    # Filter by mode
    modes = _coerce_iter(modes)
    if modes:
        if "mode" not in df.columns:
            raise KeyError("Column 'mode' not present in metrics_all.")
        df = df[df["mode"].isin(modes)]

    # Filter by experiments
    experiments = _coerce_iter(experiments)
    if experiments:
        has_method = "method" in df.columns
        has_runid = "run_id" in df.columns
        if not has_runid:
            raise KeyError("Column 'run_id' not present in metrics_all.")

        pair_like, runid_only = [], []
        for e in experiments:
            if isinstance(e, (tuple, list)) and len(e) == 2:
                pair_like.append((str(e[0]), str(e[1])))
            else:
                runid_only.append(str(e))

        mask = pd.Series(True, index=df.index)
        if runid_only:
            mask = mask & df["run_id"].astype(str).isin(runid_only)
        if pair_like:
            if not has_method:
                raise KeyError(
                    "Column 'method' not present in metrics_all; cannot filter by (method, run_id) pairs."
                )
            pairs_df = df[["method", "run_id"]].astype(str).apply(tuple, axis=1)
            mask = mask & pairs_df.isin(pair_like)

        df = df[mask]

    # Column projection
    columns = _coerce_iter(columns)
    if columns:
        missing = [c for c in columns if c not in df.columns]
        if missing:
            raise KeyError(f"Requested columns not found: {missing}")
        df = df[columns]

    # Ensure chronological order if 'ts' exists
    if "ts" in df.columns:
        df = df.sort_values("ts")

    return df.reset_index(drop=True)


# ---------- Endpoint-wise splitting ----------


def metrics_to_endpoint_tables(df: pd.DataFrame) -> dict:
    """
    Split normalized metrics DataFrame into a dict of DataFrames,
    keyed by endpoint (instance). Each DF keeps only rows for that endpoint.
    """
    if df is None or df.empty:
        return {}
    if "instance" not in df.columns:
        raise KeyError(
            "metrics_to_endpoint_tables: 'instance' column not found. Did you normalize metrics.samples?"
        )
    tables = {}
    for inst, g in df.groupby("instance", dropna=False):
        gg = g.sort_values("ts") if "ts" in g.columns else g.copy()
        tables[inst] = gg.reset_index(drop=True)
    return tables


# ---------- Metric listing ----------


def list_numeric_metrics(df, exclude=None):
    """Return numeric columns in df, excluding any in 'exclude'."""
    if df is None or df.empty:
        return []
    if exclude is None:
        exclude = set()
    else:
        exclude = set(exclude)
    cols = []
    for c in df.columns:
        if c in exclude:
            continue
        dt = df[c].dtype if c in df.columns else None
        if str(dt).startswith(("float", "int")):
            cols.append(c)
    return cols


# ---------- Derived metrics utilities ----------

_EXPR_RE = re.compile(r"^\s*([A-Za-z0-9_]+)\s*([/\-\+\*])\s*([A-Za-z0-9_]+)\s*$")


def _safe_div(a: pd.Series, b: pd.Series) -> pd.Series:
    """Safe division: returns NaN where b==0 or b is NaN."""
    return a.astype("float64") / b.astype("float64")


def _binary_op(
    df: pd.DataFrame, left: str, op: str, right: str, out_col: str
) -> pd.DataFrame:
    """Create out_col = left <op> right if both columns exist."""
    if left not in df.columns or right not in df.columns:
        return df
    if op == "/":
        df[out_col] = _safe_div(df[left], df[right])
    elif op == "-":
        df[out_col] = df[left].astype("float64") - df[right].astype("float64")
    elif op == "+":
        df[out_col] = df[left].astype("float64") + df[right].astype("float64")
    elif op == "*":
        df[out_col] = df[left].astype("float64") * df[right].astype("float64")
    return df


def maybe_derive_column(df: pd.DataFrame, metric: str) -> pd.DataFrame:
    """
    If metric looks like 'A/B' or 'A-B' (also '+','*'), derive it on the fly.
    Returns df with metric column added if possible.
    """
    if metric in df.columns:
        return df
    m = _EXPR_RE.match(metric or "")
    if not m:
        return df
    left, op, right = m.group(1), m.group(2), m.group(3)
    return _binary_op(df.copy(), left, op, right, out_col=metric)


def add_standard_derived_output_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds common derived metrics for output.jsonl if source fields exist:
      - http_latency_s = t_recv_http_s - t_send_http_s
      - end2end_latency_s = t_recv_http_s - t_enq_client_s
      - total_tokens = in_tokens + out_tokens
      - throughput_toks_per_s = out_tokens / latency_s
      - server_toks_per_s = out_tokens / sim_service_s (if sim_service_s)
    """
    if df is None or df.empty:
        return df

    df = df.copy()

    # Latencies from timestamps
    if {"t_recv_http_s", "t_send_http_s"}.issubset(
        df.columns
    ) and "http_latency_s" not in df.columns:
        df["http_latency_s"] = df["t_recv_http_s"].astype("float64") - df[
            "t_send_http_s"
        ].astype("float64")

    if {"t_recv_http_s", "t_enq_client_s"}.issubset(
        df.columns
    ) and "end2end_latency_s" not in df.columns:
        df["end2end_latency_s"] = df["t_recv_http_s"].astype("float64") - df[
            "t_enq_client_s"
        ].astype("float64")

    # Token totals
    if {"in_tokens", "out_tokens"}.issubset(
        df.columns
    ) and "total_tokens" not in df.columns:
        df["total_tokens"] = df["in_tokens"].astype("float64") + df[
            "out_tokens"
        ].astype("float64")

    # Throughput based on client-observed HTTP latency or provided latency_s
    base_lat_col = "latency_s" if "latency_s" in df.columns else None
    if "http_latency_s" in df.columns:
        base_lat_col = "http_latency_s"  # prefer pure HTTP latency if present

    if (
        base_lat_col
        and "out_tokens" in df.columns
        and "throughput_toks_per_s" not in df.columns
    ):
        df["throughput_toks_per_s"] = _safe_div(df["out_tokens"], df[base_lat_col])

    # Server-side throughput (if sim service time is available)
    if {"out_tokens", "sim_service_s"}.issubset(
        df.columns
    ) and "server_toks_per_s" not in df.columns:
        df["server_toks_per_s"] = _safe_div(df["out_tokens"], df["sim_service_s"])

    return df


# ---------- Plotting ----------

# --- Shared resampling helper (handles different cadences cleanly) ---
import matplotlib.pyplot as plt
import pandas as pd


def _resample_series(
    df: pd.DataFrame,
    time_col: str,
    value_col: str,
    freq: str | None,
    how: str = "mean",
    ffill: bool = False,
) -> pd.Series:
    """
    Return a time-indexed Series resampled to `freq`.
    - how="mean" for request/event-driven signals (metrics/output).
    - how="last" + ffill=True for queue/state signals.
    """
    s = df[[time_col, value_col]].dropna()
    if s.empty:
        return pd.Series(dtype="float64")

    s = s.set_index(time_col)[value_col].sort_index()

    if not freq:
        return s  # leave native cadence

    if how == "mean":
        sr = s.resample(freq).mean()
    elif how in ("last", "pad"):
        sr = s.resample(freq).last()
        if ffill:
            sr = sr.ffill()
    else:
        # fallback: try pandas reduction by name
        sr = getattr(s.resample(freq), how)()
    return sr


# --- Draw request/event-driven metric over time (metrics.jsonl) ---
def draw_metric_time(
    method: str,
    run_id: str,
    metric: str,
    *,
    endpoints=None,
    base_dir: str = "results",
    resample: str | None = None,  # e.g., "1S"
    title: str | None = None,
    save_path: str | None = None,
) -> None:
    from utils_analysis import (
        load_experiment,
        normalize_metrics_samples,
        metrics_to_endpoint_tables,
        maybe_derive_column,
    )

    exp = load_experiment(method, run_id, base_dir=base_dir)
    mdf = exp.get("metrics")
    if mdf is None or mdf.empty:
        raise ValueError(f"No metrics.jsonl found or empty for {method}/{run_id}")

    norm = normalize_metrics_samples(mdf, method, run_id)
    if norm.empty:
        raise ValueError("Normalized metrics are empty (no 'samples' data).")
    if "ts" not in norm.columns:
        raise KeyError("'ts' column missing in normalized metrics.")

    # Try to derive if not present
    if metric not in norm.columns:
        norm = maybe_derive_column(norm, metric)

    if metric not in norm.columns:
        numeric_cols = [
            c for c, dt in norm.dtypes.items() if str(dt).startswith(("float", "int"))
        ]
        raise KeyError(
            f"Metric '{metric}' not found/derivable. Numeric columns include: {numeric_cols[:12]}"
        )

    if endpoints is not None:
        if isinstance(endpoints, str):
            endpoints = [endpoints]
        norm = norm[norm["instance"].isin(endpoints)]
        if norm.empty:
            raise ValueError("No rows after endpoint filter.")

    tables = metrics_to_endpoint_tables(norm)
    if not tables:
        raise ValueError("No endpoints found.")

    fig, ax = plt.subplots(figsize=(9, 4))
    for inst, df_ep in tables.items():
        sr = _resample_series(df_ep, "ts", metric, resample, how="mean", ffill=False)
        if sr.empty:
            continue
        ax.plot(sr.index, sr.values, label=str(inst))

    ax.set_title(title or f"{metric} over time — {method}/{run_id} (metrics)")
    ax.set_xlabel("time")
    ax.set_ylabel(metric)
    ax.legend(loc="best")
    ax.grid(True, linestyle="--", alpha=0.3)
    if save_path:
        plt.savefig(save_path, bbox_inches="tight", dpi=150)
    plt.show()


# --- Draw request-level metric over time (output.jsonl) ---
def draw_output_time(
    method: str,
    run_id: str,
    metric: str = "latency_s",
    *,
    endpoints=None,  # None = all; or str/list[str] of endpoint URLs
    base_dir: str = "results",
    resample: str | None = None,  # e.g., "1S" (ignored when per_request=True)
    per_request: bool = False,  # show raw per-request points
    title: str | None = None,
    save_path: str | None = None,
    ylim: tuple[float, float] | None = None,  # y-axis limit
) -> None:
    """
    Plot a metric from output.jsonl over time, overlaying one line per endpoint.
    Supports derived metrics via A/B, A-B, and standard derived outputs (see add_standard_derived_output_metrics).
    """
    import matplotlib.pyplot as plt
    import pandas as pd
    from utils_analysis import (
        load_experiment,
        list_numeric_metrics,
        _resample_series,
        maybe_derive_column,
        add_standard_derived_output_metrics,
    )

    exp = load_experiment(method, run_id, base_dir=base_dir)
    odf = exp.get("output")
    if odf is None or odf.empty:
        raise ValueError(f"No output.jsonl found or empty for {method}/{run_id}")

    if "ts" not in odf.columns:
        raise KeyError("'ts' column missing in output.jsonl")
    if "endpoint" not in odf.columns:
        raise KeyError("'endpoint' column missing in output.jsonl")

    odf = odf.copy()
    odf["ts"] = pd.to_datetime(odf["ts"], errors="coerce")
    odf = odf.sort_values("ts")

    # Add standard derived columns (latencies, totals, throughputs, etc.)
    odf = add_standard_derived_output_metrics(odf)

    # Try expression-derived metrics if still missing
    if metric not in odf.columns:
        odf = maybe_derive_column(odf, metric)

    if metric not in odf.columns:
        nums = list_numeric_metrics(odf, exclude={"ts"})
        raise KeyError(
            f"Metric '{metric}' not in output.jsonl and not derivable. Numeric columns include: {nums[:12]}"
        )

    if endpoints is not None:
        if isinstance(endpoints, str):
            endpoints = [endpoints]
        odf = odf[odf["endpoint"].isin(endpoints)]
        if odf.empty:
            raise ValueError("No rows after endpoint filter.")

    fig, ax = plt.subplots(figsize=(9, 4))

    for ep, g in odf.groupby("endpoint", dropna=False):
        series = g[["ts", metric]].dropna()
        if series.empty:
            continue

        if per_request:
            ax.plot(
                series["ts"].values,
                series[metric].values,
                linestyle="none",
                marker="o",
                markersize=3,
                label=str(ep),
            )
        else:
            s = _resample_series(
                series, "ts", metric, resample, how="mean", ffill=False
            )
            ax.plot(s.index, s.values, label=str(ep))

    ax.set_title(title or f"{metric} over time — {method}/{run_id} (output)")
    ax.set_xlabel("time")
    ax.set_ylabel(metric)
    ax.legend(loc="best")
    ax.grid(True, which="both", linestyle="--", alpha=0.3)

    if ylim is not None:
        ax.set_ylim(*ylim)

    if save_path:
        plt.savefig(save_path, bbox_inches="tight", dpi=150)
    plt.show()


# --- Draw queue/state metric over time (queue.json) ---
def draw_queue_time(
    method: str,
    run_id: str,
    metric: str = "q_after",
    *,
    endpoints=None,
    base_dir: str = "results",
    resample: str | None = None,  # e.g., "1S"
    title: str | None = None,
    save_path: str | None = None,
) -> None:
    from utils_analysis import load_experiment, maybe_derive_column

    exp = load_experiment(method, run_id, base_dir=base_dir)
    qdf = exp.get("queue")
    if qdf is None or qdf.empty:
        raise ValueError(f"No queue.json found or empty for {method}/{run_id}")

    if "ts" not in qdf.columns:
        raise KeyError("'ts' column missing in queue.json")
    if "endpoint" not in qdf.columns:
        raise KeyError("'endpoint' column missing in queue.json")

    qdf = qdf.copy()
    qdf["ts"] = pd.to_datetime(qdf["ts"], errors="coerce")
    qdf = qdf.sort_values("ts")

    # Allow derived expressions here too (e.g., inflight_after - inflight_before)
    if metric not in qdf.columns:
        qdf = maybe_derive_column(qdf, metric)

    if metric not in qdf.columns:
        nums = [
            c for c, dt in qdf.dtypes.items() if str(dt).startswith(("float", "int"))
        ]
        raise KeyError(
            f"Metric '{metric}' not in queue.json and not derivable. Numeric columns include: {nums[:12]}"
        )

    if endpoints is not None:
        if isinstance(endpoints, str):
            endpoints = [endpoints]
        qdf = qdf[qdf["endpoint"].isin(endpoints)]
        if qdf.empty:
            raise ValueError("No rows after endpoint filter.")

    fig, ax = plt.subplots(figsize=(9, 4))
    for ep, g in qdf.groupby("endpoint", dropna=False):
        # Queue/state: use last + ffill for stable steps; plot as step to show instantaneous state changes
        sr = _resample_series(g, "ts", metric, resample, how="last", ffill=True)
        if sr.empty:
            continue
        ax.plot(sr.index, sr.values, label=str(ep), drawstyle="steps-post")

    ax.set_title(title or f"{metric} over time — {method}/{run_id} (queue)")
    ax.set_xlabel("time")
    ax.set_ylabel(metric)
    ax.legend(loc="best")
    ax.grid(True, linestyle="--", alpha=0.3)
    if save_path:
        plt.savefig(save_path, bbox_inches="tight", dpi=150)
    plt.show()


def draw_metric_time_each(
    experiments,
    metric,
    *,
    endpoints=None,
    base_dir="results",
    resample=None,
    title=None,
    save_path_template=None,
):
    """
    Draw metrics.jsonl 'metric' for each (method, run_id) in experiments,
    as separate figures (one plot per experiment).
    - save_path_template: optional format string with {method}, {run_id}, {metric}
      e.g., "plots/{method}_{run_id}_{metric}.png"
    """
    for method, run_id in experiments:
        save_path = None
        if save_path_template:
            save_path = save_path_template.format(
                method=method, run_id=run_id, metric=metric
            )

        draw_metric_time(
            method,
            run_id,
            metric,
            endpoints=endpoints,
            base_dir=base_dir,
            resample=resample,
            title=title,
            save_path=save_path,
        )


def draw_output_time_each(
    experiments,
    metric="latency_s",
    *,
    endpoints=None,
    base_dir="results",
    resample=None,
    per_request=False,
    title=None,
    save_path_template=None,
    ylim: tuple[float, float] | None = None,
):
    """
    Draw output.jsonl 'metric' for each experiment as separate figures.
    """
    for method, run_id in experiments:
        save_path = None
        if save_path_template:
            save_path = save_path_template.format(
                method=method, run_id=run_id, metric=metric
            )

        draw_output_time(
            method,
            run_id,
            metric,
            endpoints=endpoints,
            base_dir=base_dir,
            resample=resample,
            per_request=per_request,
            title=title,
            save_path=save_path,
            ylim=ylim,
        )


def draw_queue_time_each(
    experiments,
    metric,
    *,
    endpoints=None,
    base_dir="results",
    resample=None,
    title=None,
    save_path_template=None,
):
    """
    Draw queue.json 'metric' for each experiment as separate figures.
    - save_path_template: optional format string with {method}, {run_id}, {metric}
    """
    for method, run_id in experiments:
        save_path = None
        if save_path_template:
            save_path = save_path_template.format(
                method=method, run_id=run_id, metric=metric
            )

        draw_queue_time(
            method,
            run_id,
            metric,
            endpoints=endpoints,
            base_dir=base_dir,
            resample=resample,
            title=title,
            save_path=save_path,
        )


# ---------- Statistical summaries ----------


def _as_list(x):
    if x is None:
        return None
    if isinstance(x, (list, tuple, set, pd.Series, np.ndarray)):
        return list(x)
    return [x]


def _format_percentile_cols(percentiles):
    out = []
    for p in percentiles:
        # accept 0-1.0 or 0-100 input; normalize to 0-100
        q = float(p)
        if q <= 1.0:
            q *= 100.0
        out.append(f"p{int(round(q))}")
    return out


def _quantiles(series: pd.Series, percentiles) -> Dict[str, float]:
    if series.empty:
        return {f"p{int(round((p if p>1 else p*100)))}": np.nan for p in percentiles}
    probs = [p if p > 1 else p for p in (np.array(percentiles) / 100.0)]
    # If percentiles were already in 0-1, dividing again would be wrong; correct for that:
    probs = []
    for p in percentiles:
        p = float(p)
        probs.append(p / 100.0 if p > 1.0 else p)
    qs = series.quantile(probs, interpolation="linear")
    return {
        f"p{int(round((p if p>1 else p*100)))}": float(qs.iloc[i])
        for i, p in enumerate(percentiles)
    }


def _basic_stats(series: pd.Series, percentiles=(50, 90, 95, 99)) -> Dict[str, float]:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return {
            "count": 0,
            "mean": np.nan,
            "std": np.nan,
            "min": np.nan,
            "max": np.nan,
            **_quantiles(s, percentiles),
        }
    base = {
        "count": int(s.count()),
        "mean": float(s.mean()),
        "std": float(s.std(ddof=1)) if s.count() > 1 else 0.0,
        "min": float(s.min()),
        "max": float(s.max()),
    }
    base.update(_quantiles(s, percentiles))
    return base


def _maybe_filter_endpoints(df: pd.DataFrame, endpoints, endpoint_col: str):
    if endpoints is None:
        return df
    endpoints = _as_list(endpoints)
    return df[df[endpoint_col].isin(endpoints)]


def _ensure_ts_sorted(df: pd.DataFrame) -> pd.DataFrame:
    if "ts" in df.columns:
        df = df.copy()
        df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
        df = df.sort_values("ts")
    return df


def _maybe_derive_metric_output(df: pd.DataFrame, metric: str) -> pd.DataFrame:
    # Add standard derived columns and expression support for output.jsonl
    df = add_standard_derived_output_metrics(df)
    if metric not in df.columns:
        df = maybe_derive_column(df, metric)
    return df


def summarize_output_metric(
    method: str,
    run_id: str,
    metric: str = "latency_s",
    *,
    endpoints=None,
    percentiles=(50, 90, 95, 99),
    base_dir: str = "results",
    by_endpoint: bool = True,
) -> pd.DataFrame:
    """
    Compute summary stats for an output.jsonl metric (supports derived metrics).
    Returns a DataFrame with one row per endpoint (default) or a single overall row.
    Columns: count, mean, std, min, max, pXX...
    """
    exp = load_experiment(method, run_id, base_dir=base_dir)
    odf = exp.get("output")
    if odf is None or odf.empty:
        raise ValueError(f"No output.jsonl found or empty for {method}/{run_id}")
    if "endpoint" not in odf.columns:
        raise KeyError("'endpoint' column missing in output.jsonl")
    odf = _ensure_ts_sorted(odf.copy())
    odf = _maybe_derive_metric_output(odf, metric)

    if metric not in odf.columns:
        raise KeyError(f"Metric '{metric}' not found or derivable in output.jsonl")
    odf = _maybe_filter_endpoints(odf, endpoints, "endpoint")

    if by_endpoint:
        recs = []
        for ep, g in odf.groupby("endpoint", dropna=False):
            stats = _basic_stats(g[metric], percentiles=percentiles)
            stats["endpoint"] = ep
            recs.append(stats)
        df = pd.DataFrame(recs)
        # order columns
        pcols = _format_percentile_cols(percentiles)
        return df[["endpoint", "count", "mean", "std", "min", "max", *pcols]]
    else:
        stats = _basic_stats(odf[metric], percentiles=percentiles)
        pcols = _format_percentile_cols(percentiles)
        df = pd.DataFrame([{**stats, "endpoint": "ALL"}])
        return df[["endpoint", "count", "mean", "std", "min", "max", *pcols]]


def cumulative_output_metric(
    method: str,
    run_id: str,
    metric: str = "latency_s",
    *,
    endpoints=None,
    base_dir: str = "results",
    by_endpoint: bool = True,
) -> pd.DataFrame:
    """
    Return a time-indexed cumulative table for an output.jsonl metric:
      ts, endpoint, value, cumcount, cumsum, cummean
    If by_endpoint=False, aggregates across all endpoints into a single series.
    """
    exp = load_experiment(method, run_id, base_dir=base_dir)
    odf = exp.get("output")
    if odf is None or odf.empty:
        raise ValueError(f"No output.jsonl found or empty for {method}/{run_id}")
    if "endpoint" not in odf.columns or "ts" not in odf.columns:
        raise KeyError("'endpoint' or 'ts' column missing in output.jsonl")
    odf = _ensure_ts_sorted(odf.copy())
    odf = _maybe_derive_metric_output(odf, metric)
    if metric not in odf.columns:
        raise KeyError(f"Metric '{metric}' not found/derivable in output.jsonl")
    odf = _maybe_filter_endpoints(odf, endpoints, "endpoint")

    cols = ["ts", "endpoint", metric]
    odf = odf[cols].dropna()
    if by_endpoint:
        frames = []
        for ep, g in odf.groupby("endpoint", dropna=False):
            gg = g.sort_values("ts").copy()
            gg["cumcount"] = np.arange(1, len(gg) + 1, dtype=int)
            gg["cumsum"] = gg[metric].astype("float64").cumsum()
            gg["cummean"] = gg["cumsum"] / gg["cumcount"]
            frames.append(gg)
        return (
            pd.concat(frames, ignore_index=True)
            if frames
            else pd.DataFrame(
                columns=["ts", "endpoint", metric, "cumcount", "cumsum", "cummean"]
            )
        )
    else:
        g = odf.sort_values("ts").copy()
        g["endpoint"] = "ALL"
        g["cumcount"] = np.arange(1, len(g) + 1, dtype=int)
        g["cumsum"] = g[metric].astype("float64").cumsum()
        g["cummean"] = g["cumsum"] / g["cumcount"]
        return g


def cumulative_metrics_metric(
    method: str,
    run_id: str,
    metric: str,
    *,
    endpoints=None,  # instances
    base_dir: str = "results",
    by_endpoint: bool = True,
) -> pd.DataFrame:
    """
    Cumulative table for a metrics.jsonl metric (after normalizing samples).
    """
    exp = load_experiment(method, run_id, base_dir=base_dir)
    mdf = exp.get("metrics")
    if mdf is None or mdf.empty:
        raise ValueError(f"No metrics.jsonl found or empty for {method}/{run_id}")
    norm = normalize_metrics_samples(mdf, method, run_id)
    if norm.empty:
        return pd.DataFrame(
            columns=["ts", "instance", metric, "cumcount", "cumsum", "cummean"]
        )
    norm = _ensure_ts_sorted(norm)
    if metric not in norm.columns:
        norm = maybe_derive_column(norm, metric)
    if metric not in norm.columns:
        raise KeyError(f"Metric '{metric}' not found/derivable in metrics.jsonl")
    if endpoints is not None:
        endpoints = _as_list(endpoints)
        norm = norm[norm["instance"].isin(endpoints)]
    cols = ["ts", "instance", metric]
    norm = norm[cols].dropna()

    if by_endpoint:
        frames = []
        for inst, g in norm.groupby("instance", dropna=False):
            gg = g.sort_values("ts").copy()
            gg["cumcount"] = np.arange(1, len(gg) + 1, dtype=int)
            gg["cumsum"] = gg[metric].astype("float64").cumsum()
            gg["cummean"] = gg["cumsum"] / gg["cumcount"]
            frames.append(gg)
        return (
            pd.concat(frames, ignore_index=True)
            if frames
            else pd.DataFrame(
                columns=["ts", "instance", metric, "cumcount", "cumsum", "cummean"]
            )
        )
    else:
        g = norm.sort_values("ts").copy()
        g["instance"] = "ALL"
        g["cumcount"] = np.arange(1, len(g) + 1, dtype=int)
        g["cumsum"] = g[metric].astype("float64").cumsum()
        g["cummean"] = g["cumsum"] / g["cumcount"]
        return g


def cumulative_queue_metric(
    method: str,
    run_id: str,
    metric: str = "q_after",
    *,
    endpoints=None,
    base_dir: str = "results",
    by_endpoint: bool = True,
) -> pd.DataFrame:
    """
    Cumulative table for queue.json metric (sample-based).
    Note: This is cumulative over sample values, not time-weighted.
    """
    exp = load_experiment(method, run_id, base_dir=base_dir)
    qdf = exp.get("queue")
    if qdf is None or qdf.empty:
        raise ValueError(f"No queue.json found or empty for {method}/{run_id}")
    if "endpoint" not in qdf.columns or "ts" not in qdf.columns:
        raise KeyError("'endpoint' or 'ts' column missing in queue.json")
    qdf = _ensure_ts_sorted(qdf.copy())
    if metric not in qdf.columns:
        qdf = maybe_derive_column(qdf, metric)
    if metric not in qdf.columns:
        raise KeyError(f"Metric '{metric}' not found/derivable in queue.json")
    qdf = _maybe_filter_endpoints(qdf, endpoints, "endpoint")
    cols = ["ts", "endpoint", metric]
    qdf = qdf[cols].dropna()

    if by_endpoint:
        frames = []
        for ep, g in qdf.groupby("endpoint", dropna=False):
            gg = g.sort_values("ts").copy()
            gg["cumcount"] = np.arange(1, len(gg) + 1, dtype=int)
            gg["cumsum"] = gg[metric].astype("float64").cumsum()
            gg["cummean"] = gg["cumsum"] / gg["cumcount"]
            frames.append(gg)
        return (
            pd.concat(frames, ignore_index=True)
            if frames
            else pd.DataFrame(
                columns=["ts", "endpoint", metric, "cumcount", "cumsum", "cummean"]
            )
        )
    else:
        g = qdf.sort_values("ts").copy()
        g["endpoint"] = "ALL"
        g["cumcount"] = np.arange(1, len(g) + 1, dtype=int)
        g["cumsum"] = g[metric].astype("float64").cumsum()
        g["cummean"] = g["cumsum"] / g["cumcount"]
        return g


# --- updated _basic_stats (adds "sum") ---
def _basic_stats(series: pd.Series, percentiles=(50, 90, 95, 99)) -> Dict[str, float]:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return {
            "count": 0,
            "sum": 0.0,
            "mean": np.nan,
            "std": np.nan,
            "min": np.nan,
            "max": np.nan,
            **_quantiles(s, percentiles),
        }
    base = {
        "count": int(s.count()),
        "sum": float(s.sum()),
        "mean": float(s.mean()),
        "std": float(s.std(ddof=1)) if s.count() > 1 else 0.0,
        "min": float(s.min()),
        "max": float(s.max()),
    }
    base.update(_quantiles(s, percentiles))
    return base


# --- updated summarize_output_metric ---
def summarize_output_metric(
    method: str,
    run_id: str,
    metric: str = "latency_s",
    *,
    endpoints=None,
    percentiles=(50, 90, 95, 99),
    base_dir: str = "results",
    by_endpoint: bool = True,
    include_overall: bool = False,
) -> pd.DataFrame:
    exp = load_experiment(method, run_id, base_dir=base_dir)
    odf = exp.get("output")
    if odf is None or odf.empty:
        raise ValueError(f"No output.jsonl found or empty for {method}/{run_id}")
    if "endpoint" not in odf.columns:
        raise KeyError("'endpoint' column missing in output.jsonl")

    odf = _ensure_ts_sorted(odf.copy())
    odf = _maybe_derive_metric_output(odf, metric)
    if metric not in odf.columns:
        raise KeyError(f"Metric '{metric}' not found/derivable in output.jsonl")
    odf = _maybe_filter_endpoints(odf, endpoints, "endpoint")

    if by_endpoint:
        recs = []
        for ep, g in odf.groupby("endpoint", dropna=False):
            stats = _basic_stats(g[metric], percentiles=percentiles)
            stats["endpoint"] = ep
            recs.append(stats)
        df = pd.DataFrame(recs)
        pcols = _format_percentile_cols(percentiles)
        df = df[["endpoint", "count", "sum", "mean", "std", "min", "max", *pcols]]

        if include_overall:
            overall = _basic_stats(odf[metric], percentiles=percentiles)
            overall["endpoint"] = "ALL"
            overall = pd.DataFrame([overall])[
                ["endpoint", "count", "sum", "mean", "std", "min", "max", *pcols]
            ]
            df = pd.concat([df, overall], ignore_index=True)
        return df
    else:
        stats = _basic_stats(odf[metric], percentiles=percentiles)
        pcols = _format_percentile_cols(percentiles)
        df = pd.DataFrame([{**stats, "endpoint": "ALL"}])
        return df[["endpoint", "count", "sum", "mean", "std", "min", "max", *pcols]]


def summarize_metrics_metric(
    method: str,
    run_id: str,
    metric: str,
    *,
    endpoints=None,  # 'instance' values
    percentiles=(50, 90, 95, 99),
    base_dir: str = "results",
    by_endpoint: bool = True,
    include_overall: bool = False,
) -> pd.DataFrame:
    exp = load_experiment(method, run_id, base_dir=base_dir)
    mdf = exp.get("metrics")
    if mdf is None or mdf.empty:
        raise ValueError(f"No metrics.jsonl found or empty for {method}/{run_id}")
    norm = normalize_metrics_samples(mdf, method, run_id)
    if norm.empty:
        raise ValueError("Normalized metrics are empty (no 'samples' data).")
    if metric not in norm.columns:
        norm = maybe_derive_column(norm, metric)
    if metric not in norm.columns:
        raise KeyError(f"Metric '{metric}' not found/derivable in metrics.jsonl")
    norm = _ensure_ts_sorted(norm)
    if endpoints is not None:
        endpoints = _as_list(endpoints)
        norm = norm[norm["instance"].isin(endpoints)]

    if by_endpoint:
        recs = []
        for inst, g in norm.groupby("instance", dropna=False):
            stats = _basic_stats(g[metric], percentiles=percentiles)
            stats["instance"] = inst
            recs.append(stats)
        df = pd.DataFrame(recs)
        pcols = _format_percentile_cols(percentiles)
        df = df[["instance", "count", "sum", "mean", "std", "min", "max", *pcols]]

        if include_overall:
            overall = _basic_stats(norm[metric], percentiles=percentiles)
            overall["instance"] = "ALL"
            overall = pd.DataFrame([overall])[
                ["instance", "count", "sum", "mean", "std", "min", "max", *pcols]
            ]
            df = pd.concat([df, overall], ignore_index=True)
        return df
    else:
        stats = _basic_stats(norm[metric], percentiles=percentiles)
        pcols = _format_percentile_cols(percentiles)
        df = pd.DataFrame([{**stats, "instance": "ALL"}])
        return df[["instance", "count", "sum", "mean", "std", "min", "max", *pcols]]


def summarize_queue_metric(
    method: str,
    run_id: str,
    metric: str = "q_after",
    *,
    endpoints=None,
    percentiles=(50, 90, 95, 99),
    base_dir: str = "results",
    by_endpoint: bool = True,
    include_overall: bool = False,
) -> pd.DataFrame:
    exp = load_experiment(method, run_id, base_dir=base_dir)
    qdf = exp.get("queue")
    if qdf is None or qdf.empty:
        raise ValueError(f"No queue.json found or empty for {method}/{run_id}")
    if "endpoint" not in qdf.columns:
        raise KeyError("'endpoint' column missing in queue.json")

    qdf = _ensure_ts_sorted(qdf.copy())
    if metric not in qdf.columns:
        qdf = maybe_derive_column(qdf, metric)
    if metric not in qdf.columns:
        raise KeyError(f"Metric '{metric}' not found/derivable in queue.json")
    qdf = _maybe_filter_endpoints(qdf, endpoints, "endpoint")

    if by_endpoint:
        recs = []
        for ep, g in qdf.groupby("endpoint", dropna=False):
            stats = _basic_stats(g[metric], percentiles=percentiles)
            stats["endpoint"] = ep
            recs.append(stats)
        df = pd.DataFrame(recs)
        pcols = _format_percentile_cols(percentiles)
        df = df[["endpoint", "count", "sum", "mean", "std", "min", "max", *pcols]]

        if include_overall:
            overall = _basic_stats(qdf[metric], percentiles=percentiles)
            overall["endpoint"] = "ALL"
            overall = pd.DataFrame([overall])[
                ["endpoint", "count", "sum", "mean", "std", "min", "max", *pcols]
            ]
            df = pd.concat([df, overall], ignore_index=True)
        return df
    else:
        stats = _basic_stats(qdf[metric], percentiles=percentiles)
        pcols = _format_percentile_cols(percentiles)
        df = pd.DataFrame([{**stats, "endpoint": "ALL"}])
        return df[["endpoint", "count", "sum", "mean", "std", "min", "max", *pcols]]


# ---------- Metric discovery (simple "just list") ----------


def _list_cols(df: pd.DataFrame, *, numeric_only: bool, exclude: set[str]) -> list[str]:
    if df is None or df.empty:
        return []
    cols = []
    for c in df.columns:
        if c in exclude:
            continue
        if not numeric_only:
            cols.append(c)
        else:
            if str(df[c].dtype).startswith(("float", "int")):
                cols.append(c)
    return sorted(cols)


def list_available_output_metrics(
    method: str,
    run_id: str,
    *,
    base_dir: str = "results",
    include_derived: bool = True,
    numeric_only: bool = True,
) -> list[str]:
    """
    List metric columns available in output.jsonl for a run.
    - include_derived: adds http_latency_s, end2end_latency_s, total_tokens,
      throughput_toks_per_s, server_toks_per_s (when source fields exist).
    - numeric_only: if True, only numeric columns are returned.
    """
    exp = load_experiment(method, run_id, base_dir=base_dir)
    odf = exp.get("output")
    if odf is None or odf.empty:
        return []

    df = odf.copy()
    if include_derived:
        df = add_standard_derived_output_metrics(df)

    exclude = {"ts", "endpoint", "method", "run_id", "prompt", "response", "id"}
    return _list_cols(df, numeric_only=numeric_only, exclude=exclude)


def list_available_metrics_metrics(
    method: str,
    run_id: str,
    *,
    base_dir: str = "results",
    numeric_only: bool = True,
) -> list[str]:
    """
    List metric columns from metrics.jsonl (after normalizing samples) for a run.
    """
    exp = load_experiment(method, run_id, base_dir=base_dir)
    mdf = exp.get("metrics")
    if mdf is None or mdf.empty:
        return []

    norm = normalize_metrics_samples(mdf, method, run_id)
    if norm.empty:
        return []

    exclude = {"ts", "instance", "method", "run_id", "mode"}
    return _list_cols(norm, numeric_only=numeric_only, exclude=exclude)


def list_available_queue_metrics(
    method: str,
    run_id: str,
    *,
    base_dir: str = "results",
    numeric_only: bool = True,
) -> list[str]:
    """
    List metric columns available in queue.json for a run.
    (No built-in derived fields here — just what exists numerically.)
    """
    exp = load_experiment(method, run_id, base_dir=base_dir)
    qdf = exp.get("queue")
    if qdf is None or qdf.empty:
        return []

    exclude = {"ts", "endpoint", "method", "run_id"}
    return _list_cols(qdf, numeric_only=numeric_only, exclude=exclude)


def list_all_metrics_for_run(
    method: str,
    run_id: str,
    *,
    base_dir: str = "results",
    include_output_derived: bool = True,
    numeric_only: bool = True,
) -> dict:
    """
    Convenience helper returning a dict with lists for each file:
      { "output": [...], "metrics": [...], "queue": [...] }
    """
    return {
        "output": list_available_output_metrics(
            method,
            run_id,
            base_dir=base_dir,
            include_derived=include_output_derived,
            numeric_only=numeric_only,
        ),
        "metrics": list_available_metrics_metrics(
            method,
            run_id,
            base_dir=base_dir,
            numeric_only=numeric_only,
        ),
        "queue": list_available_queue_metrics(
            method,
            run_id,
            base_dir=base_dir,
            numeric_only=numeric_only,
        ),
    }


def draw_violin_for_metric(
    dfs,
    value_col="mean",
    group_col="endpoint",
    drop_overall=True,
    title=None,
    save_path=None,
):
    """
    Draws a violin plot across strategies for a chosen numeric column.
    One violin per strategy, built from values across `group_col`.

    dfs: dict, e.g. {"rr-batching": df1, "least-queue-batching": df2, ...}
    value_col: which metric to plot (e.g. "mean", "p50", "p90", "p99")
    group_col: how to group within each strategy (usually "endpoint")
    drop_overall: remove the 'ALL' row
    """
    prepared = {}
    for strategy, df in dfs.items():
        df_use = df.copy()
        if drop_overall and group_col in df_use.columns:
            df_use = df_use[df_use[group_col] != "ALL"]
        prepared[strategy] = df_use[value_col].dropna().values

    data = [prepared[s] for s in prepared]
    labels = list(prepared.keys())

    plt.figure(figsize=(8, 5))
    plt.violinplot(data, showmeans=True, showmedians=False, showextrema=True)
    plt.xticks(np.arange(1, len(labels) + 1), labels, rotation=15)
    plt.xlabel("Strategy")
    plt.ylabel(value_col)
    plt.title(title or f"Distribution of '{value_col}' by strategy")
    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.show()
