# load_runner.py
# Per-request threaded load runner:
# - Optional request-based warmup before the main load.
# - For each (plan_time, prompt), spawn a thread.
# - Each thread sleeps until its scheduled monotonic timestamp, sends the request,
#   and waits for the synchronous response from /enqueue.

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Dict, Any
import threading
import time

import requests

from http_client import send_one
from config import GenerationConfig
from trace_utils import print_trace_block, compute_trace_metrics
from experiment_io import ExperimentLogger


@dataclass
class RequestTask:
    idx: int
    prompt: str
    ts_mono: float  # absolute monotonic timestamp for sending


# Shared state for per-second send statistics
_sec_lock = threading.Lock()
_sec_counts: Dict[int, int] = {}


def _request_thread(
    task: RequestTask,
    router_url: str,
    gen_cfg: GenerationConfig,
    t0_mono: float,
    logger: Optional[ExperimentLogger] = None,
    output_log_mode: str = "preview",
    print_trace: bool = True,
):
    """
    Per-request worker:
      - Sleep until scheduled timestamp
      - Log per-second send stats at the actual send time
      - Log SEND event
      - Build meta
      - Call send_one() with its own Session (blocking until response)
      - Log RECV event with timing, optional output preview, and optional trace fields
      - Append a JSON record to logs.json via ExperimentLogger (if provided).

    output_log_mode:
      - "preview": logs truncated single-line output under key "output"
      - "full":    logs full model output under key "output"

    print_trace:
      - If True: print trace metrics and output_preview to stdout.
      - If False: do NOT print trace block or preview (logs.json unaffected).
    """
    session = requests.Session()
    try:
        now = time.monotonic()
        delay = task.ts_mono - now
        if delay > 0:
            time.sleep(delay)

        # Per-second logging at actual send time
        now_send = time.monotonic()
        rel = now_send - t0_mono
        sec = int(rel)
        with _sec_lock:
            _sec_counts[sec] = _sec_counts.get(sec, 0) + 1
            count_in_sec = _sec_counts[sec]
        print(
            f"[load_runner][SEC] t=[{sec},{sec+1}) sent_so_far_in_sec={count_in_sec}"
        )

        meta = {
            "max_tokens": int(gen_cfg.max_tokens),
            "temperature": float(gen_cfg.temperature),
            "length_mode": gen_cfg.length_mode,
            "enable_thinking": bool(gen_cfg.think),
        }
        if gen_cfg.target_output_tokens is not None:
            meta["target_output_tokens"] = int(gen_cfg.target_output_tokens)
        if gen_cfg.target_total_tokens is not None:
            meta["target_total_tokens"] = int(gen_cfg.target_total_tokens)

        print(
            f"[client][SEND][T{task.idx}] idx={task.idx} "
            f"planned_ts={task.ts_mono:.6f}"
        )

        t0 = time.time()
        try:
            rid, result = send_one(session, router_url, task.prompt, meta)
            t1 = time.time()
            total_wait = t1 - t0

            latency_s: Optional[float] = None
            finish_reason: Optional[str] = None
            output_preview: Optional[str] = None
            output_full: Optional[str] = None

            # --- token usage fields (from result.usage or result.raw.usage) ---
            usage_prompt_tokens: Optional[int] = None
            usage_completion_tokens: Optional[int] = None
            usage_total_tokens: Optional[int] = None

            if isinstance(result, dict):
                # --- main fields ---
                if isinstance(result.get("latency_s"), (int, float)):
                    latency_s = float(result["latency_s"])
                if isinstance(result.get("finish_reason"), str):
                    finish_reason = result["finish_reason"]
                if isinstance(result.get("output"), str):
                    output_full = result["output"]
                    # preview is single-line + truncated for logs/console
                    output_preview = output_full.replace("\n", " ")
                    if len(output_preview) > 120:
                        output_preview = output_preview[:117] + "..."

                # --- usage (tokens) ---
                # Prefer top-level result["usage"], fall back to result["raw"]["usage"]
                usage_dict: Optional[Dict[str, Any]] = None
                u_top = result.get("usage")
                if isinstance(u_top, dict):
                    usage_dict = u_top
                else:
                    raw = result.get("raw")
                    if isinstance(raw, dict):
                        u_raw = raw.get("usage")
                        if isinstance(u_raw, dict):
                            usage_dict = u_raw

                if isinstance(usage_dict, dict):
                    pt = usage_dict.get("prompt_tokens")
                    ct = usage_dict.get("completion_tokens")
                    tt = usage_dict.get("total_tokens")
                    try:
                        if pt is not None:
                            usage_prompt_tokens = int(pt)
                    except Exception:
                        pass
                    try:
                        if ct is not None:
                            usage_completion_tokens = int(ct)
                    except Exception:
                        pass
                    try:
                        if tt is not None:
                            usage_total_tokens = int(tt)
                    except Exception:
                        pass

            if latency_s is not None:
                print(
                    f"[client][RECV][T{task.idx}] idx={task.idx} req_id={rid} "
                    f"wait_wall={total_wait:.3f}s model_latency={latency_s:.3f}s"
                )
            else:
                print(
                    f"[client][RECV][T{task.idx}] idx={task.idx} req_id={rid} "
                    f"wait_wall={total_wait:.3f}s"
                )

            if finish_reason is not None:
                print(f"[client][T{task.idx}]   finish_reason={finish_reason}")

            # ----------------------------------------------------------
            # Only show preview on stdout if print_trace is enabled.
            # (Logs are controlled separately by output_log_mode.)
            # ----------------------------------------------------------
            if print_trace and output_preview is not None:
                print(f"[client][T{task.idx}]   output_preview={output_preview!r}")

            # ==========================================================
            # --- TRACE ADDITION: derived latencies (no raw timestamps)
            # ==========================================================
            trace_dict: Optional[Dict[str, Any]] = None
            trace_metrics: Optional[Dict[str, float]] = None
            if isinstance(result, dict):
                trace = result.get("trace")
                if isinstance(trace, dict):
                    if print_trace:
                        # Prints endpoint, router_mode, derived metrics, extras
                        print_trace_block(task.idx, result)
                    trace_dict = trace
                    trace_metrics = compute_trace_metrics(trace)
            # ==========================================================

            # Persist per-request JSON record if logger is provided.
            if logger is not None:
                # Common fields
                record: Dict[str, Any] = {
                    "idx": task.idx,
                    "req_id": rid,
                    "prompt": task.prompt,
                    "planned_ts_mono": task.ts_mono,
                    "actual_send_ts_mono": now_send,
                    "t0_wall": t0,
                    "t1_wall": t1,
                    "wait_wall_s": total_wait,
                    "model_latency_s": latency_s,
                    "finish_reason": finish_reason,
                }

                # Token usage (if present) — no "usage_" prefix in log keys
                if usage_prompt_tokens is not None:
                    record["prompt_tokens"] = usage_prompt_tokens
                if usage_completion_tokens is not None:
                    record["completion_tokens"] = usage_completion_tokens
                if usage_total_tokens is not None:
                    record["total_tokens"] = usage_total_tokens


                # Decide what goes under "output"
                log_output: Optional[str] = None
                mode = output_log_mode or "preview"
                if mode == "full":
                    # Prefer full text; fall back to preview if for some reason we don't have it
                    if output_full is not None:
                        log_output = output_full
                    elif output_preview is not None:
                        log_output = output_preview
                else:
                    # preview mode: always truncated if we can
                    if output_preview is not None:
                        log_output = output_preview
                    elif output_full is not None:
                        log_output = output_full

                if log_output is not None:
                    record["output"] = log_output

                if trace_dict is not None:
                    record["trace"] = trace_dict
                if trace_metrics is not None:
                    record["trace_metrics"] = trace_metrics
                logger.log_request(record)

        except Exception as e:
            print(f"[client][RECV][T{task.idx}] ✗ ERROR idx={task.idx}: {e}")
            if logger is not None:
                # Log error record as well.
                err_record: Dict[str, Any] = {
                    "idx": task.idx,
                    "error": str(e),
                    "prompt": task.prompt,
                    "planned_ts_mono": task.ts_mono,
                    "send_failed": True,
                }
                logger.log_request(err_record)
    finally:
        session.close()


def run_open_loop_load(
    *,
    router_url: str,
    prompts: List[str],
    plan_times: List[float],
    gen_cfg: GenerationConfig,
    warmup_reqs: int = 0,
    logger: Optional[ExperimentLogger] = None,
    output_log_mode: str = "preview",
    print_trace: bool = True,
):
    """
    Execute a precomputed schedule using one thread per request.

    - router_url: base URL of the router (no /enqueue).
    - prompts: list of prompts.
    - plan_times: list of absolute monotonic timestamps (same length as prompts).
    - gen_cfg: generation config (max_tokens, temperature, etc.).
    - warmup_reqs: number of dummy warmup requests to send before the timed load.
    - logger: optional ExperimentLogger; if provided, per-request JSON records
      will be written into logs.json in the experiment directory.
    - output_log_mode: "preview" or "full" (controls what goes into logs.json).
    - print_trace: if False, do not print trace block or preview to stdout.

    Each per-request thread blocks on /enqueue until the router has received a
    result from the sidecar, so RECV logs imply the response was actually received.
    """
    if len(prompts) != len(plan_times):
        raise ValueError("prompts and plan_times length mismatch")

    total = len(prompts)
    if total == 0:
        print("[load_runner] No jobs to send.")
        return

    warmup_reqs = int(warmup_reqs or 0)
    if warmup_reqs > 0:
        print(f"[load_runner] warmup: sending {warmup_reqs} dummy requests before timed load")

        session = requests.Session()
        meta = {
            "max_tokens": int(gen_cfg.max_tokens),
            "temperature": float(gen_cfg.temperature),
            "length_mode": gen_cfg.length_mode,
            "enable_thinking": bool(gen_cfg.think),
        }
        if gen_cfg.target_output_tokens is not None:
            meta["target_output_tokens"] = int(gen_cfg.target_output_tokens)
        if gen_cfg.target_total_tokens is not None:
            meta["target_total_tokens"] = int(gen_cfg.target_total_tokens)

        warm_prompt = "WARMUP: dummy request to warm up vLLM workers, router, and caches."

        t_w0 = time.time()
        for i in range(warmup_reqs):
            try:
                rid, result = send_one(session, router_url, warm_prompt, meta)
                print(f"[client][warmup] i={i} req_id={rid}")
            except Exception as e:
                print(f"[client][warmup] ERROR i={i}: {e}")
        session.close()
        t_w1 = time.time()
        print(f"[load_runner] warmup done in {t_w1 - t_w0:.3f}s")

    print(f"[load_runner] Starting thread-per-request mode for {total} requests")

    if not plan_times:
        print("[load_runner] Empty schedule, nothing to send.")
        return

    # Re-anchor the schedule so that the first planned time starts now.
    t0_mono = time.monotonic()
    t0_plan = plan_times[0]
    adj_plan_times = [t0_mono + (ts - t0_plan) for ts in plan_times]

    threads: List[threading.Thread] = []
    t0_wall = time.time()

    for idx, (ts_mono, prompt) in enumerate(zip(adj_plan_times, prompts)):
        task = RequestTask(idx=idx, prompt=prompt, ts_mono=ts_mono)
        t = threading.Thread(
            target=_request_thread,
            args=(task, router_url, gen_cfg, t0_mono, logger, output_log_mode, print_trace),
            daemon=True,
        )
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    elapsed = time.time() - t0_wall
    print(f"[load_runner] Done. Sent {total} requests in {elapsed:.3f}s")
