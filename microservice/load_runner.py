# load_runner.py
# Per-request threaded load runner:
# - Optional request-based warmup before the main load.
# - For each (plan_time, prompt), spawn a thread.
# - Each thread sleeps until its scheduled monotonic timestamp, sends the request,
#   and waits for the synchronous response from /enqueue.

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Dict
import threading
import time

import requests

from http_client import send_one
from config import GenerationConfig


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
):
    """
    Per-request worker:
      - Sleep until scheduled timestamp
      - Log per-second send stats at the actual send time
      - Log SEND event
      - Build meta
      - Call send_one() with its own Session (blocking until response)
      - Log RECV event with timing and output preview
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

            if isinstance(result, dict):
                if isinstance(result.get("latency_s"), (int, float)):
                    latency_s = float(result["latency_s"])
                if isinstance(result.get("finish_reason"), str):
                    finish_reason = result["finish_reason"]
                if isinstance(result.get("output"), str):
                    output_preview = result["output"].replace("\n", " ")
                    if len(output_preview) > 120:
                        output_preview = output_preview[:117] + "..."

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

            if output_preview is not None:
                print(f"[client][T{task.idx}]   output_preview={output_preview!r}")

        except Exception as e:
            print(f"[client][RECV][T{task.idx}] ✗ ERROR idx={task.idx}: {e}")
    finally:
        session.close()


def run_open_loop_load(
    *,
    router_url: str,
    prompts: List[str],
    plan_times: List[float],
    gen_cfg: GenerationConfig,
    warmup_reqs: int = 0,
):
    """
    Execute a precomputed schedule using one thread per request.

    - router_url: base URL of the router (no /enqueue).
    - prompts: list of prompts.
    - plan_times: list of absolute monotonic timestamps (same length as prompts).
    - gen_cfg: generation config (max_tokens, temperature, etc.).
    - warmup_reqs: number of dummy warmup requests to send before the timed load.

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
            args=(task, router_url, gen_cfg, t0_mono),
            daemon=True,
        )
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    elapsed = time.time() - t0_wall
    print(f"[load_runner] Done. Sent {total} requests in {elapsed:.3f}s")
