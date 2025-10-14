# -*- coding: utf-8 -*-
"""
Router modes (batching-only):
- pull-batching
- rr-batching
- random-batching
- least-queue-batching
"""

import time
import threading
from collections import deque
from typing import List

from kubernetes import client

from config import get_config
from utils_k8s import discover_endpoints
from utils_prom import (
    start_metrics_collection,
    update_metrics_endpoints,
    stop_metrics_collection,
)
from utils import save_summary

from router_core import (
    PullBatchingRouter,
    RRBatchingRouter,
    RandomBatchingRouter,
    LeastQueueBatchingRouter,
)

# Centralized config
_cfg = get_config()

# -------------------------
# Helpers (loadgen wiring)
# -------------------------


from itertools import count

_req_id_counter = count(start=0)

# def _make_enqueue_fn_for_batched(router) -> callable:
#     """Return enqueue(prompt, t_enq) for routers with shared queue."""
#     def enqueue_one(prompt: str, t_enq_client: float):
#         rid = next(_req_id_counter)
#         try:
#             router.q.put((prompt, t_enq_client, rid))
#         except Exception:
#             router.q.put((prompt, time.time(), rid))
#     return enqueue_one
def _make_enqueue_fn_for_batched(router) -> callable:
    def enqueue_one(prompt: str, t_enq_client: float):
        rid = next(_req_id_counter)
        try:
            router.q.put((prompt, t_enq_client, rid))
        except Exception:
            router.q.put((prompt, time.time(), rid))
    return enqueue_one

def _start_load_feeder_if_needed(
    pattern: str,
    prompts: deque,
    enqueue_one: callable,
) -> threading.Thread | None:
    """
    Start a timed feeder thread for non-dump patterns.
    Returns the Thread (or None if dump/immediate).
    """
    from loadgen import drive_load  # local import

    if (pattern or "dump").lower() == "dump":
        t0 = time.time()
        while prompts:
            p = prompts.popleft()
            enqueue_one(p, t0)
        return None

    def _runner():
        drive_load(
            pattern=pattern,
            prompts=prompts,
            enqueue_one=enqueue_one,
            rate_rps=_cfg.LOAD_RATE_RPS,
            warmup_s=_cfg.LOAD_WARMUP_S,
            duration_s=_cfg.LOAD_DURATION_S,
            burst_on_s=_cfg.BURST_ON_S,
            burst_off_s=_cfg.BURST_OFF_S,
            burst_rps_on=_cfg.BURST_RPS_ON,
            burst_rps_off=_cfg.BURST_RPS_OFF,
            step_schedule=_cfg.STEP_SCHEDULE,
        )

    th = threading.Thread(target=_runner, daemon=True)
    th.start()
    return th

def _run_batched_common(
    core: client.CoreV1Api,
    prompts: deque,
    metrics_interval: float,
    mode_name: str,
    router_cls,
    log_prefix: str,
    summary_caption: str,
):
    # --- metrics start
    start_metrics_collection(mode_name, _cfg.METRICS_PATH, metrics_interval)
    start_time = time.time()

    # --- optional: print where logs will go if config exposes it
    try:
        results_dir = getattr(_cfg, "RESULTS_PATH", None) or getattr(
            _cfg, "RESULTS_DIR", None
        )
        if results_dir:
            print(f"[RESULTS] Per-request logs are written under: {results_dir}")
    except Exception:
        pass

    # --- router
    router = router_cls(mode_name=mode_name)

    # --- STRICT HIST plan (precompute for exactly N enqueues)
    try:
        from length_backend import set_hist_plan
        strict = bool(getattr(_cfg, "LENGTH_DIST_STRICT_HIST", False))
        if strict:
            # Number of enqueues this run (after PROMPTS_LIMIT already applied)
            N = len(prompts)
            d = dict(getattr(_cfg, "SIM_OUT_DIST", {}) or {})
            values = list(map(int, d.get("values", [])))
            probs = d.get("probs") or ([1.0 / max(1, len(values))] * len(values))
            label = str(getattr(_cfg, "LENGTH_HIST_SERIES_LABEL", "default"))
            seed_base = int(getattr(_cfg, "LENGTH_DIST_SEED", None) or getattr(_cfg, "SEED", 0) or 0)

            if values:
                set_hist_plan(label, probs, int(N), seed_base)
                print(f"[LENGTH*PLAN] strict-hist plan prepared: label={label} N={N} values={len(values)}")
            else:
                print("[LENGTH*PLAN][WARN] SIM_OUT_DIST.values is empty; strict-hist plan skipped.")
    except Exception as e:
        print(f"[LENGTH*PLAN][WARN] failed to prepare strict-hist plan: {e}")

    # --- load feeder (assigns req_id in _make_enqueue_fn_for_batched)
    feeder = _start_load_feeder_if_needed(
        _cfg.LOAD_PATTERN,
        prompts,
        _make_enqueue_fn_for_batched(router),
    )

    # --- initial discovery
    eps = discover_endpoints(core, _cfg.NAMESPACE, _cfg.LABEL_SELECTOR, _cfg.VLLM_PORT)
    if not eps:
        print("No running vLLM pods found. Exiting.")
        stop_metrics_collection(mode_name)
        return
    update_metrics_endpoints(mode_name, eps)
    router.ensure_endpoints(eps)

    last_discovery = 0.0
    try:
        while router.has_work() or (feeder.is_alive() if feeder else False):
            now = time.time()
            if now - last_discovery > _cfg.DISCOVERY_INTERVAL_S:
                new_eps = discover_endpoints(
                    core, _cfg.NAMESPACE, _cfg.LABEL_SELECTOR, _cfg.VLLM_PORT
                )
                update_metrics_endpoints(mode_name, new_eps)
                router.ensure_endpoints(new_eps)
                last_discovery = now
                if not new_eps:
                    print("No running vLLM pods found yet. Waiting...")
                    time.sleep(1.0)
                    continue

            router.step()
            print(f"[{log_prefix}]", router.status_line())
            time.sleep(_cfg.SAMPLE_INTERVAL)
    finally:
        # daemon worker threads are tracked via inflight; nothing explicit to stop here
        pass

    # --- summary (console)
    print(f"\n==== SUMMARY ({summary_caption}) ====")
    runtime = time.time() - start_time
    print(f"Total runtime: {runtime:.2f} seconds")
    stats = router.stats()
    for ep in sorted(stats.keys()):
        s = stats[ep]
        print(f"{ep}: ok={s['ok']} err={s['err']}")

    # --- metrics stop
    stop_metrics_collection(mode_name)

    # --- persisted run summary
    try:
        summary_path = save_summary(
            mode_name,
            {
                "runtime_sec": runtime,
                "endpoints": stats,
                "total_ok": sum(v["ok"] for v in stats.values()),
                "total_err": sum(v["err"] for v in stats.values()),
            },
        )
        if summary_path:
            print(f"[SUMMARY] Saved summary to: {summary_path}")
        else:
            print("[SUMMARY] Summary saved (path not returned by save_summary).")
    except Exception as e:
        print(f"[SUMMARY][WARN] Failed to save summary: {e}")



def run_pull_batching(core: client.CoreV1Api, prompts: deque, metrics_interval: float):
    _run_batched_common(
        core,
        prompts,
        metrics_interval,
        mode_name="pull-batching",
        router_cls=PullBatchingRouter,
        log_prefix="PULL-BATCH",
        summary_caption="PULL-BATCHING",
    )


def run_rr_batching(core: client.CoreV1Api, prompts: deque, metrics_interval: float):
    _run_batched_common(
        core,
        prompts,
        metrics_interval,
        mode_name="rr-batching",
        router_cls=RRBatchingRouter,
        log_prefix="RR-BATCH",
        summary_caption="RR-BATCHING",
    )


def run_random_batching(
    core: client.CoreV1Api, prompts: deque, metrics_interval: float
):
    _run_batched_common(
        core,
        prompts,
        metrics_interval,
        mode_name="random-batching",
        router_cls=RandomBatchingRouter,
        log_prefix="RANDOM-BATCH",
        summary_caption="RANDOM-BATCHING",
    )


def run_least_queue_batching(
    core: client.CoreV1Api, prompts: deque, metrics_interval: float
):
    _run_batched_common(
        core,
        prompts,
        metrics_interval,
        mode_name="least-queue-batching",
        router_cls=LeastQueueBatchingRouter,
        log_prefix="LQ-BATCH",
        summary_caption="LEAST-QUEUE-BATCHING",
    )
