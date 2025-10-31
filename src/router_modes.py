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
from typing import Type, Union

from kubernetes import client

from config import get_config
from utils_k8s import discover_endpoints
from utils_prom import (
    start_metrics_collection,
    update_metrics_endpoints,
    stop_metrics_collection,
)
from utils import save_summary

# Concrete router imports
from router_core import (
    PullBatchingRouter,
    RRBatchingRouter,
    RandomBatchingRouter,
    LeastQueueBatchingRouter,
)

# Union types for router class / instance
RouterInstance = Union[
    PullBatchingRouter,
    RRBatchingRouter,
    RandomBatchingRouter,
    LeastQueueBatchingRouter,
]

RouterClass = Union[
    Type[PullBatchingRouter],
    Type[RRBatchingRouter],
    Type[RandomBatchingRouter],
    Type[LeastQueueBatchingRouter],
]

# Optional: for clarity
PromptItem = Union[str, tuple[str, int]]
from autoscaler import QueueBacklogAutoscaler
from utils import log_autoscale


# Centralized config
_cfg = get_config()

# -------------------------
# Helpers (loadgen wiring)
# -------------------------


from itertools import count

_req_id_counter = count(start=0)

def _make_enqueue_fn_for_batched(router) -> callable:
    """
    enqueue_one(prompt_or_pair, t_enq_client)
    - prompt_or_pair: either a string prompt or (prompt, replay_out_len) from HF loader
    """
    from length_backend import register_replay_out_len  # local import to avoid cycles

    def enqueue_one(prompt_or_pair, t_enq_client: float):
        # Accept both "str" and "(prompt, out_len)" tuples
        if isinstance(prompt_or_pair, tuple) and len(prompt_or_pair) == 2:
            prompt, replay_out_len = prompt_or_pair
        else:
            prompt, replay_out_len = prompt_or_pair, None

        rid = next(_req_id_counter)
        try:
            if replay_out_len is not None:
                try:
                    register_replay_out_len(int(rid), int(replay_out_len))
                except Exception:
                    pass
            router.q.put((str(prompt), float(t_enq_client), int(rid)))
        except Exception:
            if replay_out_len is not None:
                try:
                    register_replay_out_len(int(rid), int(replay_out_len))
                except Exception:
                    pass
            router.q.put((str(prompt), time.time(), int(rid)))
    return enqueue_one

def _start_load_feeder_if_needed(pattern, prompts, enqueue_one) -> threading.Thread | None:
    from loadgen import drive_load
    pat = (pattern or "dump").lower()

    # For "dump", handle both deque and iterator
    if pat == "dump":
        t0 = time.time()
        if hasattr(prompts, "popleft"):
            while prompts:
                enqueue_one(prompts.popleft(), t0)
        else:
            for p in prompts:
                enqueue_one(p, t0)
        return None

    # Non-dump: just pass through; drive_load now supports iterators
    def _runner():
        drive_load(
            pattern=pat,
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
    prompts: deque[PromptItem],
    metrics_interval: float,
    mode_name: str,
    router_cls: RouterClass,
    log_prefix: str,
    summary_caption: str,
) -> None:
    """Shared driver for pull/push batching modes."""

    start_metrics_collection(mode_name, _cfg.METRICS_PATH, metrics_interval)
    start_time = time.time()

    # --- results path
    try:
        results_dir = getattr(_cfg, "RESULTS_PATH", None) or getattr(_cfg, "RESULTS_DIR", None)
        if results_dir:
            print(f"[RESULTS] Logs under: {results_dir}")
    except Exception:
        pass

    # --- router construction
    router: RouterInstance = router_cls(mode_name=mode_name)

    # --- load feeder setup
    feeder = _start_load_feeder_if_needed(
        _cfg.LOAD_PATTERN,
        prompts,
        _make_enqueue_fn_for_batched(router),
    )

    # --- initial endpoint discovery
    eps_all = discover_endpoints(core, _cfg.NAMESPACE, _cfg.LABEL_SELECTOR, _cfg.VLLM_PORT)
    if not eps_all:
        print("No running vLLM pods found. Exiting.")
        stop_metrics_collection(mode_name)
        return
    update_metrics_endpoints(mode_name, eps_all)

    # --- autoscaler setup
    scaler = None
    if getattr(_cfg, "AUTOSCALE_ENABLED", True):
        scaler = QueueBacklogAutoscaler(
            mode=str(getattr(_cfg, "AUTOSCALE_MODE", "virtual")),
            target_q_per_server=int(getattr(_cfg, "AUTOSCALE_Q_PER_SERVER", 10)),
            min_servers=int(getattr(_cfg, "AUTOSCALE_MIN_SERVERS", 1)),
            max_servers=int(getattr(_cfg, "AUTOSCALE_MAX_SERVERS", 10_000)),
            hysteresis=float(getattr(_cfg, "AUTOSCALE_HYSTERESIS", 0.20)),
            debounce_s=float(getattr(_cfg, "AUTOSCALE_DEBOUNCE_S", 1.0)),
            router_mode_name=mode_name,
        )
    else:
        print("[AUTOSCALE] Disabled in config.")
        log_autoscale(
            router_mode=mode_name,
            desired_servers=len(eps_all),
            realized_servers=len(eps_all),
            total_eps=len(eps_all),
            active_eps=len(eps_all),
            draining_eps=0,
            queue_len=0,
            inflight=0,
            reason="autoscale-disabled",
        )
        scaler = None

    router.ensure_endpoints(eps_all)
    last_discovery = 0.0
    last_logged_sig: tuple | None = None  # (desired_servers, sorted(active), sorted(draining))

    try:
        while router.has_work() or (feeder.is_alive() if feeder else False):
            now = time.time()

            # Periodic discovery refresh
            if now - last_discovery > _cfg.DISCOVERY_INTERVAL_S:
                eps_all = discover_endpoints(core, _cfg.NAMESPACE, _cfg.LABEL_SELECTOR, _cfg.VLLM_PORT)
                update_metrics_endpoints(mode_name, eps_all)
                last_discovery = now
                if not eps_all:
                    print("No vLLM pods found; waiting...")
                    time.sleep(1.0)
                    continue

            # --- inflight summary
            inflight_sum = 0
            inflight_by_ep = {}
            try:
                if hasattr(router, "inflight") and isinstance(router.inflight, dict):
                    inflight_by_ep = {str(k): int(v) for k, v in router.inflight.items()}
                    inflight_sum = sum(inflight_by_ep.values())
            except Exception:
                pass

            # --- autoscale decision
            if scaler:
                active_eps, draining_eps, desired_servers, reason, changed = scaler.step_and_select(
                    eps_all,
                    queue_len=router.q.qsize(),
                    inflight_by_ep=inflight_by_ep,
                )
            else:
                active_eps = list(eps_all)
                draining_eps = []
                desired_servers = len(active_eps)
                reason, changed = "autoscale-disabled", False

            realized_servers = len(active_eps) + len(draining_eps)

            # --- router endpoint updates
            if hasattr(router, "set_draining_eps"):
                try:
                    router.set_draining_eps(set(draining_eps))
                except Exception:
                    pass
            router.ensure_endpoints(list(active_eps) + list(draining_eps))

            # --- structured autoscale log
            sig = (desired_servers, tuple(sorted(active_eps)), tuple(sorted(draining_eps)))
            if changed or sig != last_logged_sig:
                log_autoscale(
                    router_mode=mode_name,
                    desired_servers=desired_servers,
                    realized_servers=realized_servers,
                    total_eps=len(eps_all),
                    active_eps=len(active_eps),
                    draining_eps=len(draining_eps),
                    queue_len=router.q.qsize(),
                    inflight=inflight_sum,
                    reason=reason,
                )
                last_logged_sig = sig

            # --- router scheduling tick
            router.step()
            print(
                f"[{log_prefix}] desired={desired_servers} realized={realized_servers} "
                f"active={len(active_eps)} draining={len(draining_eps)} total={len(eps_all)} "
                f"q={router.q.qsize()} inflight={inflight_sum} reason={reason} | "
                f"{router.status_line()}"
            )
            time.sleep(_cfg.SAMPLE_INTERVAL)
    finally:
        pass

    # --- summary
    runtime = time.time() - start_time
    print(f"\n==== SUMMARY ({summary_caption}) ====")
    print(f"Total runtime: {runtime:.2f}s")

    stats = router.stats()
    for ep, s in sorted(stats.items()):
        print(f"{ep}: ok={s['ok']} err={s['err']}")

    stop_metrics_collection(mode_name)
    save_summary(
        mode_name,
        {
            "runtime_sec": runtime,
            "endpoints": stats,
            "total_ok": sum(v["ok"] for v in stats.values()),
            "total_err": sum(v["err"] for v in stats.values()),
        },
    )


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
