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
from utils import log_autoscale, log_load

# Concrete router imports
from router_core import (
    PullBatchingRouter,
    RRBatchingRouter,
    RandomBatchingRouter,
    LeastQueueBatchingRouter,
)

# KV-aware
from kv_aware import notify_arrival
from kv_watcher import KVWatcher

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

# Centralized config
_cfg = get_config()

# -------------------------
# Helpers (loadgen wiring)

# Load flow (detailed, with file origins):
#
#  ┌──────────────────────────────────────────────────────────────────────┐
#  │ loadgen (from: loadgen.py)                                          │
#  │  - drive_load() produces prompts (dump/poisson/det/bursty/steps)    │
#  │  - enqueue_one() (defined in router_modes.py) assigns req_id,       │
#  │    t_enq_client, and optional replay_out_len                        │
#  └─────────────┬────────────────────────────────────────────────────────┘
#                │ (prompt, t_enq_client, req_id)
#                ▼
#  ┌──────────────────────────────┐
#  │ router.q (Queue object)      │  ← shared pending requests
#  │ from: router_core.py         │
#  └─────────────┬────────────────┘
#                │ consumed by router.step()
#                ▼
#  ┌──────────────────────────────────────────────────────────────────────┐
#  │ router (from: router_core.py)                                       │
#  │  - PullBatchingRouter / RR / Random / LeastQueue                    │
#  │  1) Peek N items under _Q_LOCK                                      │
#  │  2) Use len_select.select_batch() (len_select.py)                   │
#  │  3) Compute 'want' via GPU util + admission mode                    │
#  │  4) Send batches, record timestamps (t_arrival/dispatch/response)   │
#  │  5) Handle draining endpoints, errors, requeues                     │
#  └─────────────┬────────────────────────────────────────────────────────┘
#                │ batched HTTP calls (via requests + HTTPAdapter)
#                ▼
#  ┌──────────────────────────────────────────────────────────────────────┐
#  │ endpoints (vLLM pods discovered via utils_k8s.py)                   │
#  │  - Serve requests, produce completions                              │
#  │  - Router logs ok/err, updates Prometheus via utils_prom.py         │
#  │  - Metrics: queue_wait, roundtrip, end_to_end                       │
#  └──────────────────────────────────────────────────────────────────────┘


# -------------------------

from itertools import count
_req_id_counter = count(start=0)


def _make_enqueue_fn_for_batched(router) -> callable:
    """
    enqueue_one(prompt_or_pair, t_enq_client)
    - prompt_or_pair: either a string prompt or (prompt, replay_out_len) from HF loader
    """
    from length_backend import register_replay_out_len

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
            try:
                notify_arrival(1)
            except Exception:
                pass
        except Exception:
            if replay_out_len is not None:
                try:
                    register_replay_out_len(int(rid), int(replay_out_len))
                except Exception:
                    pass
            router.q.put((str(prompt), time.time(), int(rid)))
            try:
                notify_arrival(1)
            except Exception:
                pass

    return enqueue_one


def _start_load_feeder_if_needed(pattern, prompts, enqueue_one, *, router_mode: str) -> threading.Thread | None:
    """
    Starts the load feeder if needed.
    - For "dump": enqueue here (and log start/arrival/done).
    - For others: spawn a daemon thread that calls drive_load(...) with logging enabled.
    """
    from loadgen import drive_load
    pat = (pattern or "dump").lower()

    # For "dump", handle both deque and iterator (and log)
    if pat == "dump":
        t0 = time.time()
        try:
            log_load(router_mode=router_mode, event="start", pattern="dump", rps=None, extra={"effective_start_ts": t0})
        except Exception:
            pass

        sent = 0
        trace_idx = 0
        log_every = int(getattr(_cfg, "LOAD_LOG_EVERY", 1))

        if hasattr(prompts, "popleft"):
            while prompts:
                enqueue_one(prompts.popleft(), t0)
                sent += 1
                if log_every > 0 and (sent % log_every) == 0:
                    trace_idx += 1
                    try:
                        log_load(
                            router_mode=router_mode,
                            event="arrival",
                            pattern="dump",
                            phase="dump",
                            idx=trace_idx,
                            rps_effective=None,
                            planned_at=t0,
                            woke_at=time.time(),
                            enq_at=t0,
                        )
                    except Exception:
                        pass
        else:
            for p in prompts:
                enqueue_one(p, t0)
                sent += 1
                if log_every > 0 and (sent % log_every) == 0:
                    trace_idx += 1
                    try:
                        log_load(
                            router_mode=router_mode,
                            event="arrival",
                            pattern="dump",
                            phase="dump",
                            idx=trace_idx,
                            rps_effective=None,
                            planned_at=t0,
                            woke_at=time.time(),
                            enq_at=t0,
                        )
                    except Exception:
                        pass

        try:
            log_load(
                router_mode=router_mode,
                event="done",
                pattern="dump",
                extra={"sent": sent, "elapsed_s": max(0.0, time.time() - t0)},
            )
        except Exception:
            pass
        return None

    # Non-dump: just pass through; drive_load supports iterators and logging
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
            # random-range pattern params
            rand_rps_min=getattr(_cfg, "RAND_RPS_MIN", None),
            rand_rps_max=getattr(_cfg, "RAND_RPS_MAX", None),
            rand_epoch_s=float(getattr(_cfg, "RAND_EPOCH_S", 5.0)),
            rand_kind=str(getattr(_cfg, "RAND_KIND", "poisson")),
            # logging controls
            router_mode=router_mode,
            log_every=int(getattr(_cfg, "LOAD_LOG_EVERY", 1)),
            verbose=bool(getattr(_cfg, "VERBOSE_LOAD", True)),
        )
    th = threading.Thread(target=_runner, daemon=True)
    th.start()
    return th


# -------------------------
# Main Experiment Loop
# -------------------------

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

    try:
        results_dir = getattr(_cfg, "RESULTS_PATH", None) or getattr(_cfg, "RESULTS_DIR", None)
        if results_dir:
            print(f"[RESULTS] Logs under: {results_dir}")
    except Exception:
        pass

    router: RouterInstance = router_cls(mode_name=mode_name)

    # KV watcher
    kv_watcher = None
    try:
        kv_watcher = KVWatcher(
            # model_name=_cfg.MODEL_NAME,
            # redis_host=_cfg.REDIS_HOST,
            # redis_port=_cfg.REDIS_PORT,
            # namespace=_cfg.NAMESPACE,
            # label_selector=_cfg.LABEL_SELECTOR,
            # service_port=_cfg.VLLM_PORT,
            # interval_s=float(_cfg.KV_WATCH_INTERVAL_S),
            # max_keys=int(_cfg.KV_WATCH_MAX_KEYS),
        )
        kv_watcher.start()
        print("[KV-WATCHER] started.")
    except Exception as e:
        print(f"[KV-WATCHER] FAILED TO START: {e}")
        kv_watcher = None

    feeder = _start_load_feeder_if_needed(
        _cfg.LOAD_PATTERN,
        prompts,
        _make_enqueue_fn_for_batched(router),
        router_mode=mode_name,
    )

    eps_all = discover_endpoints(core, _cfg.NAMESPACE, _cfg.LABEL_SELECTOR, _cfg.VLLM_PORT)
    if not eps_all:
        print("No running vLLM pods found. Exiting.")
        stop_metrics_collection(mode_name)
        if kv_watcher:
            try:
                kv_watcher.stop()
            except Exception:
                pass
        return
    update_metrics_endpoints(mode_name, eps_all)

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
    last_logged_sig: tuple | None = None

    try:
        while router.has_work() or (feeder.is_alive() if feeder else False):
            now = time.time()

            if now - last_discovery > _cfg.DISCOVERY_INTERVAL_S:
                eps_all = discover_endpoints(core, _cfg.NAMESPACE, _cfg.LABEL_SELECTOR, _cfg.VLLM_PORT)
                update_metrics_endpoints(mode_name, eps_all)
                last_discovery = now
                if not eps_all:
                    print("No vLLM pods found; waiting...")
                    time.sleep(1.0)
                    continue

            inflight_sum = 0
            inflight_by_ep = {}
            try:
                if hasattr(router, "inflight") and isinstance(router.inflight, dict):
                    inflight_by_ep = {str(k): int(v) for k, v in router.inflight.items()}
                    inflight_sum = sum(inflight_by_ep.values())
            except Exception:
                pass

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

            if hasattr(router, "set_draining_eps"):
                try:
                    router.set_draining_eps(set(draining_eps))
                except Exception:
                    pass
            router.ensure_endpoints(list(active_eps) + list(draining_eps))

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

            router.step()
            print(
                f"[{log_prefix}] desired={desired_servers} realized={realized_servers} "
                f"active={len(active_eps)} draining={len(draining_eps)} total={len(eps_all)} "
                f"q={router.q.qsize()} inflight={inflight_sum} reason={reason} | "
                f"{router.status_line()}"
            )
            time.sleep(_cfg.SAMPLE_INTERVAL)
    finally:
        if kv_watcher:
            try:
                kv_watcher.stop()
                print("[KV-WATCHER] stopped.")
            except Exception:
                pass

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
