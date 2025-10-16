# -*- coding: utf-8 -*-
"""
Core components for vLLM router (batching-only):
- PullBatchingRouter (true pull)
- _BaseBatchingRouter (push) + RRBatchingRouter / RandomBatchingRouter / LeastQueueBatchingRouter

Logs include (single source of truth):
- timestamps: t_arrival_router, t_dispatch_router, t_response_router
- latencies:  router_queue_wait, server_roundtrip, end_to_end_latency

Identity:
  end_to_end_latency = router_queue_wait + server_roundtrip
"""

import time
import threading
import random
from queue import Queue, Empty
from typing import List, Dict, Tuple, Optional
import requests

from config import get_config
from utils import log_result, log_queue, healthy, send_chat_request
from utils_prom import probe_gpu_util

# NEW: predictor hook
from predictors import get_length_predictor

# NEW: shared length-aware selection hook
from threading import RLock
from len_select import select_batch

_cfg = get_config()
_pred = get_length_predictor()  # singleton predictor based on cfg.PREDICTOR_NAME

# ---- Length-aware config knobs (shared by both pull & push) ----
_USE_LEN_AWARE: bool = bool(getattr(_cfg, "USE_LEN_AWARE", False))
_LEN_POLICY: str = str(getattr(_cfg, "LEN_POLICY", "short_first") or "short_first")
# _PRED_LEN_THRESHOLD: int = int(getattr(_cfg, "PRED_LEN_THRESHOLD", 512) or 512)
_POOL_FACTOR: int = int(getattr(_cfg, "POOL_FACTOR", 3) or 3)
_DEFAULT_MAX_TOKENS: int = int(getattr(_cfg, "MAX_TOKENS", 256) or 256)

# One global lock for queue peeking/return across threads
_Q_LOCK = RLock()


# =========================
# Pull-batching (true pull)
# =========================
class PullBatchingRouter:
    """
    Adaptive pull-batching router (TRUE pull):
      - One long-lived worker per endpoint
      - Event-driven refill: wake immediately when capacity frees
      - Periodic safety tick paced by _cfg.SAMPLE_INTERVAL
      - Util probing cached with TTL to reduce /metrics pressure
      - Queue logging throttled to 0, N, 2N, ... sessions per endpoint
    """

    def __init__(self, mode_name: str = "pull-batching"):
        self.mode_name = mode_name
        self.q: "Queue[Tuple[str, float]]" = Queue()
        self.eps: List[str] = []
        self.inflight: Dict[str, int] = {}
        self.ok_counts: Dict[str, int] = {}
        self.err_counts: Dict[str, int] = {}

        self._lock = threading.Lock()
        self._workers: Dict[str, threading.Thread] = {}
        self._stop_flags: Dict[str, threading.Event] = {}
        self._log_counters: Dict[str, int] = {}  # per-EP session index

        self._refill_evts: Dict[str, threading.Event] = {}
        self._safety_interval_s: float = float(getattr(_cfg, "SAMPLE_INTERVAL", 0.1))
        self._util_ttl_s: float = float(getattr(_cfg, "UTIL_TTL_S", 0.15))
        self._util_cache: Dict[str, Tuple[float, Optional[float]]] = {}

        self._id_cache: Dict[str, Tuple[float, str, Optional[float]]] = {}
        self._id_ttl_s: float = float(getattr(_cfg, "TPS_TTL_S", 5.0))


    def ensure_endpoints(self, endpoints: List[str]):
        for ep in endpoints:
            if ep not in self.inflight:
                self.inflight[ep] = 0
                self.ok_counts[ep] = 0
                self.err_counts[ep] = 0
                self._log_counters[ep] = 0
            if ep not in self._refill_evts:
                self._refill_evts[ep] = threading.Event()
            if ep not in self._workers:
                stop_evt = threading.Event()
                self._stop_flags[ep] = stop_evt
                t = threading.Thread(
                    target=self._worker_loop, args=(ep, stop_evt), daemon=True
                )
                self._workers[ep] = t
                t.start()

        for ep in list(self._workers.keys()):
            if ep not in endpoints:
                self._stop_flags[ep].set()
                self._workers[ep].join(timeout=2)
                self._workers.pop(ep, None)
                self._stop_flags.pop(ep, None)
                with self._lock:
                    self.inflight.pop(ep, None)
                    self.ok_counts.pop(ep, None)
                    self.err_counts.pop(ep, None)
                    self._log_counters.pop(ep, None)
                self._refill_evts.pop(ep, None)
                self._util_cache.pop(ep, None)
                self._id_cache.pop(ep, None)

        self.eps = list(endpoints)

    def _cached_util(self, ep: str) -> Optional[float]:
        now = time.time()
        ts, val = self._util_cache.get(ep, (0.0, None))
        if now - ts < self._util_ttl_s:
            return val
        val = probe_gpu_util(ep, _cfg.METRICS_PATH)
        self._util_cache[ep] = (now, val)
        return val

    def _cached_identity(self, ep: str) -> Tuple[str, Optional[float]]:
        now = time.time()
        ts, nm, tps = self._id_cache.get(ep, (0.0, None, None))
        if now - ts < self._id_ttl_s and nm is not None:
            return nm, tps
        try:
            r = requests.get(ep.rstrip("/") + _cfg.HEALTH_PATH, timeout=0.5)
            if r.ok:
                data = r.json() or {}
                nm = data.get("name") or ep
                tps = float(data.get("tps")) if data.get("tps") is not None else None
            else:
                nm, tps = ep, None
        except Exception:
            nm, tps = ep, None
        self._id_cache[ep] = (now, nm, tps)
        return nm, tps

    def _compute_want(self, ep: str, util_pct: Optional[float]) -> int:
        with self._lock:
            current = self.inflight.get(ep, 0)
        cap = max(0, int(_cfg.MAX_INFLIGHT_PER_EP) - current)
        if cap <= 0:
            return 0
        if util_pct is None:
            return min(int(_cfg.NO_UTIL_BURST), cap)
        headroom = max(0.0, (float(_cfg.UTIL_THRESHOLD) - (util_pct / 100.0))) / max(
            float(_cfg.UTIL_THRESHOLD), 1e-6
        )
        return min(max(1, int(float(_cfg.BURST) * (1.0 + headroom))), cap)

    def _launch_one(self, ep: str, prompt: str, t_enq_client: float, req_id: int):
        def _runner():
            try:
                predicted_out_tokens = _pred.predict_out_tokens(prompt, req_id=req_id)

                # --- SSOT timestamps (client/router) ---
                t_arrival_router = float(t_enq_client or time.time())
                t_dispatch_router = time.time()

                resp = send_chat_request(
                    endpoint=ep,
                    model=_cfg.MODEL_NAME,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=_cfg.MAX_TOKENS,
                    temperature=_cfg.TEMPERATURE,
                    request_timeout_s=_cfg.REQUEST_TIMEOUT_S,
                    req_id=req_id,
                )

                t_response_router = time.time()

                # --- Derived latencies (measured) ---
                router_queue_wait = t_dispatch_router - t_arrival_router
                server_roundtrip = t_response_router - t_dispatch_router
                end_to_end_latency = t_response_router - t_arrival_router

                # --- Override with simulated (server-provided) times when present ---
                sim_total   = resp.get("_sim_total_wall_s")
                sim_queue   = resp.get("_sim_queue_wait_s")
                sim_service = resp.get("_sim_service_s") or resp.get("_sim_latency_s")
                used_sim_times = False
                if sim_total is not None:
                    end_to_end_latency = float(sim_total); used_sim_times = True
                if sim_queue is not None:
                    router_queue_wait = float(sim_queue); used_sim_times = True
                if sim_service is not None:
                    server_roundtrip = float(sim_service); used_sim_times = True

                choice = (resp.get("choices") or [{}])[0]
                content = (choice.get("message") or {}).get("content", "")

                actual_out = None
                try:
                    usage = resp.get("usage") or {}
                    actual_out = usage.get("completion_tokens")
                except Exception:
                    pass

                log_result(
                    mode=self.mode_name,
                    endpoint=ep,
                    model=_cfg.MODEL_NAME,
                    prompt=prompt,
                    status="ok",
                    response_preview=content,
                    latency_s=end_to_end_latency,
                    extra={
                        "req_id": int(req_id),
                        "t_arrival_router": t_arrival_router,
                        "t_dispatch_router": t_dispatch_router,
                        "t_response_router": t_response_router,
                        "router_queue_wait": router_queue_wait,
                        "server_roundtrip": server_roundtrip,
                        "end_to_end_latency": end_to_end_latency,
                        "latency_source": "sim" if used_sim_times else "measured",
                        "measured_router_queue_wait": (t_dispatch_router - t_arrival_router),
                        "measured_server_roundtrip": (t_response_router - t_dispatch_router),
                        "measured_end_to_end_latency": (t_response_router - t_arrival_router),
                        "predictor_name": getattr(_pred, "name", "unknown"),
                        "predicted_out_tokens": None if predicted_out_tokens is None else int(predicted_out_tokens),
                        "actual_out_tokens": None if actual_out is None else int(actual_out),
                    },
                )
                with self._lock:
                    self.ok_counts[ep] = self.ok_counts.get(ep, 0) + 1
            except Exception as e:
                log_result(
                    mode=self.mode_name,
                    endpoint=ep,
                    model=_cfg.MODEL_NAME,
                    prompt=prompt,
                    status="error",
                    error=str(e),
                    extra={
                        "req_id": int(req_id),
                        "predictor_name": getattr(_pred, "name", "unknown"),
                    },
                )
                with self._lock:
                    self.err_counts[ep] = self.err_counts.get(ep, 0) + 1
                try:
                    self.q.put((prompt, time.time(), req_id))
                except Exception:
                    pass
            finally:
                with self._lock:
                    self.inflight[ep] = max(0, self.inflight.get(ep, 0) - 1)
                evt = self._refill_evts.get(ep)
                if evt:
                    evt.set()
                try:
                    self.q.task_done()
                except Exception:
                    pass

        threading.Thread(target=_runner, daemon=True).start()


    def _worker_loop(self, ep: str, stop_evt: threading.Event):
        time.sleep(random.uniform(0.0, 0.02))

        evt = self._refill_evts.get(ep)
        if evt is None:
            evt = threading.Event()
            self._refill_evts[ep] = evt

        while not stop_evt.is_set():
            signaled = evt.wait(timeout=self._safety_interval_s)
            if signaled:
                evt.clear()

            if not healthy(ep, _cfg.HEALTH_PATH):
                continue

            with self._lock:
                inflight_snapshot = self.inflight.get(ep, 0)
            cap = max(0, int(_cfg.MAX_INFLIGHT_PER_EP) - inflight_snapshot)
            if cap <= 0:
                continue

            util_pct = self._cached_util(ep)
            want = self._compute_want(ep, util_pct)
            if want <= 0:
                continue

            q_before = self.q.qsize()

            # ---- Length-aware selection ----
            if _USE_LEN_AWARE:
                selected = select_batch(
                    self.q, want, _pred, _Q_LOCK,
                    policy=_LEN_POLICY,
                    pool_factor=_POOL_FACTOR,
                    default_max_tokens=_DEFAULT_MAX_TOKENS,
                )
            else:
                selected = []
                for _ in range(want):
                    try:
                        prompt, t_enq_client, req_id = self.q.get_nowait()
                        selected.append((prompt, t_enq_client, None, req_id))
                    except Empty:
                        break

            pulled_n = len(selected)
            q_after = self.q.qsize()

            with self._lock:
                before_now = self.inflight.get(ep, 0)
                self.inflight[ep] = before_now + pulled_n
                after_now = self.inflight[ep]
                cur_idx = self._log_counters.get(ep, 0)
                every_n = max(1, int(_cfg.QUEUE_LOG_EVERY_N))
                do_log = cur_idx % every_n == 0
                self._log_counters[ep] = cur_idx + 1

            if do_log:
                util_str = "NONE" if util_pct is None else f"{util_pct:.1f}%"
                name, _ = self._cached_identity(ep)
                log_extra = {
                    "session_idx": int(cur_idx),
                    "util_pct": None if util_pct is None else float(util_pct),
                    "want": int(want),
                    "pulled": int(pulled_n),
                    "inflight_before": int(before_now),
                    "inflight_after": int(after_now),
                    "q_before": int(q_before),
                    "q_after": int(q_after),
                    "every_n": every_n,
                    "len_aware": bool(_USE_LEN_AWARE),
                    "len_policy": str(_LEN_POLICY),
                    # "len_threshold": int(_PRED_LEN_THRESHOLD),
                    "pool_factor": int(_POOL_FACTOR)
                }
                print(
                    f"[PULL*BATCH] {name} (ep={ep}) util={util_str} want={want} pulled={pulled_n} "
                    f"inflight={before_now}->{after_now} q={q_before}->{q_after} lenAware={_USE_LEN_AWARE} pol={_LEN_POLICY}"
                )
                log_queue(router_mode=self.mode_name, endpoint=ep, event="pull-batch", extra=log_extra)

            if pulled_n == 0:
                continue

            for prompt, t_enq_client, _pred_tok, req_id in selected:
                self._launch_one(ep, prompt, t_enq_client, req_id)

    def step(self):
        return

    def has_work(self) -> bool:
        with self._lock:
            active = any(v > 0 for v in self.inflight.values())
        return (not self.q.empty()) or active

    def status_line(self) -> str:
        with self._lock:
            parts = []
            for ep in self.eps:
                name, _ = self._cached_identity(ep)
                parts.append(
                    f"{name} ({ep}) inflight={self.inflight.get(ep,0)} "
                    f"ok={self.ok_counts.get(ep,0)} err={self.err_counts.get(ep,0)}"
                )
            return " | ".join(parts) + f" | queue={self.q.qsize()}"

    def stats(self) -> Dict[str, Dict[str, int]]:
        with self._lock:
            out: Dict[str, Dict[str, int]] = {}
            for ep in sorted(self.ok_counts.keys()):
                name, _ = self._cached_identity(ep)
                key = f"{name} ({ep})"
                out[key] = {
                    "ok": self.ok_counts.get(ep, 0),
                    "err": self.err_counts.get(ep, 0),
                }
            return out


# =========================
# Generic Batching Base + Variants (push)
# =========================
class _BaseBatchingRouter:
    """
    Shared batching primitives for push-side schedulers:
      - shared input Queue
      - per-endpoint inflight / ok / err maps
      - endpoint churn handling
      - util-aware 'want' computation (default) + admission modes
      - thread launcher to send requests
    Subclasses must implement: `_ordered_eps_for_step()`
    """

    def __init__(self, mode_name: str):
        self.mode_name = mode_name
        self.q: "Queue[Tuple[str, float]]" = Queue()
        self.eps: List[str] = []
        self.inflight: Dict[str, int] = {}
        self.ok_counts: Dict[str, int] = {}
        self.err_counts: Dict[str, int] = {}
        self._lock = threading.Lock()
        self._log_counters: Dict[str, int] = {}

        self._step_interval_s: float = float(getattr(_cfg, "SAMPLE_INTERVAL", 0.1))
        self._last_step_ts: float = 0.0

        self._util_cache: Dict[str, Tuple[float, Optional[float]]] = {}
        self._util_ttl_s: float = float(getattr(_cfg, "UTIL_TTL_S", 0.15))

        self._id_cache: Dict[str, Tuple[float, str, Optional[float]]] = {}
        self._id_ttl_s: float = float(getattr(_cfg, "TPS_TTL_S", 5.0))

        self._admission_mode: str = str(getattr(_cfg, "ADMISSION_MODE", "util")).lower()


    def ensure_endpoints(self, endpoints: List[str]):
        for ep in endpoints:
            if ep not in self.inflight:
                self.inflight[ep] = 0
                self.ok_counts[ep] = 0
                self.err_counts[ep] = 0
                self._log_counters[ep] = 0
        for ep in list(self.inflight.keys()):
            if ep not in endpoints:
                self.inflight.pop(ep, None)
                self.ok_counts.pop(ep, None)
                self.err_counts.pop(ep, None)
                self._log_counters.pop(ep, None)
                self._id_cache.pop(ep, None)
        self.eps = list(endpoints)
        self._on_endpoints_changed(endpoints)

    def _on_endpoints_changed(self, endpoints: List[str]):
        pass

    def _cached_util(self, ep: str) -> Optional[float]:
        now = time.time()
        ts, val = self._util_cache.get(ep, (0.0, None))
        if now - ts < self._util_ttl_s:
            return val
        val = probe_gpu_util(ep, _cfg.METRICS_PATH)
        self._util_cache[ep] = (now, val)
        return val

    def _cached_identity(self, ep: str) -> Tuple[str, Optional[float]]:
        now = time.time()
        ts, nm, tps = self._id_cache.get(ep, (0.0, None, None))
        if now - ts < self._id_ttl_s and nm is not None:
            return nm, tps
        try:
            r = requests.get(ep.rstrip("/") + _cfg.HEALTH_PATH, timeout=0.5)
            if r.ok:
                data = r.json() or {}
                nm = data.get("name") or ep
                tps = float(data.get("tps")) if data.get("tps") is not None else None
            else:
                nm, tps = ep, None
        except Exception:
            nm, tps = ep, None
        self._id_cache[ep] = (now, nm, tps)
        return nm, tps

    def _launch_one(self, ep: str, prompt: str, t_enq_client: float, req_id: int):
        def _runner():
            try:
                predicted_out_tokens = _pred.predict_out_tokens(prompt, req_id=req_id)

                t_arrival_router = float(t_enq_client or time.time())
                t_dispatch_router = time.time()

                resp = send_chat_request(
                    endpoint=ep,
                    model=_cfg.MODEL_NAME,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=_cfg.MAX_TOKENS,
                    temperature=_cfg.TEMPERATURE,
                    request_timeout_s=_cfg.REQUEST_TIMEOUT_S,
                    req_id=req_id,
                )

                t_response_router = time.time()

                # --- Derived latencies (measured) ---
                router_queue_wait = t_dispatch_router - t_arrival_router
                server_roundtrip = t_response_router - t_dispatch_router
                end_to_end_latency = t_response_router - t_arrival_router

                # --- Override with simulated (server-provided) times when present ---
                sim_total   = resp.get("_sim_total_wall_s")
                sim_queue   = resp.get("_sim_queue_wait_s")
                sim_service = resp.get("_sim_service_s") or resp.get("_sim_latency_s")
                used_sim_times = False
                if sim_total is not None:
                    end_to_end_latency = float(sim_total); used_sim_times = True
                if sim_queue is not None:
                    router_queue_wait = float(sim_queue); used_sim_times = True
                if sim_service is not None:
                    server_roundtrip = float(sim_service); used_sim_times = True

                choice = (resp.get("choices") or [{}])[0]
                content = (choice.get("message") or {}).get("content", "")

                actual_out = None
                try:
                    usage = resp.get("usage") or {}
                    actual_out = usage.get("completion_tokens")
                except Exception:
                    pass

                log_result(
                    mode=self.mode_name,
                    endpoint=ep,
                    model=_cfg.MODEL_NAME,
                    prompt=prompt,
                    status="ok",
                    response_preview=content,
                    latency_s=end_to_end_latency,
                    extra={
                        "req_id": int(req_id),
                        "t_arrival_router": t_arrival_router,
                        "t_dispatch_router": t_dispatch_router,
                        "t_response_router": t_response_router,
                        "router_queue_wait": router_queue_wait,
                        "server_roundtrip": server_roundtrip,
                        "end_to_end_latency": end_to_end_latency,
                        "latency_source": "sim" if used_sim_times else "measured",
                        "measured_router_queue_wait": (t_dispatch_router - t_arrival_router),
                        "measured_server_roundtrip": (t_response_router - t_dispatch_router),
                        "measured_end_to_end_latency": (t_response_router - t_arrival_router),
                        "predictor_name": getattr(_pred, "name", "unknown"),
                        "predicted_out_tokens": None if predicted_out_tokens is None else int(predicted_out_tokens),
                        "actual_out_tokens": None if actual_out is None else int(actual_out),
                    },
                )
                with self._lock:
                    self.ok_counts[ep] = self.ok_counts.get(ep, 0) + 1
            except Exception as e:
                log_result(
                    mode=self.mode_name,
                    endpoint=ep,
                    model=_cfg.MODEL_NAME,
                    prompt=prompt,
                    status="error",
                    error=str(e),
                    extra={
                        "req_id": int(req_id),
                        "predictor_name": getattr(_pred, "name", "unknown"),
                    },
                )
                with self._lock:
                    self.err_counts[ep] = self.err_counts.get(ep, 0) + 1
                self.q.put((prompt, time.time(), req_id))
            finally:
                with self._lock:
                    self.inflight[ep] = max(0, self.inflight.get(ep, 0) - 1)

        threading.Thread(target=_runner, daemon=True).start()

    def _want_for_ep(self, ep: str) -> int:
        mode = self._admission_mode
        with self._lock:
            current = self.inflight.get(ep, 0)
        cap = max(0, int(_cfg.MAX_INFLIGHT_PER_EP) - current)

        if mode == "algo":
            return min(int(_cfg.BURST), self.q.qsize())

        if mode == "cap":
            if cap <= 0:
                return 0
            return min(int(_cfg.BURST), cap)

        util_pct = self._cached_util(ep)
        if cap <= 0:
            return 0
        if util_pct is None:
            return min(int(_cfg.NO_UTIL_BURST), cap)
        headroom = max(0.0, (float(_cfg.UTIL_THRESHOLD) - (util_pct / 100.0))) / max(
            float(_cfg.UTIL_THRESHOLD), 1e-6
        )
        return min(max(1, int(float(_cfg.BURST) * (1.0 + headroom))), cap)

    def _ordered_eps_for_step(self) -> List[str]:
        return list(self.eps)

    def step(self):
        now = time.time()
        if now - self._last_step_ts < self._step_interval_s:
            return
        self._last_step_ts = now

        for ep in self._ordered_eps_for_step():
            if not healthy(ep, _cfg.HEALTH_PATH):
                continue

            want = self._want_for_ep(ep)

            with self._lock:
                inflight_before = self.inflight.get(ep, 0)
                cur_idx = self._log_counters.get(ep, 0)
                every_n = max(1, int(_cfg.QUEUE_LOG_EVERY_N))
                do_log = cur_idx % every_n == 0

            q_before = self.q.qsize()
            pulled = 0

            if _USE_LEN_AWARE:
                selected = select_batch(
                    self.q, want, _pred, _Q_LOCK,
                    policy=_LEN_POLICY,
                    pool_factor=_POOL_FACTOR,
                    default_max_tokens=_DEFAULT_MAX_TOKENS,
                )
                for prompt, t_enq_client, _pred_tok, req_id in selected:
                    with self._lock:
                        self.inflight[ep] = self.inflight.get(ep, 0) + 1
                    pulled += 1
                    self._launch_one(ep, prompt, t_enq_client, req_id)
            else:
                from queue import Empty as QEmpty
                for _ in range(want):
                    try:
                        prompt, t_enq_client, req_id = self.q.get_nowait()
                    except QEmpty:
                        break
                    with self._lock:
                        self.inflight[ep] = self.inflight.get(ep, 0) + 1
                    pulled += 1
                    self._launch_one(ep, prompt, t_enq_client, req_id)

            q_after = self.q.qsize()
            with self._lock:
                inflight_after = self.inflight.get(ep, 0)

            if do_log:
                util_pct = self._cached_util(ep)
                util_str = "NONE" if util_pct is None else f"{util_pct:.1f}%"
                name, _ = self._cached_identity(ep)
                print(
                    f"[PUSH*STEP] {name} (ep={ep}) util={util_str} want={want} pulled={pulled} "
                    f"inflight={inflight_before}->{inflight_after} q={q_before}->{q_after} lenAware={_USE_LEN_AWARE} pol={_LEN_POLICY}"
                )
                log_queue(
                    router_mode=self.mode_name,
                    endpoint=ep,
                    event="push-step",
                    extra={
                        "session_idx": int(cur_idx),
                        "util_pct": None if util_pct is None else float(util_pct),
                        "want": int(want),
                        "pulled": int(pulled),
                        "inflight_before": int(inflight_before),
                        "inflight_after": int(inflight_after),
                        "q_before": int(q_before),
                        "q_after": int(q_after),
                        "every_n": every_n,
                        "len_aware": bool(_USE_LEN_AWARE),
                        "len_policy": str(_LEN_POLICY),
                        # "len_threshold": int(_PRED_LEN_THRESHOLD),
                        "pool_factor": int(_POOL_FACTOR)
                    },
                )

            with self._lock:
                self._log_counters[ep] = cur_idx + 1


    def has_work(self) -> bool:
        with self._lock:
            active = any(v > 0 for v in self.inflight.values())
        return (not self.q.empty()) or active

    def status_line(self) -> str:
        with self._lock:
            parts = []
            for ep in self.eps:
                name, _ = self._cached_identity(ep)
                parts.append(
                    f"{name} ({ep}) inflight={self.inflight.get(ep,0)} "
                    f"ok={self.ok_counts.get(ep,0)} err={self.err_counts.get(ep,0)}"
                )
        return (
            " | ".join(parts)
            + f" | queue={self.q.qsize()} | admit={self._admission_mode}"
        )

    def stats(self) -> Dict[str, Dict[str, int]]:
        with self._lock:
            out: Dict[str, Dict[str, int]] = {}
            for ep in sorted(self.ok_counts.keys()):
                name, _ = self._cached_identity(ep)
                key = f"{name} ({ep})"
                out[key] = {
                    "ok": self.ok_counts.get(ep, 0),
                    "err": self.err_counts.get(ep, 0),
                }
            return out


class RRBatchingRouter(_BaseBatchingRouter):
    """Batching with round-robin endpoint ordering each step."""

    def __init__(self, mode_name: str = "rr-batching"):
        super().__init__(mode_name)
        self._rr_order: List[str] = []
        self._rr_idx = 0

    def _on_endpoints_changed(self, endpoints: List[str]):
        self._rr_order = [ep for ep in self._rr_order if ep in endpoints]
        for ep in endpoints:
            if ep not in self._rr_order:
                self._rr_order.append(ep)
        self._rr_idx = (self._rr_idx % len(self._rr_order)) if self._rr_order else 0

    def _ordered_eps_for_step(self) -> List[str]:
        if not self._rr_order:
            return []
        n = len(self._rr_order)
        order = [self._rr_order[(self._rr_idx + i) % n] for i in range(n)]
        self._rr_idx = (self._rr_idx + 1) % n
        return order


class RandomBatchingRouter(_BaseBatchingRouter):
    """Batching with random endpoint ordering each step."""

    def __init__(self, mode_name: str = "random-batching"):
        super().__init__(mode_name)

    def _ordered_eps_for_step(self) -> List[str]:
        order = list(self.eps)
        random.shuffle(order)
        return order


class LeastQueueBatchingRouter(_BaseBatchingRouter):
    """Prefer endpoints with the fewest inflight (join-the-shortest-queue)."""

    def __init__(self, mode_name: str = "least-queue-batching"):
        super().__init__(mode_name)

    def _ordered_eps_for_step(self) -> List[str]:
        with self._lock:
            return sorted(self.eps, key=lambda ep: (self.inflight.get(ep, 0), ep))
