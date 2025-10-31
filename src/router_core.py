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
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

from config import get_config
from utils import log_result, log_queue, healthy, send_chat_request
from utils_prom import probe_gpu_util

# NEW: predictor hook
from predictors import get_length_predictor

# NEW: shared length-aware selection hook
from threading import RLock
from len_select import select_batch
from requests.adapters import HTTPAdapter

from length_backend import count_input_tokens


_cfg = get_config()
_pred = get_length_predictor()  # singleton predictor based on cfg.PREDICTOR_NAME

# ---- Length-aware config knobs (shared by both pull & push) ----
_USE_LEN_AWARE = bool(_cfg.USE_LEN_AWARE)
_LEN_POLICY = str(_cfg.LEN_POLICY or "short_first")
_POOL_FACTOR = int(_cfg.POOL_FACTOR)
_DEFAULT_MAX_TOKENS = int(_cfg.MAX_TOKENS)
_LEN_BASIS = str(_cfg.LEN_BASIS or "output")

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
      - NEW: archives stats for endpoints removed during the run,
             so final summary includes *all* used servers.
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

        # NEW: draining endpoints (present but should pull no new work)
        self._draining_eps: set[str] = set()

        # NEW: archive stats for endpoints that are removed during the run
        # key: endpoint URL -> {"ok": int, "err": int}
        self._stats_archive: Dict[str, Dict[str, int]] = {}

        # NEW: remember last known human-friendly name for endpoints
        # key: endpoint URL -> name string
        self._name_book: Dict[str, str] = {}

    # ------------- endpoints lifecycle -------------

    def ensure_endpoints(self, endpoints: List[str]):
        # Add / refresh workers and bookkeeping for present endpoints
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

        # Remove workers for endpoints that disappeared — ARCHIVE before dropping
        for ep in list(self._workers.keys()):
            if ep not in endpoints:
                # stop worker
                self._stop_flags[ep].set()
                self._workers[ep].join(timeout=2)
                self._workers.pop(ep, None)
                self._stop_flags.pop(ep, None)

                # ARCHIVE ok/err
                ok = int(self.ok_counts.pop(ep, 0))
                err = int(self.err_counts.pop(ep, 0))
                prev = self._stats_archive.get(ep, {"ok": 0, "err": 0})
                self._stats_archive[ep] = {"ok": prev["ok"] + ok, "err": prev["err"] + err}

                # clear the rest of the state
                with self._lock:
                    self.inflight.pop(ep, None)
                    self._log_counters.pop(ep, None)
                self._refill_evts.pop(ep, None)
                self._util_cache.pop(ep, None)

                # Keep _id_cache entry (and _name_book) if available so we can still show a nice name.
                # If you prefer to drop it, comment the next line and rely on _name_book instead.
                # self._id_cache.pop(ep, None)

        self.eps = list(endpoints)

    # ------------- util + identity caches -------------

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
            # remember last known name
            self._name_book[ep] = nm
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
        # remember last known name
        if nm:
            self._name_book[ep] = nm
        return nm, tps

    # ------------- want / admission -------------

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

    # ------------- launch + worker -------------

    def _launch_one(self, ep: str, prompt: str, t_enq_client: float, req_id: int):
        def _runner():
            try:
                predicted_out_tokens = _pred.predict_out_tokens(prompt, req_id=req_id)

                # SSOT timestamps
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

                # Derived latencies (measured)
                router_queue_wait = t_dispatch_router - t_arrival_router
                server_roundtrip = t_response_router - t_dispatch_router
                end_to_end_latency = t_response_router - t_arrival_router

                # Override with simulated times when present
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

                # --- Token accounting for logs ---
                input_actual_tokens = int(count_input_tokens(prompt))
                pred_out_int = None if predicted_out_tokens is None else int(predicted_out_tokens)
                act_out_int = None if actual_out is None else int(actual_out)

                total_predicted_tokens = (input_actual_tokens + pred_out_int) if pred_out_int is not None else None
                total_actual_tokens    = (input_actual_tokens + act_out_int) if act_out_int is not None else None


                log_result(
                    mode=self.mode_name,
                    endpoint=ep,
                    model=_cfg.MODEL_NAME,
                    prompt=prompt,
                    status="ok",
                    response=content,
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

                        # --- token stats ---
                        "input_tokens": int(input_actual_tokens),
                        "predicted_out_tokens": pred_out_int,
                        "actual_out_tokens": act_out_int,
                        "total_predicted_tokens": total_predicted_tokens,
                        "total_actual_tokens": total_actual_tokens,

                        # "predicted_out_tokens": None if predicted_out_tokens is None else int(predicted_out_tokens),
                        # "actual_out_tokens": None if actual_out is None else int(actual_out),
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

            # If draining, do not pull new work; just wait for inflight to drain
            if ep in getattr(self, "_draining_eps", set()):
                with self._lock:
                    active = self.inflight.get(ep, 0)
                if active == 0:
                    time.sleep(self._safety_interval_s * 0.5)
                continue

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
            # if _USE_LEN_AWARE:
            #     selected = select_batch(
            #         self.q, want, _pred, _Q_LOCK,
            #         policy=_LEN_POLICY,
            #         pool_factor=_POOL_FACTOR,
            #         default_max_tokens=_DEFAULT_MAX_TOKENS,
            #     )
            # else:
            #     selected = []
            #     for _ in range(want):
            #         try:
            #             prompt, t_enq_client, req_id = self.q.get_nowait()
            #             selected.append((prompt, t_enq_client, None, req_id))
            #         except Empty:
            #             break
            # ---- Length-aware selection ----
            if _USE_LEN_AWARE:
                selected = select_batch(
                    self.q, want, _pred, _Q_LOCK,
                    policy=_LEN_POLICY,
                    pool_factor=_POOL_FACTOR,
                    default_max_tokens=_DEFAULT_MAX_TOKENS,
                    length_basis=_LEN_BASIS,
                    input_len_fn=count_input_tokens,
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

    # ------------- router api -------------

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
        """
        Merge current and archived stats so final summary includes ALL endpoints
        that processed traffic during the run (even if removed before the end).
        Keys are "name (url)" when a name is known, else just "url (url)".
        """
        with self._lock:
            out: Dict[str, Dict[str, int]] = {}

            # 1) archived first
            for ep, s in self._stats_archive.items():
                name = self._name_book.get(ep) or ep
                key = f"{name} ({ep})"
                out[key] = {"ok": int(s.get("ok", 0)), "err": int(s.get("err", 0))}

            # 2) then overlay current (still-present eps)
            for ep in sorted(self.ok_counts.keys()):
                name, _ = self._cached_identity(ep)
                name = name or self._name_book.get(ep) or ep
                key = f"{name} ({ep})"
                ok = int(self.ok_counts.get(ep, 0))
                err = int(self.err_counts.get(ep, 0))
                if key in out:
                    out[key]["ok"] += ok
                    out[key]["err"] += err
                else:
                    out[key] = {"ok": ok, "err": err}

            return out

    def set_draining_eps(self, eps: set[str]):
        if not isinstance(eps, set):
            eps = set(eps)
        self._draining_eps = set(eps)


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
      - per-endpoint bounded-concurrency senders with persistent sessions
    Subclasses must implement: `_ordered_eps_for_step()`
    """

    def __init__(self, mode_name: str):
        self.mode_name = mode_name

        # Global work queue (prompt, t_enq_client, req_id)
        self.q: "Queue[Tuple[str, float, int]]" = Queue()

        # Endpoint state
        self.eps: List[str] = []
        self.inflight: Dict[str, int] = {}
        self.ok_counts: Dict[str, int] = {}
        self.err_counts: Dict[str, int] = {}

        # Archive stats for removed endpoints (so final summary includes all used)
        self._stats_archive: Dict[str, Dict[str, int]] = {}

        # Locks / pacing
        self._lock = threading.Lock()
        self._log_counters: Dict[str, int] = {}
        self._step_interval_s: float = float(getattr(_cfg, "SAMPLE_INTERVAL", 0.1))
        self._last_step_ts: float = 0.0

        # Caches
        self._util_cache: Dict[str, Tuple[float, Optional[float]]] = {}
        self._util_ttl_s: float = float(getattr(_cfg, "UTIL_TTL_S", 0.15))
        self._id_cache: Dict[str, Tuple[float, str, Optional[float]]] = {}
        self._id_ttl_s: float = float(getattr(_cfg, "TPS_TTL_S", 5.0))

        # Admission
        self._admission_mode: str = str(getattr(_cfg, "ADMISSION_MODE", "util")).lower()

        # Per-endpoint sender infra
        self._send_queues: Dict[str, Queue] = {}
        self._send_workers: Dict[str, threading.Thread] = {}
        self._send_stops: Dict[str, threading.Event] = {}
        self._per_ep_sem: Dict[str, threading.Semaphore] = {}
        self._session_pool: Dict[str, List[requests.Session]] = {}
        self._session_pool_lock: Dict[str, threading.Lock] = {}

        # Optional: endpoints present but should receive no new work (draining)
        self._draining_eps: set[str] = set()

    # ---------- identity & util caches ----------

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

    # ---------- admission: util/cap/algo ----------

    def _want_for_ep(self, ep: str) -> int:
        """
        Decide how many items to pull off the global queue for this endpoint on this tick.
        Robust to BURST<=0 (treated as 1) so ADMISSION_MODE='algo' can't stall.
        """
        mode = self._admission_mode

        # Capacity snapshot
        with self._lock:
            current = self.inflight.get(ep, 0)
        cap = max(0, int(_cfg.MAX_INFLIGHT_PER_EP) - current)

        # Global queue size snapshot
        try:
            qsz = self.q.qsize()
        except Exception:
            qsz = 0

        # Robust burst (avoid 0)
        try:
            burst = int(getattr(_cfg, "BURST", 1))
        except Exception:
            burst = 1
        if burst <= 0:
            burst = 1

        if mode == "algo":
            # Ignore cap here; per-EP sender semaphore enforces concurrency anyway.
            return min(burst, qsz)

        if mode == "cap":
            if cap <= 0:
                return 0
            return min(burst, cap, qsz)

        # util-based (default)
        util_pct = self._cached_util(ep)
        if cap <= 0:
            return 0
        if util_pct is None:
            no_util_burst = int(getattr(_cfg, "NO_UTIL_BURST", 1)) or 1
            return min(no_util_burst, cap, qsz)
        headroom = max(0.0, (float(_cfg.UTIL_THRESHOLD) - (util_pct / 100.0))) / max(float(_cfg.UTIL_THRESHOLD), 1e-6)
        want = max(1, int(float(burst) * (1.0 + headroom)))
        return min(want, cap, qsz)

    # ---------- endpoint churn ----------

    def ensure_endpoints(self, endpoints: List[str]):
        # Add/refresh
        for ep in endpoints:
            if ep not in self.inflight:
                self.inflight[ep] = 0
                self.ok_counts[ep] = 0
                self.err_counts[ep] = 0
                self._log_counters[ep] = 0
            self._ensure_sender_worker(ep)

        # Remove (archive stats so we can report them at the end)
        for ep in list(self.inflight.keys()):
            if ep not in endpoints:
                # archive before dropping
                ok = int(self.ok_counts.pop(ep, 0))
                err = int(self.err_counts.pop(ep, 0))
                prev = self._stats_archive.get(ep, {"ok": 0, "err": 0})
                self._stats_archive[ep] = {"ok": prev["ok"] + ok, "err": prev["err"] + err}

                self.inflight.pop(ep, None)
                self._log_counters.pop(ep, None)
                self._id_cache.pop(ep, None)
                self._stop_sender_worker(ep)

        self.eps = list(endpoints)
        self._on_endpoints_changed(endpoints)

    def _on_endpoints_changed(self, endpoints: List[str]):
        # Subclasses may override (RR reorders, etc.)
        pass

    # ---------- per-endpoint sender infra ----------

    # def _make_session(self) -> requests.Session:
    #     s = requests.Session()
    #     adapter = HTTPAdapter(pool_connections=1, pool_maxsize=1, pool_block=True)
    #     s.mount("http://", adapter)
    #     s.mount("https://", adapter)
    #     return s

    def _make_session(self) -> requests.Session:
        s = requests.Session()
        s.trust_env = False  # ← ignore HTTP(S)_PROXY/NO_PROXY for this session
        retry = Retry(
            total=3, connect=3, read=3,
            backoff_factor=0.2,
            status_forcelist=[502, 503, 504],
            allowed_methods=frozenset(["GET", "POST"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(pool_connections=1, pool_maxsize=1, pool_block=True, max_retries=retry)
        s.mount("http://", adapter)
        s.mount("https://", adapter)
        return s

    def _ensure_sender_worker(self, ep: str):
        if ep in self._send_workers:
            return

        conc = int(getattr(_cfg, "MAX_INFLIGHT_PER_EP", 8))
        self._per_ep_sem[ep] = threading.Semaphore(conc)
        self._session_pool[ep] = [self._make_session() for _ in range(conc)]
        self._session_pool_lock[ep] = threading.Lock()

        q = Queue()
        stop_evt = threading.Event()
        self._send_queues[ep] = q
        self._send_stops[ep] = stop_evt

        t = threading.Thread(target=self._sender_loop, args=(ep,), daemon=True)
        self._send_workers[ep] = t
        t.start()

    def _stop_sender_worker(self, ep: str):
        ev = self._send_stops.pop(ep, None)
        if ev:
            ev.set()
        th = self._send_workers.pop(ep, None)
        if th:
            th.join(timeout=1.5)

        # Drop queue first to prevent further use
        self._send_queues.pop(ep, None)

        # Close all sessions for this endpoint
        pool = self._session_pool.pop(ep, [])
        for s in pool:
            try:
                s.close()
            except Exception:
                pass

        self._session_pool_lock.pop(ep, None)
        self._per_ep_sem.pop(ep, None)

    # ----- sender helpers (no nested functions) -----

    def _sender_checkout_session(self, ep: str) -> Optional[requests.Session]:
        lock = self._session_pool_lock.get(ep)
        pool = self._session_pool.get(ep)
        if not lock or pool is None:
            return None
        with lock:
            if pool:
                return pool.pop()
        return None

    def _sender_return_session(self, ep: str, sess: Optional[requests.Session]):
        if sess is None:
            return
        lock = self._session_pool_lock.get(ep)
        if not lock:
            try:
                sess.close()
            except Exception:
                pass
            return
        with lock:
            pool = self._session_pool.get(ep)
            if pool is None:
                try:
                    sess.close()
                except Exception:
                    pass
            else:
                pool.append(sess)

    def _sender_loop(self, ep: str):
        # small jitter to de-sync across pods
        time.sleep(random.uniform(0.0, 0.01))
        q = self._send_queues.get(ep)
        stop_evt = self._send_stops.get(ep)
        sem = self._per_ep_sem.get(ep)
        if q is None or stop_evt is None or sem is None:
            return

        while not stop_evt.is_set():
            try:
                prompt, t_enq_client, req_id = q.get(timeout=0.1)
            except Empty:
                continue

            # Bound concurrent inflight per endpoint
            try:
                sem.acquire()
            except Exception:
                # Sem gone? requeue the item and exit
                try:
                    self.q.put((prompt, t_enq_client, req_id))
                except Exception:
                    pass
                break

            # Tiny pacing avoids micro-bursts; still inside batching window
            time.sleep(0.0015)

            # Launch runner thread
            threading.Thread(
                target=self._sender_runner,
                args=(ep, prompt, t_enq_client, req_id),
                daemon=True,
            ).start()

            try:
                q.task_done()
            except Exception:
                pass

    def _sender_runner(self, ep: str, prompt: str, t_enq_client: float, req_id: int):
        sess = self._sender_checkout_session(ep)
        try:
            if sess is None:
                # If we cannot get a session (pool gone), requeue and bail
                try:
                    self.q.put((prompt, t_enq_client, req_id))
                except Exception:
                    pass
                return

            # Mark ACTIVE now (after lane acquired)
            with self._lock:
                self.inflight[ep] = self.inflight.get(ep, 0) + 1

            self._send_one(ep, prompt, t_enq_client, req_id, sess)

        finally:
            # Return/close session safely
            try:
                self._sender_return_session(ep, sess)
            except Exception:
                try:
                    if sess is not None:
                        sess.close()
                except Exception:
                    pass

            # Release semaphore safely
            sem = self._per_ep_sem.get(ep)
            if sem:
                try:
                    sem.release()
                except Exception:
                    pass

    # ---------- single send (used by per-EP runners) ----------

    def _send_one(self, ep: str, prompt: str, t_enq_client: float, req_id: int, session: Optional[requests.Session]):
        predicted_out_tokens = _pred.predict_out_tokens(prompt, req_id=req_id)
        t_arrival_router = float(t_enq_client or time.time())
        t_dispatch_router = time.time()
        try:
            # bounded retry for transient 'connection reset by peer'
            tries = 0
            while True:
                try:
                    resp = send_chat_request(
                        endpoint=ep,
                        model=_cfg.MODEL_NAME,
                        messages=[{"role": "user", "content": prompt}],
                        max_tokens=_cfg.MAX_TOKENS,
                        temperature=_cfg.TEMPERATURE,
                        request_timeout_s=_cfg.REQUEST_TIMEOUT_S,
                        req_id=req_id,
                        session=session,  # persistent connection reuse
                    )
                    break
                except Exception as e:
                    se = str(e)
                    tries += 1
                    if "Connection reset by peer" in se and tries <= 2:
                        time.sleep(0.03 * tries)
                        continue
                    raise

            t_response_router = time.time()
            router_queue_wait = t_dispatch_router - t_arrival_router
            server_roundtrip = t_response_router - t_dispatch_router
            end_to_end_latency = t_response_router - t_arrival_router

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

            # --- Token accounting for logs ---
            input_actual_tokens = int(count_input_tokens(prompt))
            pred_out_int = None if predicted_out_tokens is None else int(predicted_out_tokens)
            act_out_int = None if actual_out is None else int(actual_out)

            total_predicted_tokens = (input_actual_tokens + pred_out_int) if pred_out_int is not None else None
            total_actual_tokens    = (input_actual_tokens + act_out_int) if act_out_int is not None else None

            log_result(
                mode=self.mode_name,
                endpoint=ep,
                model=_cfg.MODEL_NAME,
                prompt=prompt,
                status="ok",
                response=content,
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

                    # --- token stats ---
                    "input_tokens": int(input_actual_tokens),
                    "predicted_out_tokens": pred_out_int,
                    "actual_out_tokens": act_out_int,
                    "total_predicted_tokens": total_predicted_tokens,
                    "total_actual_tokens": total_actual_tokens,
                    # "predicted_out_tokens": None if predicted_out_tokens is None else int(predicted_out_tokens),
                    # "actual_out_tokens": None if actual_out is None else int(actual_out),
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
                extra={"req_id": int(req_id), "predictor_name": getattr(_pred, "name", "unknown")},
            )
            with self._lock:
                self.err_counts[ep] = self.err_counts.get(ep, 0) + 1
            # requeue; sender pacing avoids hammering
            try:
                self.q.put((prompt, time.time(), req_id))
            except Exception:
                pass
        finally:
            with self._lock:
                self.inflight[ep] = max(0, self.inflight.get(ep, 0) - 1)

    # ---------- scheduler ----------

    def _ordered_eps_for_step(self) -> List[str]:
        return list(self.eps)

    def step(self):
        now = time.time()
        if now - self._last_step_ts < self._step_interval_s:
            return
        self._last_step_ts = now

        for ep in self._ordered_eps_for_step():
            # Skip endpoints that are draining (if any)
            if ep in getattr(self, "_draining_eps", set()):
                continue

            # Skip unhealthy endpoints
            if not healthy(ep, _cfg.HEALTH_PATH):
                continue

            # Ensure per-endpoint sender exists
            if ep not in self._send_queues:
                try:
                    self._ensure_sender_worker(ep)
                except Exception:
                    continue  # skip this endpoint for this tick

            want = self._want_for_ep(ep)
            if want <= 0:
                # Still increment log counter occasionally to keep heartbeat
                with self._lock:
                    cur_idx = self._log_counters.get(ep, 0)
                    self._log_counters[ep] = cur_idx + 1
                continue

            # Pre-step state
            with self._lock:
                inflight_before = self.inflight.get(ep, 0)
                cur_idx = self._log_counters.get(ep, 0)
                every_n = max(1, int(_cfg.QUEUE_LOG_EVERY_N))
                do_log = cur_idx % every_n == 0

            q_before = self.q.qsize()

            # backlog BEFORE enqueuing to per-EP sender
            try:
                q_ep = self._send_queues.get(ep)
                backlog_before = q_ep.qsize() if q_ep is not None else 0
            except Exception:
                backlog_before = 0

            logical_before = inflight_before + backlog_before
            pulled = 0

            # --- main selection + enqueue block (now correctly inside loop) ---
            if _USE_LEN_AWARE:
                selected = select_batch(
                    self.q, want, _pred, _Q_LOCK,
                    policy=_LEN_POLICY,
                    pool_factor=_POOL_FACTOR,
                    default_max_tokens=_DEFAULT_MAX_TOKENS,
                    length_basis=_LEN_BASIS,
                    input_len_fn=count_input_tokens,
                )
                for prompt, t_enq_client, _key_len, req_id in selected:
                    q_ep = self._send_queues.get(ep)
                    if q_ep is None:
                        break
                    pulled += 1
                    q_ep.put((prompt, t_enq_client, req_id))
            else:
                from queue import Empty as QEmpty
                for _ in range(max(0, want)):
                    try:
                        prompt, t_enq_client, req_id = self.q.get_nowait()
                    except QEmpty:
                        break
                    q_ep = self._send_queues.get(ep)
                    if q_ep is None:
                        # endpoint disappeared; put item back and stop
                        try:
                            self.q.put((prompt, t_enq_client, req_id))
                        except Exception:
                            pass
                        break
                    pulled += 1
                    q_ep.put((prompt, t_enq_client, req_id))
            # --- end selection + enqueue ---

            q_after = self.q.qsize()

            with self._lock:
                inflight_after = self.inflight.get(ep, 0)

            # backlog AFTER enqueuing
            try:
                q_ep = self._send_queues.get(ep)
                backlog_after = q_ep.qsize() if q_ep is not None else max(0, backlog_before + pulled)
            except Exception:
                backlog_after = max(0, backlog_before + pulled)

            logical_after = inflight_after + backlog_after

            # optional logging
            if do_log:
                util_pct = self._cached_util(ep)
                util_str = "NONE" if util_pct is None else f"{util_pct:.1f}%"
                name, _ = self._cached_identity(ep)
                print(
                    f"[PUSH*STEP] {name} (ep={ep}) util={util_str} want={want} pulled={pulled} "
                    f"inflight={logical_before}->{logical_after} (active={inflight_before}->{inflight_after}) "
                    f"backlog={backlog_before}->{backlog_after} q={q_before}->{q_after} "
                    f"lenAware={_USE_LEN_AWARE} pol={_LEN_POLICY}"
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
                        "active_before": int(inflight_before),
                        "active_after": int(inflight_after),
                        "backlog_before": int(backlog_before),
                        "backlog_after": int(backlog_after),
                        "logical_before": int(logical_before),
                        "logical_after": int(logical_after),
                        "q_before": int(q_before),
                        "q_after": int(q_after),
                        "every_n": every_n,
                        "len_aware": bool(_USE_LEN_AWARE),
                        "len_policy": str(_LEN_POLICY),
                        "pool_factor": int(_POOL_FACTOR),
                    },
                )

            # bump per-EP log counter
            with self._lock:
                self._log_counters[ep] = cur_idx + 1


    # ---------- router api ----------

    def has_work(self) -> bool:
        with self._lock:
            active = any(v > 0 for v in self.inflight.values())
        return (not self.q.empty()) or active

    def status_line(self) -> str:
        with self._lock:
            parts = []
            for ep in self.eps:
                name, _ = self._cached_identity(ep)
                active = self.inflight.get(ep, 0)  # actual inflight to vLLM
                try:
                    backlog = self._send_queues[ep].qsize()  # queued-to-send at per-EP sender
                except Exception:
                    backlog = 0
                logical = active + backlog  # total assigned to this endpoint
                parts.append(
                    f"{name} ({ep}) inflight={logical} (active={active}) "
                    f"backlog={backlog} ok={self.ok_counts.get(ep,0)} err={self.err_counts.get(ep,0)}"
                )
        return " | ".join(parts) + f" | queue={self.q.qsize()} | admit={self._admission_mode}"

    def stats(self) -> Dict[str, Dict[str, int]]:
        # Merge current and archived so end-of-run summary includes all used endpoints
        with self._lock:
            out: Dict[str, Dict[str, int]] = {}
            # archived first
            for ep, s in self._stats_archive.items():
                out[ep] = {"ok": int(s.get("ok", 0)), "err": int(s.get("err", 0))}
            # then overlay current (to include those still present)
            for ep in self.ok_counts.keys():
                ok = int(self.ok_counts.get(ep, 0))
                err = int(self.err_counts.get(ep, 0))
                if ep in out:
                    out[ep]["ok"] += ok
                    out[ep]["err"] += err
                else:
                    out[ep] = {"ok": ok, "err": err}
            return out

    def set_draining_eps(self, eps: set[str]):
        """Mark endpoints that should receive no new work (kept alive until inflight drains)."""
        if not isinstance(eps, set):
            eps = set(eps)
        self._draining_eps = set(eps)




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
    """Prefer endpoints with the smallest total backlog (inflight + per-EP sender queue)."""

    def __init__(self, mode_name: str = "least-queue-batching"):
        super().__init__(mode_name)

    def _ordered_eps_for_step(self) -> List[str]:
        with self._lock:
            # Snapshots
            inflight = {ep: self.inflight.get(ep, 0) for ep in self.eps}
            backlog = {}
            for ep in self.eps:
                q_ep = self._send_queues.get(ep)
                backlog[ep] = q_ep.qsize() if q_ep is not None else 0

            # Logical load = active inflight + queued-to-send backlog
            logical = {ep: inflight[ep] + backlog[ep] for ep in self.eps}

            # Order by logical, then inflight, then endpoint (stable)
            return sorted(self.eps, key=lambda ep: (logical[ep], inflight[ep], ep))
