# router/api.py
# -*- coding: utf-8 -*-
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from fastapi import status
from fastapi import BackgroundTasks

import httpx
import time
import sys
import asyncio
from threading import RLock
from typing import Optional, Dict, Any, List, Tuple

from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from .config import get_config, print_config
from .models import (
    EnqueueRequest,  # required (used by /enqueue and /submit handler type)
    PullRequest,
    PullResponse,
)
from .router_state import router_state
from .kv_watcher import KVWatcher
from .kv_aware import register_request_blocks
from .push_router import PushRouter
from .metrics import (
    inc_admission,
    observe_output_len_error,
    observe_ttft_prediction_error,
    observe_e2e_prediction_error,
    inc_slo_actual_miss,
    inc_slo_actual_met,
    set_slo_registry_size,
)
from .slo_state import SLORegistry, SLOEntry

# Push-dispatch metrics (present in router/metrics.py per prior changes)
try:
    from .metrics import (
        set_push_dispatch_queue_length,   # Gauge
        inc_push_dispatch_enqueued,       # Counter
        inc_push_dispatch_started,        # Counter
        inc_push_dispatch_failed,         # Counter
        inc_push_dispatch_dropped,        # Counter
    )
except Exception:  # pragma: no cover
    def set_push_dispatch_queue_length(_n: int) -> None:
        return

    def inc_push_dispatch_enqueued() -> None:
        return

    def inc_push_dispatch_started() -> None:
        return

    def inc_push_dispatch_failed() -> None:
        return

    def inc_push_dispatch_dropped() -> None:
        return


# Async pubsub publisher (added later as router/pubsub.py)
from .pubsub import ResultPublisher  # type: ignore

# ============================================================
# Pydantic models for OpenAI-compatible /v1/chat/completions
# ============================================================
from pydantic import BaseModel

class _ChatMessage(BaseModel):
    role: str
    content: str

class _ChatCompletionRequest(BaseModel):
    model: str = "served-model"
    messages: List[_ChatMessage]
    max_tokens: Optional[int] = None
    temperature: Optional[float] = None
    stream: Optional[bool] = False
# ============================================================

_cfg = get_config()
app = FastAPI(title="KV-aware Router Service")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_kv_watcher: KVWatcher | None = None
_push_router: PushRouter | None = None

# PubSub publisher (optional; enabled when TRANSPORT_MODE=async_pubsub)
_publisher: Optional[ResultPublisher] = None

# Long-lived hash client (avoid creating AsyncClient per request)
_hash_client: Optional[httpx.AsyncClient] = None

# Map req_id -> run_id (so results publish can include run_id filtering)
_rid_runid_lock = RLock()
_rid_to_run_id: Dict[str, str] = {}

# SLO registry (in-memory, thread-safe)
_slo_registry = SLORegistry(ttl_s=float(getattr(_cfg, "POLL_RESULT_TTL_S", 300.0)))


# ============================================================
# Push dispatch decoupling (PUSH mode only)
# ============================================================

# (req_id, prompt, meta, t_submit)
_PushJob = Tuple[str, str, Dict[str, Any], float]


class _PushDispatcher:
    """
    Background dispatcher for PUSH mode.

    Goal: decouple request handler latency from sidecar push.

    Behavior:
      - enqueue job (req_id, prompt, meta) into an asyncio.Queue
      - N worker tasks do:
           meta2 = await _maybe_register_kv_blocks(..., is_pull_mode=False)
           await _push_router.route_and_push(...)
      - On queue full / failures, store a synthetic error result to unblock /enqueue waiters
        (and publish via pubsub best-effort).
    """

    def __init__(self, *, queue_max: int, workers: int, max_delay_s: float):
        self._queue_max = max(1, int(queue_max))
        self._workers_n = max(1, int(workers))
        self._max_delay_s = max(0.0, float(max_delay_s))

        self._q: asyncio.Queue[_PushJob] = asyncio.Queue(maxsize=self._queue_max)
        self._tasks: List[asyncio.Task] = []
        self._stopped = False

        set_push_dispatch_queue_length(0)

    def qsize(self) -> int:
        try:
            return int(self._q.qsize())
        except Exception:
            return 0

    def try_submit(self, req_id: str, prompt: str, meta: Dict[str, Any]) -> bool:
        """
        Non-blocking enqueue. Returns False if queue is full or dispatcher stopped.
        """
        if self._stopped:
            return False
        try:
            job: _PushJob = (str(req_id), str(prompt), dict(meta or {}), time.time())
            self._q.put_nowait(job)
            inc_push_dispatch_enqueued()
            set_push_dispatch_queue_length(self.qsize())
            return True
        except asyncio.QueueFull:
            inc_push_dispatch_dropped()
            set_push_dispatch_queue_length(self.qsize())
            return False
        except Exception:
            inc_push_dispatch_dropped()
            return False

    def start(self) -> None:
        if self._tasks:
            return
        loop = asyncio.get_running_loop()
        for i in range(self._workers_n):
            self._tasks.append(loop.create_task(self._worker(i)))

    async def stop(self) -> None:
        """
        Stop workers cleanly.

        IMPORTANT: asyncio.CancelledError is not an Exception (it inherits BaseException),
        so we must swallow it explicitly; otherwise Starlette reports "shutdown failed".
        """
        self._stopped = True
        for t in self._tasks:
            try:
                t.cancel()
            except Exception:
                pass

        for t in self._tasks:
            try:
                await t
            except asyncio.CancelledError:
                # Expected during shutdown
                pass
            except Exception:
                pass

        self._tasks = []
        set_push_dispatch_queue_length(self.qsize())

    async def _worker(self, idx: int) -> None:
        while True:
            req_id = "unknown"
            try:
                req_id, prompt, meta, t_submit = await self._q.get()
            except asyncio.CancelledError:
                # Normal shutdown path: exit quietly
                return
            except Exception:
                await asyncio.sleep(0.01)
                continue

            try:
                set_push_dispatch_queue_length(self.qsize())
                inc_push_dispatch_started()

                # Drop ancient tasks (prevents unbounded lag under overload)
                if self._max_delay_s > 0.0:
                    age_s = time.time() - float(t_submit)
                    if age_s > self._max_delay_s:
                        inc_push_dispatch_failed()
                        _store_and_maybe_publish_local_result(
                            req_id=req_id,
                            result={"error": f"push_dispatch_stale age_s={age_s:.3f}"},
                        )
                        continue

                # KV hashing moved here for push-mode decoupling.
                meta2 = await _maybe_register_kv_blocks(
                    req_id,
                    prompt,
                    meta=meta,
                    is_pull_mode=False,
                )

                if _push_router is None:
                    raise RuntimeError("PushRouter not initialized")

                await _push_router.route_and_push(req_id, prompt, meta2)

            except asyncio.CancelledError:
                # If cancelled mid-processing, exit quietly
                return
            except Exception as e:
                inc_push_dispatch_failed()
                _store_and_maybe_publish_local_result(
                    req_id=req_id,
                    result={"error": f"push_dispatch_failed: {type(e).__name__}: {e}"},
                )
            finally:
                try:
                    self._q.task_done()
                except Exception:
                    pass
                set_push_dispatch_queue_length(self.qsize())


_push_dispatcher: Optional[_PushDispatcher] = None


def _push_decouple_enabled() -> bool:
    """
    Enabled only in push-* router modes and when config enables it.
    """
    if not _is_push_mode():
        return False
    return bool(getattr(_cfg, "PUSH_DECOUPLE_DISPATCH", False))


def _store_and_maybe_publish_local_result(*, req_id: str, result: Any, endpoint: Optional[str] = None) -> None:
    """
    Used only for synthetic local errors in push dispatch.
    Mirrors _ingest_result_payload's publish behavior.
    """
    rid = str(req_id)
    try:
        router_state.store_result(rid, result)
    except Exception:
        pass

    if _publisher is not None:
        try:
            run_id = _pop_run_id(rid)
            pub_payload: Dict[str, Any] = {"req_id": rid, "result": result}
            if endpoint:
                pub_payload["endpoint"] = endpoint
            if run_id:
                pub_payload["run_id"] = run_id
            _publisher.publish(pub_payload)
        except Exception:
            pass
    else:
        _pop_run_id(rid)


# ============================================================
# Helpers
# ============================================================

def _is_push_mode() -> bool:
    """Return True if router is running a push-* mode."""
    return str(_cfg.ROUTER_MODE).startswith("push-")


def _transport_async_pubsub_enabled() -> bool:
    return str(getattr(_cfg, "TRANSPORT_MODE", "sync")).lower() == "async_pubsub"


def _result_transport_submit_ack() -> bool:
    return str(getattr(_cfg, "RESULT_TRANSPORT_MODE", "sync")).lower() == "submit_ack"


def _remember_run_id(req_id: str, meta: Dict[str, Any]) -> None:
    """
    Best-effort: remember run_id so pubsub publish can include it.
    Convention: client sets meta["__run_id"] = "<string>".
    """
    try:
        run_id = meta.get("__run_id")
        if not isinstance(run_id, str) or not run_id:
            return
        with _rid_runid_lock:
            _rid_to_run_id[req_id] = run_id
    except Exception:
        pass


def _pop_run_id(req_id: str) -> Optional[str]:
    with _rid_runid_lock:
        return _rid_to_run_id.pop(req_id, None)


def _log_api_req(msg: str, *, level: str = "summary") -> None:
    """
    Logging controlled by REQ_LOG_MODE (off | summary | full)
    """
    mode = str(_cfg.REQ_LOG_MODE).lower()

    if mode == "off":
        return

    try:
        qlen = router_state.size()
        msg = f"{msg} (queue_len={qlen})"
    except Exception:
        pass

    prefix = "[API]"

    if mode == "summary":
        if level == "summary":
            print(f"{prefix} {msg}")
            sys.stdout.flush()
        return

    print(f"{prefix} {msg}")
    sys.stdout.flush()


def _log_kv_hash(msg: str, *, level: str = "full") -> None:
    """
    KV-hash debug logging (uses existing REQ_LOG_MODE gating).
    """
    mode = str(_cfg.REQ_LOG_MODE).lower()
    if mode == "off":
        return
    if level == "summary":
        print(f"[KVHASH] {msg}")
        sys.stdout.flush()
        return
    if level == "full" and mode == "full":
        print(f"[KVHASH] {msg}")
        sys.stdout.flush()


def _safe_int_list(xs: Any) -> List[int]:
    out: List[int] = []
    if not isinstance(xs, list):
        return out
    for x in xs:
        try:
            out.append(int(x))
        except Exception:
            continue
    return out


def _get_hash_client() -> Optional[httpx.AsyncClient]:
    # Best-effort: might be None during early startup or if creation failed.
    return _hash_client


async def _maybe_register_kv_blocks(
    req_id: str,
    prompt: str,
    *,
    meta: Optional[Dict[str, Any]] = None,
    is_pull_mode: bool,
) -> Dict[str, Any]:
    """
    Best-effort KV-block computation.

    Side effects:
    - register_request_blocks(req_id, block_hashes)
    - If TRACE_ENABLED, attach router-computed block hashes into meta["__trace__"].
    """
    m: Dict[str, Any] = dict(meta or {})

    if not _cfg.KV_AWARE:
        return m

    # If tracing is on, pre-seed trace keys so "missing" is meaningful.
    if getattr(_cfg, "TRACE_ENABLED", False):
        tr0 = dict(m.get("__trace__") or {})
        tr0.setdefault("router_block_hashes", None)  # None => not computed yet
        m["__trace__"] = tr0

        if is_pull_mode:
            try:
                router_state.update_meta(req_id, m)
            except Exception:
                pass

    t0 = time.time()
    try:
        client = _get_hash_client()
        close_after = False
        if client is None:
            # Fallback (should be rare): create a short-lived client.
            t = float(getattr(_cfg, "HASH_TIMEOUT_S", 2.0))
            timeout = httpx.Timeout(connect=t, read=t, write=t, pool=t)
            limits = httpx.Limits(
                max_keepalive_connections=int(getattr(_cfg, "HASH_MAX_KEEPALIVE", 50)),
                max_connections=int(getattr(_cfg, "HASH_MAX_KEEPALIVE", 50)),
                keepalive_expiry=float(getattr(_cfg, "HASH_KEEPALIVE_EXPIRY_S", 30.0)),
            )
            client = httpx.AsyncClient(timeout=timeout, limits=limits)
            close_after = True

        try:
            resp = await client.post(
                f"{_cfg.HASH_SERVICE_URL}/compute_hashes",
                json={"prompt": prompt},
                timeout=_cfg.HASH_TIMEOUT_S,
            )
            resp.raise_for_status()
            data = resp.json()
        finally:
            if close_after:
                try:
                    await client.aclose()
                except Exception:
                    pass

        block_hashes = _safe_int_list(data.get("block_hashes") or [])

        _log_kv_hash(
            f"req_id={req_id} status={resp.status_code} "
            f"took_s={(time.time() - t0):.3f} "
            f"n_hashes={len(block_hashes)}",
            level="full",
        )

        register_request_blocks(req_id, block_hashes)

        if getattr(_cfg, "TRACE_ENABLED", False):
            tr = dict(m.get("__trace__") or {})
            tr["router_block_hashes"] = block_hashes  # ALWAYS set (possibly [])
            tr.pop("router_kv_hash_error", None)
            m["__trace__"] = tr

            if is_pull_mode:
                try:
                    router_state.update_meta(req_id, m)
                except Exception:
                    pass

    except Exception as e:
        print(f"[router] WARNING: KV hash compute failed for req_id={req_id}: {e}")
        sys.stdout.flush()

        _log_kv_hash(
            f"req_id={req_id} ERROR took_s={(time.time() - t0):.3f} err={type(e).__name__}: {e}",
            level="summary",
        )

        if getattr(_cfg, "TRACE_ENABLED", False):
            tr = dict(m.get("__trace__") or {})
            tr.setdefault("router_block_hashes", None)
            tr["router_kv_hash_error"] = f"{type(e).__name__}: {e}"
            m["__trace__"] = tr

            if is_pull_mode:
                try:
                    router_state.update_meta(req_id, m)
                except Exception:
                    pass

    return m


def _register_slo(rid: str, req: EnqueueRequest, arrival_ts: float) -> None:
    """Register SLO state for a request if annotations are present."""
    if not (req.slo_type or req.task_type or req.output_len_hint is not None):
        if not _cfg.SLO_AWARE:
            return
    input_tokens = max(1, len(req.prompt) // 4)
    entry = _slo_registry.register(
        rid,
        slo_type=req.slo_type,
        slo_ttft_ms=req.slo_ttft_ms,
        slo_tpot_ms=req.slo_tpot_ms,
        slo_e2e_ms=req.slo_e2e_ms,
        task_type=req.task_type,
        output_len_hint=req.output_len_hint,
        input_tokens=input_tokens,
        arrival_ts=arrival_ts,
    )

    # Predict output length
    from .predictors import get_output_length_predictor
    pred = get_output_length_predictor()
    if req.output_len_hint is not None and req.output_len_hint > 0:
        predicted_len = req.output_len_hint
    else:
        predicted_len = pred.predict(
            req.prompt,
            input_tokens=input_tokens,
            task_type=req.task_type or "",
            req_id=rid,
        )
    _slo_registry.set_predicted_output_len(rid, predicted_len)


def _install_submit_route() -> None:
    """
    Install the /submit endpoint at the configured SUBMIT_PATH.
    """
    async def _submit_handler(req: EnqueueRequest):
        return await submit(req)

    submit_path = getattr(_cfg, "SUBMIT_PATH", "/submit") or "/submit"
    if not str(submit_path).startswith("/"):
        submit_path = "/" + str(submit_path)

    if submit_path != "/submit":
        app.add_api_route(
            submit_path,
            _submit_handler,
            methods=["POST"],
            name="submit",
        )


def _install_result_submit_route() -> None:
    """
    Install RESULT_SUBMIT_PATH (default /result_submit) if RESULT_TRANSPORT_MODE=submit_ack.
    """
    async def _result_submit_handler(payload: dict, background_tasks: BackgroundTasks):
        return await result_submit_ack(payload, background_tasks)

    if not _result_transport_submit_ack():
        return

    path = getattr(_cfg, "RESULT_SUBMIT_PATH", "/result_submit") or "/result_submit"
    if not str(path).startswith("/"):
        path = "/" + str(path)

    if path == "/result":
        print("[router] WARNING: RESULT_SUBMIT_PATH=/result is not allowed; ignoring.")
        sys.stdout.flush()
        return

    app.add_api_route(
        path,
        _result_submit_handler,
        methods=["POST"],
        name="result_submit_ack",
    )
    print(f"[router] Result submit-ack route installed at {path}")
    sys.stdout.flush()


def _ingest_slo_actuals(rid: str, result: Any) -> None:
    """Extract actual latency / token counts from the result and update SLO registry + predictors."""
    if not isinstance(result, dict):
        return

    trace = result.get("trace") or result.get("__trace__") or {}
    usage = result.get("usage") or {}

    actual_ttft: Optional[float] = None
    actual_e2e: Optional[float] = None
    actual_output_len: Optional[int] = None

    if isinstance(trace, dict):
        if "ttft_s" in trace:
            actual_ttft = float(trace["ttft_s"])
        elif "t_first_token" in trace and "t_prefill_start" in trace:
            actual_ttft = float(trace["t_first_token"]) - float(trace["t_prefill_start"])
        if "e2e_s" in trace:
            actual_e2e = float(trace["e2e_s"])

    if isinstance(usage, dict):
        ct = usage.get("completion_tokens")
        if ct is not None:
            actual_output_len = int(ct)

    # Update output length predictor with actuals
    if actual_output_len is not None and actual_output_len > 0:
        from .predictors import get_output_length_predictor
        pred = get_output_length_predictor()
        entry = _slo_registry.get(rid)
        task_type = entry.task_type if entry else ""
        input_tokens = entry.input_tokens if entry else 0
        pred.update(rid, actual_output_len, task_type=task_type or "", input_tokens=input_tokens)

    entry = _slo_registry.ingest_result(
        rid,
        actual_ttft=actual_ttft,
        actual_output_len=actual_output_len,
        actual_e2e=actual_e2e,
    )

    if entry is None:
        return

    # Prediction error logging
    if entry.predicted_output_len and entry.actual_output_len and entry.actual_output_len > 0:
        ratio = (entry.predicted_output_len - entry.actual_output_len) / entry.actual_output_len
        observe_output_len_error(ratio)

    if entry.predicted_ttft is not None and entry.actual_ttft is not None:
        observe_ttft_prediction_error(entry.predicted_ttft - entry.actual_ttft)

    if entry.predicted_e2e is not None and entry.actual_e2e is not None:
        observe_e2e_prediction_error(entry.predicted_e2e - entry.actual_e2e)

    # SLO attainment check
    if entry.slo_type:
        met = True
        if entry.slo_type == "ttft" and entry.deadline_ttft and entry.actual_ttft is not None:
            if entry.arrival_ts + entry.actual_ttft > entry.deadline_ttft:
                met = False
        elif entry.slo_type == "e2e" and entry.deadline_e2e and entry.actual_e2e is not None:
            if entry.arrival_ts + entry.actual_e2e > entry.deadline_e2e:
                met = False
        elif entry.slo_type == "ttft+tpot":
            if entry.deadline_ttft and entry.actual_ttft is not None:
                if entry.arrival_ts + entry.actual_ttft > entry.deadline_ttft:
                    met = False

        if met:
            inc_slo_actual_met()
        else:
            inc_slo_actual_miss()

    set_slo_registry_size(_slo_registry.size())


def _ingest_result_payload(payload: dict) -> None:
    """
    Shared ingestion logic for /result and submit-ack result endpoint.
    """
    req_id_raw = payload.get("req_id")
    if req_id_raw is None:
        return

    rid = str(req_id_raw)
    result = payload.get("result")

    # Backward compatibility — sidecar might send: {output, trace}
    if result is None and "output" in payload:
        result = {"output": payload["output"]}
        if "trace" in payload and isinstance(payload["trace"], dict):
            result["trace"] = payload["trace"]

    if result is None:
        return

    endpoint = payload.get("endpoint")

    if endpoint and _push_router is not None:
        try:
            _push_router.notify_result(endpoint)
        except Exception as e:
            print(f"[router] PushRouter notify_result failed for endpoint={endpoint}: {e}")
            sys.stdout.flush()

    # Preserve full trace
    if _cfg.TRACE_ENABLED and isinstance(result, dict):
        tr = result.get("trace") or result.get("__trace__") or {}
        tr = dict(tr)
        tr["t_router_result_recv"] = time.time()
        tr["t_router_result_store"] = time.time()
        result["trace"] = tr
        result.pop("__trace__", None)

    router_state.store_result(rid, result)

    # Feed actuals to SLO registry + prediction error logging
    try:
        _ingest_slo_actuals(rid, result)
    except Exception:
        pass

    # SLO inflight tracking: decrement on result arrival (Step 7)
    if endpoint and _cfg.SLO_AWARE:
        try:
            from .router_state import _batch_estimator, _queue_wait_estimator
            if _batch_estimator is not None:
                _batch_estimator.decrement_inflight(str(endpoint))
            if _queue_wait_estimator is not None:
                _queue_wait_estimator.record_completion(str(endpoint))
        except Exception:
            pass

    # Update latency predictor with observation (Step 3)
    if _cfg.SLO_AWARE:
        try:
            from .latency_predictor import get_latency_predictor, LatencyObservation
            lp = get_latency_predictor()
            if lp is not None and bool(getattr(_cfg, "LATENCY_ONLINE_UPDATE", False)):
                entry = _slo_registry.get(rid)
                if entry and entry.actual_ttft is not None:
                    obs = LatencyObservation(
                        input_tokens=entry.input_tokens,
                        cached_tokens=0,
                        output_tokens=entry.actual_output_len or 0,
                        batch_size=int(getattr(_cfg, "FIXED_BATCH_ESTIMATE", 8)),
                        actual_ttft_s=entry.actual_ttft or 0.0,
                        actual_e2e_s=entry.actual_e2e or 0.0,
                    )
                    lp.update(obs)
        except Exception:
            pass

    # Publish for async_pubsub (best-effort)
    if _publisher is not None:
        try:
            run_id = _pop_run_id(rid)
            pub_payload: Dict[str, Any] = {
                "req_id": rid,
                "result": result,
            }
            if endpoint:
                pub_payload["endpoint"] = endpoint
            if run_id:
                pub_payload["run_id"] = run_id
            _publisher.publish(pub_payload)
        except Exception as e:
            print(f"[router] WARNING: pubsub publish failed for rid={rid}: {e}")
            sys.stdout.flush()
    else:
        _pop_run_id(rid)


# ============================================================
# Startup / Shutdown
# ============================================================

@app.on_event("startup")
async def _startup():
    global _kv_watcher, _push_router, _publisher, _push_dispatcher, _hash_client

    print_config(_cfg)
    sys.stdout.flush()

    _install_submit_route()
    _install_result_submit_route()

    # Long-lived hash client (used by _maybe_register_kv_blocks)
    try:
        t = float(getattr(_cfg, "HASH_TIMEOUT_S", 2.0))
        timeout = httpx.Timeout(connect=t, read=t, write=t, pool=t)
        limits = httpx.Limits(
            max_keepalive_connections=int(getattr(_cfg, "HASH_MAX_KEEPALIVE", 50)),
            max_connections=int(getattr(_cfg, "HASH_MAX_KEEPALIVE", 50)),
            keepalive_expiry=float(getattr(_cfg, "HASH_KEEPALIVE_EXPIRY_S", 30.0)),
        )
        _hash_client = httpx.AsyncClient(timeout=timeout, limits=limits)
    except Exception as e:
        _hash_client = None
        print(f"[router] WARNING: failed to init hash AsyncClient: {e}")
        sys.stdout.flush()

    _kv_watcher = KVWatcher()
    _kv_watcher.start()
    print("[router] KVWatcher started.")
    sys.stdout.flush()

    # Push router (optional)
    if _is_push_mode():
        _push_router = PushRouter(mode=_cfg.ROUTER_MODE)
        print(f"[router] PushRouter started in mode={_cfg.ROUTER_MODE}")

        if _push_decouple_enabled():
            qmax = int(getattr(_cfg, "PUSH_DISPATCH_QUEUE_MAX", 100000))
            workers = int(getattr(_cfg, "PUSH_DISPATCH_WORKERS", 32))
            max_delay_s = float(getattr(_cfg, "PUSH_DISPATCH_MAX_DELAY_S", 60.0))

            _push_dispatcher = _PushDispatcher(
                queue_max=qmax,
                workers=workers,
                max_delay_s=max_delay_s,
            )
            _push_dispatcher.start()
            print(
                f"[router] PushDispatch enabled: workers={workers} "
                f"queue_max={qmax} max_delay_s={max_delay_s}"
            )
        else:
            _push_dispatcher = None
            print("[router] PushDispatch disabled (synchronous push in handlers).")

    else:
        _push_router = None
        _push_dispatcher = None
        print("[router] running in PULL mode.")
    sys.stdout.flush()

    # PubSub publisher (optional)
    if _transport_async_pubsub_enabled():
        try:
            _publisher = ResultPublisher(
                bind=str(_cfg.RESULTS_ZMQ_BIND),
                topic=str(_cfg.RESULTS_ZMQ_TOPIC),
                hwm=int(getattr(_cfg, "RESULTS_ZMQ_HWM", 100000)),
            )
            _publisher.start()
            print(
                f"[router] PubSub enabled: bind={_cfg.RESULTS_ZMQ_BIND} "
                f"topic={_cfg.RESULTS_ZMQ_TOPIC} hwm={getattr(_cfg,'RESULTS_ZMQ_HWM',None)}"
            )
        except Exception as e:
            _publisher = None
            print(f"[router] WARNING: failed to start pubsub publisher: {e}")
        sys.stdout.flush()


@app.on_event("shutdown")
async def _shutdown():
    global _kv_watcher, _push_router, _publisher, _push_dispatcher, _hash_client

    if _kv_watcher:
        _kv_watcher.stop()
        print("[router] KVWatcher stopped.")
        _kv_watcher = None

    if _push_dispatcher is not None:
        try:
            await _push_dispatcher.stop()
        except Exception:
            pass
        _push_dispatcher = None
        print("[router] PushDispatch stopped.")

    if _push_router is not None:
        try:
            await _push_router.aclose()
        except Exception:
            pass
        _push_router = None
        print("[router] PushRouter cleared.")

    if _publisher is not None:
        try:
            _publisher.stop()
        except Exception:
            pass
        _publisher = None
        print("[router] PubSub publisher stopped.")

    if _hash_client is not None:
        try:
            await _hash_client.aclose()
        except Exception:
            pass
        _hash_client = None
        print("[router] Hash client closed.")

    sys.stdout.flush()


# ============================================================
# Health
# ============================================================

@app.get("/health")
async def health():
    extra = {}
    if _push_dispatcher is not None:
        extra["push_dispatch_queue"] = _push_dispatcher.qsize()
    return {"status": "ok", "queue_len": router_state.size(), **extra}


# ============================================================
# SLO Debug
# ============================================================

@app.get("/debug/slo/{req_id}")
async def debug_slo(req_id: str):
    entry = _slo_registry.debug_entry(req_id)
    if entry is None:
        raise HTTPException(404, f"No SLO entry for req_id={req_id}")
    return entry


@app.get("/debug/slo")
async def debug_slo_summary():
    return _slo_registry.debug_summary()


# ============================================================
# Prometheus
# ============================================================

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


# ============================================================
# ASYNC PUBSUB: submit + immediate ack (no waiting)
# ============================================================

@app.post("/submit")
async def submit(req: EnqueueRequest):
    """
    Async submit endpoint:
      - returns immediately with {req_id}
      - completion is delivered via ZMQ PUB (router -> clients), if enabled
    """
    t_start = time.time()
    inc_admission()

    trace = None
    if _cfg.TRACE_ENABLED:
        qlen = router_state.size()
        trace = {
            "t_enq_client": float(req.t_enq_client or t_start),
            "t_arrive_router": t_start,
            "router_queue_len_at_arrive": qlen,
        }

    # Push vs Pull behavior
    if _is_push_mode():
        rid = router_state.next_req_id()
        mode_str = "push"
        meta = req.meta or {}
        is_pull_mode = False
    else:
        t_enq = req.t_enq_client or t_start
        meta = req.meta or {}
        rid = router_state.enqueue(req.prompt, t_enq, meta)
        mode_str = "pull"
        is_pull_mode = True

    _remember_run_id(rid, meta or {})
    _register_slo(rid, req, t_start)

    if trace is not None:
        meta = dict(meta)
        meta.setdefault("__trace__", trace)
        if is_pull_mode:
            router_state.update_meta(rid, meta)

    _log_api_req(
        f"submit rid={rid} mode={mode_str} len={len(req.prompt)} "
        f"kv_aware={_cfg.KV_AWARE} len_aware={_cfg.LEN_AWARE}",
        level="summary",
    )

    router_state.register_waiter(rid)

    if _is_push_mode() and _push_dispatcher is not None:
        ok = _push_dispatcher.try_submit(rid, req.prompt, meta)
        if not ok:
            _store_and_maybe_publish_local_result(
                req_id=rid,
                result={"error": "push_dispatch_queue_full"},
            )
    else:
        meta = await _maybe_register_kv_blocks(
            rid,
            req.prompt,
            meta=meta,
            is_pull_mode=is_pull_mode,
        )

        if _is_push_mode():
            if _push_router is None:
                raise HTTPException(500, "PushRouter not initialized")
            try:
                await _push_router.route_and_push(rid, req.prompt, meta)
            except Exception as e:
                raise HTTPException(503, f"push failed: {e}")

    return Response(
        content=f'{{"req_id":"{rid}"}}',
        media_type="application/json",
        status_code=status.HTTP_202_ACCEPTED,
    )


# ============================================================
# MAIN: enqueue + synchronous wait for result
# ============================================================

@app.post("/enqueue")
async def enqueue(req: EnqueueRequest):
    t_start = time.time()
    inc_admission()

    trace = None
    if _cfg.TRACE_ENABLED:
        qlen = router_state.size()
        trace = {
            "t_enq_client": float(req.t_enq_client or t_start),
            "t_arrive_router": t_start,
            "router_queue_len_at_arrive": qlen,
        }

    if _is_push_mode():
        rid = router_state.next_req_id()
        mode_str = "push"
        meta = req.meta or {}
        is_pull_mode = False
    else:
        t_enq = req.t_enq_client or t_start
        meta = req.meta or {}
        rid = router_state.enqueue(req.prompt, t_enq, meta)
        mode_str = "pull"
        is_pull_mode = True

    _remember_run_id(rid, meta or {})
    _register_slo(rid, req, t_start)

    if trace is not None:
        meta = dict(meta)
        meta.setdefault("__trace__", trace)
        if is_pull_mode:
            router_state.update_meta(rid, meta)

    _log_api_req(
        f"enqueue rid={rid} mode={mode_str} len={len(req.prompt)} "
        f"kv_aware={_cfg.KV_AWARE} len_aware={_cfg.LEN_AWARE}",
        level="summary",
    )

    router_state.register_waiter(rid)

    if _is_push_mode() and _push_dispatcher is not None:
        ok = _push_dispatcher.try_submit(rid, req.prompt, meta)
        if not ok:
            _store_and_maybe_publish_local_result(
                req_id=rid,
                result={"error": "push_dispatch_queue_full"},
            )
    else:
        meta = await _maybe_register_kv_blocks(
            rid,
            req.prompt,
            meta=meta,
            is_pull_mode=is_pull_mode,
        )

        if _is_push_mode():
            if _push_router is None:
                raise HTTPException(500, "PushRouter not initialized")
            try:
                await _push_router.route_and_push(rid, req.prompt, meta)
            except Exception as e:
                raise HTTPException(503, f"push failed: {e}")

    result = await router_state.wait_for_result_async(
        rid,
        _cfg.RESULT_TIMEOUT_S,
    )

    if _cfg.TRACE_ENABLED and isinstance(result, dict):
        tr = result.get("trace") or result.get("__trace__") or {}
        tr = dict(tr)
        tr["t_enqueue_unblocked"] = time.time()
        result["trace"] = tr

    router_latency = time.time() - t_start

    if result is None:
        _log_api_req(
            f"timeout rid={rid} mode={mode_str} after {router_latency:.3f}s",
            level="summary",
        )
        raise HTTPException(504, "timeout waiting for vLLM result")

    if not isinstance(result, dict):
        result = {"output": result}

    if _cfg.TRACE_ENABLED:
        tr = result.get("trace") or result.get("__trace__") or {}
        tr = dict(tr)
        tr["t_enqueue_about_to_return"] = time.time()
        tr["t_enqueue_response"] = time.time()
        result["trace"] = tr
        result.pop("__trace__", None)

    if "endpoint" in result and "pod" not in result:
        result["pod"] = result.pop("endpoint")

    tr = result.get("trace")
    if isinstance(tr, dict) and "endpoint" in tr and "pod" not in tr:
        tr["pod"] = tr.pop("endpoint")
        result["trace"] = tr

    _log_api_req(
        f"complete rid={rid} mode={mode_str} latency={router_latency:.3f}s",
        level="summary",
    )

    return {"req_id": rid, "result": result}


@app.post("/result")
async def result_callback(payload: dict):
    _log_api_req("result callback", level="full")
    _ingest_result_payload(payload)
    return {"status": "ok"}


async def result_submit_ack(payload: dict, background_tasks: BackgroundTasks):
    req_id_raw = payload.get("req_id")
    if req_id_raw is None:
        return {"status": "missing req_id"}

    background_tasks.add_task(_ingest_result_payload, payload)

    return Response(
        content='{"status":"accepted"}',
        media_type="application/json",
        status_code=status.HTTP_202_ACCEPTED,
    )


@app.post("/pull", response_model=PullResponse)
async def pull(req: PullRequest):
    _log_api_req(
        f"/pull endpoint={req.endpoint} want={req.want}",
        level="full",
    )

    items = router_state.pull_for_endpoint(
        endpoint=req.endpoint,
        want=req.want,
    )

    if items:
        ids = [it.req_id for it in items]
        _log_api_req(
            f"/pull ASSIGN endpoint={req.endpoint} want={req.want} "
            f"-> {len(items)} items {ids}",
            level="summary",
        )
    else:
        _log_api_req(
            f"/pull IDLE endpoint={req.endpoint} want={req.want} -> 0 items",
            level="full",
        )

    return PullResponse(items=items)


# ============================================================
# OpenAI-compatible /v1/chat/completions (LiteLLM gateway shim)
#
# Accepts standard OpenAI chat format from LiteLLM proxy.
# Internally reuses the exact same enqueue → wait → result path
# as /enqueue. All existing endpoints (/enqueue, /submit, /pull,
# /result) are completely untouched — fully backward compatible.
# ============================================================

def _messages_to_prompt(messages: List[_ChatMessage]) -> str:
    """
    Flatten OpenAI messages list into a single prompt string.
    Preserves role context so the model sees the conversation structure.
    """
    parts = []
    for msg in messages:
        role = msg.role.strip().lower()
        content = msg.content.strip()
        if role == "system":
            parts.append(f"System: {content}")
        elif role == "user":
            parts.append(f"User: {content}")
        elif role == "assistant":
            parts.append(f"Assistant: {content}")
        else:
            parts.append(content)
    return "\n".join(parts)


@app.post("/v1/chat/completions")
async def openai_chat_completions(req: _ChatCompletionRequest):
    """
    OpenAI-compatible chat completions endpoint.

    Intended for use by the LiteLLM proxy (production auth/spend layer).
    Translates OpenAI chat format into the internal enqueue flow and
    wraps the result back into a standard OpenAI ChatCompletion response.

    This endpoint is NOT used by mu-load-test benchmarks — those continue
    to use /enqueue or /submit directly for zero-overhead measurement.
    """
    t_start = time.time()
    inc_admission()

    # 1. Flatten messages → prompt
    prompt = _messages_to_prompt(req.messages)

    # 2. Enqueue via existing router_state (identical to /enqueue)
    meta: Dict[str, Any] = {"__source__": "litellm"}
    if _is_push_mode():
        rid = router_state.next_req_id()
        is_pull_mode = False
    else:
        rid = router_state.enqueue(prompt, t_start, meta)
        is_pull_mode = True

    _log_api_req(
        f"chat_completions rid={rid} model={req.model} "
        f"messages={len(req.messages)} prompt_len={len(prompt)}",
        level="summary",
    )

    router_state.register_waiter(rid)

    # 3. KV hashing + push dispatch (identical to /enqueue)
    if _is_push_mode() and _push_dispatcher is not None:
        ok = _push_dispatcher.try_submit(rid, prompt, meta)
        if not ok:
            _store_and_maybe_publish_local_result(
                req_id=rid,
                result={"error": "push_dispatch_queue_full"},
            )
    else:
        meta = await _maybe_register_kv_blocks(
            rid,
            prompt,
            meta=meta,
            is_pull_mode=is_pull_mode,
        )
        if _is_push_mode():
            if _push_router is None:
                raise HTTPException(500, "PushRouter not initialized")
            try:
                await _push_router.route_and_push(rid, prompt, meta)
            except Exception as e:
                raise HTTPException(503, f"push failed: {e}")

    # 4. Wait for result (identical to /enqueue)
    result = await router_state.wait_for_result_async(rid, _cfg.RESULT_TIMEOUT_S)

    router_latency = time.time() - t_start

    if result is None:
        _log_api_req(
            f"chat_completions timeout rid={rid} after {router_latency:.3f}s",
            level="summary",
        )
        raise HTTPException(504, "timeout waiting for vLLM result")

    if not isinstance(result, dict):
        result = {"output": result}

    output_text = result.get("output", "")
    finish_reason = result.get("finish_reason", "stop") or "stop"
    usage = result.get("usage") or {}

    _log_api_req(
        f"chat_completions complete rid={rid} latency={router_latency:.3f}s",
        level="summary",
    )

    # 5. Return OpenAI-format response so LiteLLM proxy can parse it normally
    return {
        "id": f"chatcmpl-{rid}",
        "object": "chat.completion",
        "created": int(t_start),
        "model": req.model,
        "choices": [
            {
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": output_text,
                },
                "finish_reason": finish_reason,
            }
        ],
        "usage": {
            "prompt_tokens": usage.get("prompt_tokens", 0),
            "completion_tokens": usage.get("completion_tokens", 0),
            "total_tokens": usage.get("total_tokens", 0),
        },
    }