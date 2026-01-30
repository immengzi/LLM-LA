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
from threading import RLock
from typing import Optional, Dict, Any

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
from .metrics import inc_admission

# Async pubsub publisher (added later as router/pubsub.py)
from .pubsub import ResultPublisher  # type: ignore

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

# Map req_id -> run_id (so results publish can include run_id filtering)
_rid_runid_lock = RLock()
_rid_to_run_id: Dict[str, str] = {}


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


async def _maybe_register_kv_blocks(req_id: str, prompt: str) -> None:
    """Best-effort KV-block computation."""
    if not _cfg.KV_AWARE:
        return

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{_cfg.HASH_SERVICE_URL}/compute_hashes",
                json={"prompt": prompt},
                timeout=_cfg.HASH_TIMEOUT_S,
            )
            resp.raise_for_status()
            data = resp.json()
            block_hashes = data.get("block_hashes") or []
            if block_hashes:
                register_request_blocks(req_id, block_hashes)
    except Exception as e:
        print(f"[router] WARNING: KV hash compute failed for req_id={req_id}: {e}")
        sys.stdout.flush()


def _install_submit_route() -> None:
    """
    Install the /submit endpoint at the configured SUBMIT_PATH.

    We keep a default /submit for convenience, but allow SUBMIT_PATH to be
    changed without editing code (useful behind gateways).
    """
    async def _submit_handler(req: EnqueueRequest):
        return await submit(req)

    submit_path = getattr(_cfg, "SUBMIT_PATH", "/submit") or "/submit"
    if not str(submit_path).startswith("/"):
        submit_path = "/" + str(submit_path)

    if submit_path != "/submit":
        # Avoid duplicate registration if someone sets SUBMIT_PATH="/submit"
        app.add_api_route(
            submit_path,
            _submit_handler,
            methods=["POST"],
            name="submit",
        )


def _install_result_submit_route() -> None:
    """
    Install RESULT_SUBMIT_PATH (default /result_submit) if RESULT_TRANSPORT_MODE=submit_ack.

    Note: we *also* keep /result always available for compatibility.
    """
    async def _result_submit_handler(payload: dict, background_tasks: BackgroundTasks):
        return await result_submit_ack(payload, background_tasks)

    if not _result_transport_submit_ack():
        return

    path = getattr(_cfg, "RESULT_SUBMIT_PATH", "/result_submit") or "/result_submit"
    if not str(path).startswith("/"):
        path = "/" + str(path)

    if path == "/result":
        # Safety: never alias to /result. Users can still configure it, but we refuse to.
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


def _ingest_result_payload(payload: dict) -> None:
    """
    Shared ingestion logic for /result and submit-ack result endpoint.
    This MUST NOT block on network; keep it best-effort.

    Payload format expected:
      {
        "req_id": "...",
        "result": {...},
        optional "endpoint": "...",
        optional "trace": {...}  (legacy)
      }
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

    # Store for sync (/enqueue) and for potential debugging
    router_state.store_result(rid, result)

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
        # Even if pubsub is off, drop run_id tracking to avoid leaks.
        _pop_run_id(rid)


# ============================================================
# Startup / Shutdown
# ============================================================

@app.on_event("startup")
async def _startup():
    global _kv_watcher, _push_router, _publisher

    print_config(_cfg)
    sys.stdout.flush()

    # Dynamic submit path support
    _install_submit_route()

    # Dynamic /result_submit (submit-ack) support
    _install_result_submit_route()

    # KV watcher
    _kv_watcher = KVWatcher()
    _kv_watcher.start()
    print("[router] KVWatcher started.")
    sys.stdout.flush()

    # Push router (optional)
    if _is_push_mode():
        _push_router = PushRouter(mode=_cfg.ROUTER_MODE)
        print(f"[router] PushRouter started in mode={_cfg.ROUTER_MODE}")
    else:
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
    global _kv_watcher, _push_router, _publisher

    if _kv_watcher:
        _kv_watcher.stop()
        print("[router] KVWatcher stopped.")

    _push_router = None
    print("[router] PushRouter cleared.")

    if _publisher is not None:
        try:
            _publisher.stop()
        except Exception:
            pass
        _publisher = None
        print("[router] PubSub publisher stopped.")

    sys.stdout.flush()


# ============================================================
# Health
# ============================================================

@app.get("/health")
async def health():
    return {"status": "ok", "queue_len": router_state.size()}


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
      - DOES NOT change the existing /enqueue semantics (full backward compatibility)

    This endpoint is always available, but is only *useful* when:
      TRANSPORT_MODE=async_pubsub
    """
    t_start = time.time()
    inc_admission()

    # Construct trace skeleton (same as /enqueue)
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
    else:
        t_enq = req.t_enq_client or t_start
        meta = req.meta or {}
        rid = router_state.enqueue(req.prompt, t_enq, meta)
        mode_str = "pull"

    # Remember run_id for pubsub filtering (best-effort)
    _remember_run_id(rid, meta or {})

    # Inject trace into meta if enabled (and store into queue meta in pull mode)
    if trace is not None:
        meta = dict(meta)
        meta.setdefault("__trace__", trace)
        if not _is_push_mode():
            router_state.update_meta(rid, meta)

    _log_api_req(
        f"submit rid={rid} mode={mode_str} len={len(req.prompt)} "
        f"kv_aware={_cfg.KV_AWARE} len_aware={_cfg.LEN_AWARE}",
        level="summary",
    )

    # For consistency, allow /result to arrive before a sync waiter exists.
    router_state.register_waiter(rid)

    # KV hashing (best-effort)
    await _maybe_register_kv_blocks(rid, req.prompt)

    # Push-mode dispatch now (still async, but we don't wait for result)
    if _is_push_mode():
        if _push_router is None:
            raise HTTPException(500, "PushRouter not initialized")
        try:
            await _push_router.route_and_push(rid, req.prompt, meta)
        except Exception as e:
            raise HTTPException(503, f"push failed: {e}")

    # ACK immediately
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
    """
    Synchronous enqueue with tracing support.
    """
    t_start = time.time()
    inc_admission()

    # Construct trace skeleton
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
    else:
        t_enq = req.t_enq_client or t_start
        meta = req.meta or {}
        rid = router_state.enqueue(req.prompt, t_enq, meta)
        mode_str = "pull"

    _remember_run_id(rid, meta or {})

    # Inject trace into meta if enabled
    if trace is not None:
        meta = dict(meta)
        meta.setdefault("__trace__", trace)
        if not _is_push_mode():
            router_state.update_meta(rid, meta)

    _log_api_req(
        f"enqueue rid={rid} mode={mode_str} len={len(req.prompt)} "
        f"kv_aware={_cfg.KV_AWARE} len_aware={_cfg.LEN_AWARE}",
        level="summary",
    )

    router_state.register_waiter(rid)

    # KV hashing
    await _maybe_register_kv_blocks(rid, req.prompt)

    # Push-mode dispatch
    if _is_push_mode():
        if _push_router is None:
            raise HTTPException(500, "PushRouter not initialized")
        try:
            await _push_router.route_and_push(rid, req.prompt, meta)
        except Exception as e:
            raise HTTPException(503, f"push failed: {e}")

    # Wait for result (async)
    result = await router_state.wait_for_result_async(
        rid,
        _cfg.RESULT_TIMEOUT_S,
    )

    # Trace: unblock
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

    # Merge router final timestamps into trace
    if _cfg.TRACE_ENABLED:
        tr = result.get("trace") or result.get("__trace__") or {}
        tr = dict(tr)
        tr["t_enqueue_about_to_return"] = time.time()
        tr["t_enqueue_response"] = time.time()
        result["trace"] = tr
        result.pop("__trace__", None)

    # RESPONSE-ONLY rename endpoint -> pod
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


# ============================================================
# SIDE CAR → ROUTER RESULT CALLBACK (SYNC/COMPAT)
# ============================================================

@app.post("/result")
async def result_callback(payload: dict):
    """
    Backward-compatible result ingestion.
    """
    _log_api_req("result callback", level="full")
    _ingest_result_payload(payload)
    return {"status": "ok"}


# ============================================================
# SIDE CAR → ROUTER RESULT SUBMIT (ACK IMMEDIATELY)
# ============================================================

async def result_submit_ack(payload: dict, background_tasks: BackgroundTasks):
    """
    New: sidecar submits results, router ACKs immediately (202),
    and ingestion runs in a background task.

    Enabled when:
      RESULT_TRANSPORT_MODE=submit_ack
    Exposed at:
      RESULT_SUBMIT_PATH (default /result_submit)
    """
    req_id_raw = payload.get("req_id")
    if req_id_raw is None:
        return {"status": "missing req_id"}

    background_tasks.add_task(_ingest_result_payload, payload)

    return Response(
        content='{"status":"accepted"}',
        media_type="application/json",
        status_code=status.HTTP_202_ACCEPTED,
    )


# ============================================================
# SIDE CAR → ROUTER /pull
# ============================================================

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
