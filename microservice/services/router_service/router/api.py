# -*- coding: utf-8 -*-
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

import httpx
import asyncio
import time
import sys

from .config import get_config, print_config
from .models import (
    EnqueueRequest,
    EnqueueResponse,
    PullRequest,
    PullResponse,
)
from .router_state import router_state
from .kv_watcher import KVWatcher
from .kv_aware import register_request_blocks
from .push_router import PushRouter

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


# ============================================================
# Helpers
# ============================================================

def _is_push_mode() -> bool:
    """Return True if router is running in a push-* mode."""
    return _cfg.ROUTER_MODE.startswith("push-")


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


# ============================================================
# Startup / Shutdown
# ============================================================

@app.on_event("startup")
async def _startup():
    global _kv_watcher, _push_router

    print_config(_cfg)
    sys.stdout.flush()

    _kv_watcher = KVWatcher()
    _kv_watcher.start()
    print("[router] KVWatcher started.")
    sys.stdout.flush()

    if _is_push_mode():
        _push_router = PushRouter(mode=_cfg.ROUTER_MODE)
        print(f"[router] PushRouter started in mode={_cfg.ROUTER_MODE}")
    else:
        print("[router] running in PULL mode.")

    sys.stdout.flush()


@app.on_event("shutdown")
async def _shutdown():
    global _kv_watcher, _push_router

    if _kv_watcher:
        _kv_watcher.stop()
        print("[router] KVWatcher stopped.")

    _push_router = None
    print("[router] PushRouter cleared.")
    sys.stdout.flush()


# ============================================================
# Health
# ============================================================

@app.get("/health")
async def health():
    return {"status": "ok", "queue_len": router_state.size()}


# ============================================================
# MAIN: enqueue + synchronous wait for result
# ============================================================

@app.post("/enqueue")
async def enqueue(req: EnqueueRequest):
    """
    Synchronous enqueue with tracing support.
    """
    t_start = time.time()

    # -------------------------
    # Construct trace skeleton
    # -------------------------
    trace = None
    if _cfg.TRACE_ENABLED:
        trace = {
            "t_enq_client": float(req.t_enq_client or t_start),
            "t_arrive_router": t_start,
        }

    # -------------------------
    # Push vs Pull behavior
    # -------------------------
    if _is_push_mode():
        rid = router_state.next_req_id()
        mode_str = "push"
        meta = req.meta or {}
    else:
        t_enq = req.t_enq_client or t_start
        meta = req.meta or {}
        rid = router_state.enqueue(req.prompt, t_enq, meta)
        mode_str = "pull"

    # Inject trace into meta if enabled
    if trace is not None:
        meta = dict(meta)
        meta.setdefault("__trace__", trace)
        # Update stored queue meta for pull-mode
        if not _is_push_mode():
            router_state.update_meta(rid, meta)

    _log_api_req(
        f"enqueue rid={rid} mode={mode_str} "
        f"len={len(req.prompt)} kv_aware={_cfg.KV_AWARE} len_aware={_cfg.LEN_AWARE}",
        level="summary",
    )

    router_state.register_waiter(rid)

    # -------------------------
    # KV-aware hashing
    # -------------------------
    await _maybe_register_kv_blocks(rid, req.prompt)

    # -------------------------
    # Push-mode dispatch
    # -------------------------
    if _is_push_mode():
        if _push_router is None:
            raise HTTPException(500, "PushRouter not initialized")

        try:
            await _push_router.route_and_push(rid, req.prompt, meta)
        except Exception as e:
            raise HTTPException(503, f"push failed: {e}")

    # -------------------------
    # Wait for result
    # -------------------------
    result = await asyncio.to_thread(
        router_state.wait_for_result,
        rid,
        _cfg.RESULT_TIMEOUT_S,
    )

    latency = time.time() - t_start

    if result is None:
        _log_api_req(
            f"timeout rid={rid} mode={mode_str} after {latency:.3f}s",
            level="summary",
        )
        raise HTTPException(504, "timeout waiting for vLLM result")

    # -------------------------
    # Merge router final timestamp
    # -------------------------
    if _cfg.TRACE_ENABLED and isinstance(result, dict):
        tr = result.get("trace") or result.get("__trace__") or {}
        tr = dict(tr)
        tr["t_enqueue_response"] = time.time()
        result["trace"] = tr
        result.pop("__trace__", None)

    _log_api_req(
        f"complete rid={rid} mode={mode_str} latency={latency:.3f}s",
        level="summary",
    )

    return {"req_id": rid, "result": result}


# ============================================================
# SIDE CAR → ROUTER RESULT CALLBACK
# ============================================================

@app.post("/result")
async def result_callback(payload: dict):
    req_id_raw = payload.get("req_id")
    if req_id_raw is None:
        return {"status": "missing req_id"}

    rid = str(req_id_raw)

    result = payload.get("result")

    # Sidecar simple shape: {req_id, output, trace}
    if result is None and "output" in payload:
        result = {"output": payload["output"]}
        # ★ preserve sidecar trace ★
        if "trace" in payload and isinstance(payload["trace"], dict):
            result["trace"] = payload["trace"]

    if result is None:
        return {"status": "missing result"}

    endpoint = payload.get("endpoint")

    _log_api_req(f"result callback rid={rid}", level="full")

    if endpoint and _push_router is not None:
        try:
            _push_router.notify_result(endpoint)
        except Exception as e:
            print(f"[router] PushRouter notify_result failed for endpoint={endpoint}: {e}")
            sys.stdout.flush()

    if _cfg.TRACE_ENABLED and isinstance(result, dict):
        tr = result.get("trace") or result.get("__trace__") or {}
        tr = dict(tr)
        tr["t_router_result_recv"] = time.time()
        result["trace"] = tr
        result.pop("__trace__", None)

    router_state.store_result(rid, result)
    return {"status": "ok"}


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
