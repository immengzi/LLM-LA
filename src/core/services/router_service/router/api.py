# router/api.py
# -*- coding: utf-8 -*-
_API_VERSION = "2026-05-13-streaming-endpoint-id"

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, StreamingResponse
from fastapi import status
from fastapi import BackgroundTasks

import collections
import httpx
import json
import logging
import os
import re
import time
import sys
import asyncio
from threading import RLock
from typing import Optional, Dict, Any, List, Tuple

from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from .config import get_config, print_config, get_model_registry
from .models import (
    EnqueueRequest,  # required (used by /enqueue and /submit handler type)
    PullRequest,
    PullResponse,
)
from .router_state import router_state
from .kv_watcher import KVWatcher
from .kv_aware import register_request_blocks, pop_routing, set_request_owners, drop_request
from . import owner_lookup
from .affinity import derive_affinity_key
from .push_router import PushRouter
from .metrics import (
    inc_admission,
    observe_output_len_error,
    observe_ttft_prediction_error,
    observe_e2e_prediction_error,
    inc_slo_actual_miss,
    inc_slo_actual_met,
    set_slo_registry_size,
    observe_request_ttft,
    observe_request_tpot_avg,
    observe_request_e2e,
)
from .slo_state import SLORegistry

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

# -------------------------------------------------
# Per-request latency log (ring buffer + logger)
# -------------------------------------------------
_latency_logger = logging.getLogger("router.latency")
_LATENCY_LOG_MAX = 2000
_latency_ring: collections.deque = collections.deque(maxlen=_LATENCY_LOG_MAX)
_latency_ring_lock = RLock()


def _routing_fields(rid: str) -> Dict[str, Any]:
    """Per-request prefix/kv routing fields for the /latency_log ring.

    Pulls the decision captured at dispatch time (independent of the TRACE
    system) and derives the kv-hit counts. Returns an empty dict when no
    routing info was recorded (e.g. /enqueue-only paths).
    """
    info = pop_routing(rid)
    # Release per-request KV state (block hashes + owner cache) at completion.
    drop_request(rid)
    if not info:
        return {}
    kv_hits_len = int(info.get("kv_hits_len", 0))
    total_blocks = int(info.get("total_blocks", 0))
    fields: Dict[str, Any] = {
        "kv_hits_len": kv_hits_len,
        "total_blocks": total_blocks,
        "matched_tokens": kv_hits_len * int(_cfg.KV_BLOCK_SIZE),
        "kv_hit": kv_hits_len > 0,
    }
    if info.get("affinity_key") is not None:
        fields["affinity_key"] = info["affinity_key"]
    if "block_hashes" in info:
        fields["block_hashes"] = info["block_hashes"]
    return fields


def _record_latency(entry: Dict[str, Any]) -> None:
    """Append to ring buffer and emit structured log line."""
    with _latency_ring_lock:
        _latency_ring.append(entry)
    _latency_logger.info(json.dumps(entry, default=str))


def _truncate_body_for_log(obj: Any) -> Any:
    """Return obj as-is when it serializes within the configured byte cap, else
    a bounded marker. Cap of 0 (or negative) means unlimited. Mirrors the
    client-side _truncate_body so logs.json bodies have a consistent shape."""
    max_bytes = int(getattr(_cfg, "ROUTER_LOG_REQUEST_BODY_MAX_BYTES", 0) or 0)
    try:
        s = json.dumps(obj, default=str, ensure_ascii=False)
    except Exception:
        s = str(obj)
    if max_bytes > 0:
        encoded = s.encode("utf-8")
        if len(encoded) > max_bytes:
            return {"_truncated": True, "bytes": len(encoded), "preview": s[:max_bytes]}
    return obj


def _request_body_field(body: Any) -> Dict[str, Any]:
    """Return {"request_body": <truncated>} when ROUTER_LOG_REQUEST_BODY is on
    and a body is available; otherwise {} so callers can splat it into the ring
    entry with **_request_body_field(...)."""
    if not getattr(_cfg, "ROUTER_LOG_REQUEST_BODY", False):
        return {}
    if body is None:
        return {}
    return {"request_body": _truncate_body_for_log(body)}

# ============================================================
# Pydantic models for OpenAI-compatible /v1/chat/completions
# ============================================================
from pydantic import BaseModel, validator
from typing import Union

class _ChatMessage(BaseModel):
    role: str
    content: Union[str, List[Any], None] = None

    class Config:
        extra = "allow"

    @validator("content", pre=True)
    def _normalise_content(cls, v):
        """Accept both plain strings and OpenAI/Anthropic content-block arrays."""
        if v is None:
            return None
        if isinstance(v, str):
            return v
        if isinstance(v, list):
            parts = []
            for block in v:
                if isinstance(block, str):
                    parts.append(block)
                elif isinstance(block, dict) and block.get("type") == "text":
                    parts.append(block.get("text", ""))
            return "\n".join(parts) if parts else ""
        return str(v)

class _ChatCompletionRequest(BaseModel):
    model: str = "served-model"
    messages: List[_ChatMessage]
    max_tokens: Optional[int] = None
    temperature: Optional[float] = None
    stream: Optional[bool] = False
    stream_options: Optional[Dict[str, Any]] = None
    tools: Optional[List[Any]] = None

    class Config:
        extra = "allow"
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

# Central-push dispatcher (only started in central-push mode). Imported lazily
# to avoid a hard import cycle at module load.
_central_push_dispatcher: Optional[Any] = None

# External-push components (only started in external-push mode): static endpoint
# registry, direct-vLLM delivery client, router-side KV subscriber pool, and the
# dispatcher. All None outside external-push.
_external_registry: Optional[Any] = None
_external_client: Optional[Any] = None
_external_kv_subs: Optional[Any] = None
_external_push_dispatcher: Optional[Any] = None


def _kick_central_push() -> None:
    """Nudge the central-push dispatcher to run a dispatch pass now (coalesced).

    No-op unless central-push mode is active and the dispatcher is running.
    """
    if _central_push_dispatcher is not None:
        try:
            _central_push_dispatcher.kick()
        except Exception:
            pass


def _kick_dispatch() -> None:
    """Nudge whichever router-driven dispatcher is active (central-push or
    external-push). No-op in pull / push-* modes."""
    if _central_push_dispatcher is not None:
        try:
            _central_push_dispatcher.kick()
        except Exception:
            pass
    if _external_push_dispatcher is not None:
        try:
            _external_push_dispatcher.kick()
        except Exception:
            pass


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
    """Return True if router is running a push-* mode (queue-less push)."""
    return str(_cfg.ROUTER_MODE).startswith("push-")


def _is_central_push() -> bool:
    """Return True if router is running the central-push mode."""
    return str(_cfg.ROUTER_MODE) == "central-push"


def _is_external_push() -> bool:
    """Return True if router is running the external-push mode (static external
    vLLM endpoints, no k8s pods, no sidecar; router delivers directly)."""
    return str(_cfg.ROUTER_MODE) == "external-push"


def _is_central_push_direct() -> bool:
    """True for sidecar-less central-push: ROUTER_MODE=central-push with
    ROUTER_SIDECAR_ENABLED=false. Same central-queue scheduling as central-push,
    but delivery goes DIRECTLY to each k8s-discovered vLLM (no sidecar), reusing
    the external-push direct-delivery dispatcher over a k8s-backed registry."""
    return _is_central_push() and not bool(getattr(_cfg, "ROUTER_SIDECAR_ENABLED", True))


def _is_push_direct() -> bool:
    """True for sidecar-less queue-less push: ROUTER_MODE starts with push- and
    ROUTER_SIDECAR_ENABLED=false. Selection still uses PushRouter; delivery goes
    DIRECTLY to each pod's vLLM OpenAI endpoint (no sidecar /push)."""
    return _is_push_mode() and not bool(getattr(_cfg, "ROUTER_SIDECAR_ENABLED", True))


def _uses_central_queue() -> bool:
    """True when requests are admitted into the central queue (pull scheduling
    path): pull, central-push and external-push. Push-* skip the queue."""
    return (not _is_push_mode())


def _uses_direct_delivery() -> bool:
    """True when the router delivers via the central-queue ExternalPushDispatcher
    (no sidecar): external-push and sidecar-less central-push. Queue-less
    push-* direct delivery is handled by PushRouter itself (see _is_push_direct)."""
    return _is_external_push() or _is_central_push_direct()


def _uses_push_delivery() -> bool:
    """True when PushRouter is needed for pod discovery + delivery: all push-*
    modes (sidecar or direct) and sidecar-backed central-push. External-push and
    sidecar-less central-push use ExternalPushDispatcher instead."""
    return _is_push_mode() or (_is_central_push() and not _is_central_push_direct())


def _resolve_model(model: str) -> str:
    """
    Resolve the model name for queue routing.
    In multi-model mode, validates against the registry and raises 404 for unknown models.
    In single-model mode, always returns the default MODEL_NAME.
    """
    registry = get_model_registry()
    if registry is None:
        return _cfg.MODEL_NAME
    m = model.strip() if model else ""
    if not m:
        return _cfg.MODEL_NAME
    if m not in registry:
        raise HTTPException(404, f"Unknown model '{m}'. Available: {sorted(registry.keys())}")
    return m


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


async def _compute_block_hashes_external(
    *,
    prompt: Optional[str],
    messages: Optional[List[Dict[str, Any]]],
) -> List[int]:
    """Call the legacy external hasher (vllm-cpu-hash) over HTTP.

    The legacy service accepts {prompt | messages, block_size}; it does not take
    tools (former behavior preserved). Messages are sent when available, else the
    flat prompt, mirroring the inline input selection.
    """
    payload: Dict[str, Any] = {"block_size": int(_cfg.KV_BLOCK_SIZE)}
    if messages:
        payload["messages"] = messages
    elif prompt is not None:
        payload["prompt"] = prompt
    else:
        return []

    url = f"{_cfg.HASH_SERVICE_URL.rstrip('/')}/compute_hashes"
    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.post(url, json=payload)
        resp.raise_for_status()
        data = resp.json()

    out: List[int] = []
    for x in (data.get("block_hashes") or []):
        try:
            out.append(int(x))
        except (TypeError, ValueError):
            continue
    return out


async def _maybe_register_kv_blocks(
    req_id: str,
    prompt: str,
    *,
    meta: Optional[Dict[str, Any]] = None,
    is_pull_mode: bool,
    messages: Optional[List[Dict[str, Any]]] = None,
    tools: Optional[List[Any]] = None,
    model: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Best-effort inline KV-block computation.

    Side effects:
    - register_request_blocks(req_id, block_hashes)
    - If TRACE_ENABLED, attach router-computed block hashes into meta["__trace__"].
    """
    m: Dict[str, Any] = dict(meta or {})

    # Compute/register prefix blocks when routing needs them (KV_AWARE) OR when
    # measurement-only logging is requested (ROUTER_MEASURE_PREFIX) OR when the
    # full block-hash list is being logged (ROUTER_LOG_BLOCK_HASHES). This lets
    # affinity-only / none strategies still report kv_hits_len/total_blocks
    # without using prefix data for the routing decision.
    _measure_prefix = (
        _cfg.KV_AWARE
        or getattr(_cfg, "ROUTER_MEASURE_PREFIX", False)
        or getattr(_cfg, "ROUTER_LOG_BLOCK_HASHES", False)
    )
    if not _measure_prefix:
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
        # Push decoupling may only pass meta; recover full chat inputs when present.
        if messages is None and isinstance(m.get("__chat_request__"), dict):
            maybe_messages = m["__chat_request__"].get("messages")
            if isinstance(maybe_messages, list):
                messages = maybe_messages
        if tools is None and isinstance(m.get("__chat_request__"), dict):
            maybe_tools = m["__chat_request__"].get("tools")
            if isinstance(maybe_tools, list):
                tools = maybe_tools

        if _cfg.KV_HASH_SOURCE == "external":
            block_hashes = await _compute_block_hashes_external(
                prompt=prompt, messages=messages
            )
            # External hashing yields only full-block hashes; approximate ISL at
            # block granularity (tail partial block dropped, < KV_BLOCK_SIZE).
            _isl_tokens = len(block_hashes) * int(_cfg.KV_BLOCK_SIZE)
            _hash_src = "external"
        else:
            from . import prefix_hash as _ph

            block_hashes, _isl_tokens = _ph.compute_request_block_hashes_with_len(
                messages=messages,
                prompt=prompt if not messages else None,
                tools=tools,
                block_size=int(_cfg.KV_BLOCK_SIZE),
            )
            _hash_src = "inline"

        _log_kv_hash(
            f"req_id={req_id} src={_hash_src} "
            f"took_s={(time.time() - t0):.3f} "
            f"n_hashes={len(block_hashes)}",
            level="full",
        )

        register_request_blocks(req_id, block_hashes)

        # Record the exact input token length (ISL) on meta so token/KV-block
        # budgeted pull sizing and per-endpoint token-load metrics have an exact
        # signal (falls back to block-granular estimate for the external path).
        if _isl_tokens > 0:
            m["__isl_tokens__"] = int(_isl_tokens)

        # Targeted, fresh ownership prefetch: HGETALL exactly this request's
        # block hashes so prefix_len() is exact and eviction-aware. Runs when KV
        # routing is on, or when measuring prefix hits for logging only (so
        # affinity/none strategies still get an exact, eviction-aware kv_hit
        # instead of the stale background-scan approximation). Failures fall back
        # to affinity / the watcher map.
        if (
            (_cfg.KV_AWARE or getattr(_cfg, "ROUTER_MEASURE_PREFIX", False))
            and block_hashes
            and getattr(_cfg, "KV_OWNER_SOURCE", "lookup") == "lookup"
        ):
            try:
                owners = await owner_lookup.fetch_block_owners(block_hashes, model=model)
                set_request_owners(req_id, owners)
            except Exception:
                pass

        if getattr(_cfg, "TRACE_ENABLED", False):
            tr = dict(m.get("__trace__") or {})
            tr["router_block_hashes"] = block_hashes  # ALWAYS set (possibly [])
            tr["isl_tokens"] = int(_isl_tokens)
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
    _slo_registry.register(
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

    # Endpoint identity: sidecars post the pod name as result["endpoint_id"];
    # older/push paths may set a top-level "endpoint". Accept either so the
    # always-on per-endpoint in-flight counter actually decrements (this counter
    # backs pull-mode fairness and central-push capacity).
    endpoint = payload.get("endpoint")
    if not endpoint and isinstance(result, dict):
        endpoint = result.get("endpoint_id")

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

    # Always-on per-endpoint in-flight bookkeeping (pull / central-push),
    # independent of SLO: a result arrived, so this endpoint is serving one fewer
    # request. Idempotent release (pops the req->endpoint map) so a later
    # wait-timeout reconcile can't double-decrement.
    try:
        router_state.release_inflight(rid, str(endpoint) if endpoint else None)
    except Exception:
        pass

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
    global _kv_watcher, _push_router, _publisher, _push_dispatcher, _central_push_dispatcher
    global _external_registry, _external_client, _external_kv_subs, _external_push_dispatcher

    print(f"[router] api.py version={_API_VERSION}")
    print_config(_cfg)
    sys.stdout.flush()

    _need_inline_hash = (
        _cfg.KV_HASH_SOURCE == "inline"
        and (
            _cfg.KV_AWARE
            or getattr(_cfg, "ROUTER_MEASURE_PREFIX", False)
            or getattr(_cfg, "ROUTER_LOG_BLOCK_HASHES", False)
        )
    )
    if _need_inline_hash:
        try:
            from . import prefix_hash as _ph

            _ph.init_tokenizer(_cfg.KV_TOKENIZER_PATH)
            _reason = "KV_AWARE" if _cfg.KV_AWARE else "MEASURE_PREFIX/LOG_BLOCK_HASHES"
            print(
                f"[router] inline hash ready ({_reason}): tokenizer={_cfg.KV_TOKENIZER_PATH} "
                f"block_size={_cfg.KV_BLOCK_SIZE}"
            )
            sys.stdout.flush()
        except Exception as e:
            print(f"[router] FATAL: inline hash tokenizer init failed: {e!r}")
            sys.stdout.flush()
            raise
    elif _cfg.KV_AWARE and _cfg.KV_HASH_SOURCE == "external":
        print(
            f"[router] KV hashing via external service: {_cfg.HASH_SERVICE_URL} "
            f"(legacy path; no in-process tokenizer loaded)"
        )
        sys.stdout.flush()

    _install_submit_route()
    _install_result_submit_route()

    # Reload the durable affinity map from Redis (no-op unless persistence is
    # on). Done before serving so warm mappings are available immediately.
    try:
        router_state.warm_affinity_from_store()
    except Exception as e:
        print(f"[router] WARNING: affinity warm failed: {e!r}")
        sys.stdout.flush()

    # KVWatcher does the legacy k8s-based blind Redis scan. It has no pods to
    # watch in external-push (no k8s), and prefix ownership there comes from the
    # router-side KV subscriber + owner_lookup, so skip it to avoid k8s errors.
    if _is_external_push():
        _kv_watcher = None
        print("[router] KVWatcher skipped (external-push: no k8s pods).")
    else:
        _kv_watcher = KVWatcher()
        _kv_watcher.start()
        print("[router] KVWatcher started.")
    sys.stdout.flush()

    # Targeted per-request block-owner lookup (preferred routing source; also
    # used for exact, eviction-aware kv_hit measurement when only measuring).
    if (
        (getattr(_cfg, "KV_AWARE", False) or getattr(_cfg, "ROUTER_MEASURE_PREFIX", False))
        and getattr(_cfg, "KV_OWNER_SOURCE", "lookup") == "lookup"
    ):
        try:
            await owner_lookup.init_owner_lookup()
            print(
                f"[router] KV owner lookup ready (targeted Redis, "
                f"max_blocks={getattr(_cfg, 'KV_LOOKUP_MAX_BLOCKS', 512)})."
            )
        except Exception as e:
            print(f"[router] WARNING: KV owner lookup init failed: {e!r}")
        sys.stdout.flush()

    # Push router (used by push-* and sidecar-backed central-push for discovery
    # + delivery). Sidecar-less push-* still uses PushRouter (direct-to-vLLM);
    # sidecar-less central-push uses the ExternalPushDispatcher path below.
    if _uses_push_delivery():
        ingest = _ingest_result_payload if _is_push_direct() else None
        _push_router = PushRouter(mode=_cfg.ROUTER_MODE, ingest=ingest)
        print(
            f"[router] PushRouter started in mode={_cfg.ROUTER_MODE}"
            f"{' (sidecar-less direct-to-vLLM)' if _is_push_direct() else ''}"
        )

        # Sidecar-less push-*: host per-pod KV-events subscribers so prefix/
        # both routing keeps working without a sidecar. Reuses K8sVLLMRegistry
        # (same as sidecar-less central-push); affinity/none skip subscribers.
        if _is_push_direct() and bool(getattr(_cfg, "KV_AWARE", False)):
            from .k8s_endpoints import K8sVLLMRegistry

            _external_registry = K8sVLLMRegistry()
            print(
                f"[router] push-* (sidecar-less) KV subscribers for "
                f"{_external_registry.all_ids()}"
            )

        if _is_central_push():
            # Central-push: router-driven dispatch from the central queue.
            # No legacy push-dispatch workers (those are for queue-less push-*).
            from .central_push import CentralPushDispatcher

            cap = int(getattr(_cfg, "CENTRAL_PUSH_CAP", 8))
            interval_s = float(getattr(_cfg, "CENTRAL_PUSH_INTERVAL_S", 0.05))
            _central_push_dispatcher = CentralPushDispatcher(
                router_state,
                _push_router,
                cap=cap,
                interval_s=interval_s,
            )
            _central_push_dispatcher.start()
            _push_dispatcher = None
            print(
                f"[router] CentralPushDispatch started (cap={cap} "
                f"interval_s={interval_s})"
            )
        elif _push_decouple_enabled():
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

    elif _uses_direct_delivery():
        # Direct-to-vLLM delivery (NO sidecar). Two flavors share one dispatcher:
        #   * external-push        -> static external endpoints (ExternalRegistry)
        #   * central-push, sidecar off -> live k8s pods (K8sVLLMRegistry)
        # Both admit via the central queue (pull_for_endpoint scheduling) and
        # deliver directly to each vLLM's OpenAI endpoint, ingesting results
        # inline. The registry is the only difference.
        from .external_endpoints import ExternalVLLMClient
        from .external_push import ExternalPushDispatcher

        if _is_central_push_direct():
            from .k8s_endpoints import K8sVLLMRegistry

            _external_registry = K8sVLLMRegistry()
            # K8sVLLMRegistry owns its per-pod KV subscribers (pods churn), so
            # there is no separate static subscriber pool to start here.
            _external_kv_subs = None
            cap = int(getattr(_cfg, "CENTRAL_PUSH_CAP", 8))
            interval_s = float(getattr(_cfg, "CENTRAL_PUSH_INTERVAL_S", 0.05))
            print(
                f"[router] central-push (sidecar-less) endpoints: "
                f"{_external_registry.all_ids()}"
            )
        else:
            from .external_endpoints import (
                ExternalRegistry,
                RouterKVSubscriberPool,
            )

            _external_registry = ExternalRegistry()
            print(f"[router] external-push endpoints: {_external_registry.all_ids()}")
            # Router-side KV-events subscriber(s) so prefix routing works without
            # a sidecar (no-op when EXTERNAL_KV_EVENTS off or none declared).
            _external_kv_subs = RouterKVSubscriberPool(_external_registry)
            _external_kv_subs.start()
            cap = int(getattr(_cfg, "EXTERNAL_PUSH_CAP", 8))
            interval_s = float(getattr(_cfg, "EXTERNAL_PUSH_INTERVAL_S", 0.05))

        _external_client = ExternalVLLMClient(_external_registry)
        _external_push_dispatcher = ExternalPushDispatcher(
            router_state,
            _external_registry,
            _external_client,
            _ingest_result_payload,
            cap=cap,
            interval_s=interval_s,
        )
        _external_push_dispatcher.start()
        _push_router = None
        _push_dispatcher = None
        print(
            f"[router] {'central-push (sidecar-less)' if _is_central_push_direct() else 'ExternalPushDispatch'} "
            f"started (cap={cap} interval_s={interval_s})"
        )

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
    global _kv_watcher, _push_router, _publisher, _push_dispatcher, _central_push_dispatcher
    global _external_registry, _external_client, _external_kv_subs, _external_push_dispatcher

    if _kv_watcher:
        _kv_watcher.stop()
        print("[router] KVWatcher stopped.")
        _kv_watcher = None

    if _central_push_dispatcher is not None:
        try:
            await _central_push_dispatcher.stop()
        except Exception:
            pass
        _central_push_dispatcher = None
        print("[router] CentralPushDispatch stopped.")

    if _external_push_dispatcher is not None:
        try:
            await _external_push_dispatcher.stop()
        except Exception:
            pass
        _external_push_dispatcher = None
        print("[router] ExternalPushDispatch stopped.")

    if _external_kv_subs is not None:
        try:
            _external_kv_subs.stop()
        except Exception:
            pass
        _external_kv_subs = None

    if _external_client is not None:
        try:
            await _external_client.aclose()
        except Exception:
            pass
        _external_client = None

    if _external_registry is not None:
        try:
            await _external_registry.aclose()
        except Exception:
            pass
        _external_registry = None

    try:
        await owner_lookup.close_owner_lookup()
    except Exception:
        pass

    # Flush + close the durable affinity store (no-op unless persistence is on).
    try:
        router_state.close_affinity_store()
    except Exception:
        pass

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

    sys.stdout.flush()


# ============================================================
# Health
# ============================================================

def _discover_vllm_leaders(cfg) -> Optional[Dict[str, str]]:
    """
    Discover vLLM leader pods (the ones that expose :8200/health).
    Workers in a DataParallel group have role=worker and no API server,
    so they are excluded.  Falls back to all component=vllm pods when
    no role label exists (non-DP deployments).
    """
    try:
        from kubernetes import client as k8s_client, config as k8s_config
        try:
            k8s_config.load_incluster_config()
        except Exception:
            k8s_config.load_kube_config()
        v1 = k8s_client.CoreV1Api()

        # First try leader-only; if zero results fall back to all vLLM pods
        for selector in (
            f"{cfg.LABEL_SELECTOR},role=leader",
            cfg.LABEL_SELECTOR,
        ):
            pod_list = v1.list_namespaced_pod(
                namespace=cfg.NAMESPACE,
                label_selector=selector,
            )
            pods = {
                pod.metadata.name: pod.status.pod_ip
                for pod in pod_list.items
                if pod.status.pod_ip and pod.status.phase == "Running"
            }
            if pods:
                return pods
        return {}
    except Exception:
        return None


@app.get("/health/router")
async def health_router():
    """Router-only health (used by K8s probes). Always 200 if the process is alive."""
    extra = {}
    if _push_dispatcher is not None:
        extra["push_dispatch_queue"] = _push_dispatcher.qsize()
    registry = get_model_registry()
    if registry is not None:
        extra["models"] = sorted(registry.keys())
    return {"status": "ok", "queue_len": router_state.size(), **extra}


@app.get("/health/backends")
async def health_backends():
    """
    Aggregated health for all vLLM pods behind this router.

    Returns HTTP 200 if at least one backend is healthy, HTTP 503 otherwise.
    Response body mirrors vLLM's format with added per-pod detail so BooM
    can treat the entire stack as a single healthy/unhealthy virtual node.
    """
    cfg = get_config()

    # External-push: no k8s pods; report the static external endpoints instead,
    # probing each vLLM /health directly through the registry.
    if _is_external_push() and _external_registry is not None:
        try:
            await _external_registry.refresh_health(force=True)
        except Exception:
            pass
        healthy = set(_external_registry.healthy_ids())
        all_ids = _external_registry.all_ids()
        pod_status = {i: ("healthy" if i in healthy else "unhealthy") for i in all_ids}
        healthy_count = len(healthy)
        body = {
            "status": "healthy" if healthy_count > 0 else "unhealthy",
            "healthy": healthy_count,
            "total": len(all_ids),
            "pods": pod_status,
        }
        return Response(
            content=json.dumps(body),
            status_code=200 if healthy_count > 0 else 503,
            media_type="application/json",
        )

    pods = _discover_vllm_leaders(cfg)
    if pods is None:
        return Response(
            content=json.dumps({"status": "error", "detail": "pod discovery failed"}),
            status_code=503,
            media_type="application/json",
        )

    if not pods:
        return Response(
            content=json.dumps({"status": "unhealthy", "healthy": 0, "total": 0, "pods": {}}),
            status_code=503,
            media_type="application/json",
        )

    # Probe each pod concurrently with a short timeout
    async def _probe(name: str, ip: str) -> Tuple[str, bool]:
        url = f"http://{ip}:{cfg.VLLM_PORT}/health"
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                r = await client.get(url)
                return (name, r.status_code == 200)
        except Exception:
            return (name, False)

    results = await asyncio.gather(*[_probe(n, ip) for n, ip in pods.items()])
    pod_status = {name: "healthy" if ok else "unhealthy" for name, ok in results}
    healthy_count = sum(1 for _, ok in results if ok)

    body = {
        "status": "healthy" if healthy_count > 0 else "unhealthy",
        "healthy": healthy_count,
        "total": len(results),
        "pods": pod_status,
    }

    code = 200 if healthy_count > 0 else 503
    return Response(
        content=json.dumps(body),
        status_code=code,
        media_type="application/json",
    )


@app.get("/health")
async def health_aggregated():
    """
    Aggregated health: router + all vLLM backends.

    Returns HTTP 200 if the router is up AND at least one backend is healthy.
    Returns HTTP 503 if no backends are reachable.
    BooM gateway should use this as the virtual-node health endpoint.
    """
    # Router health
    router_info: Dict[str, Any] = {"status": "ok", "queue_len": router_state.size()}
    registry = get_model_registry()
    if registry is not None:
        router_info["models"] = sorted(registry.keys())

    # Backend health (leaders only -- workers don't expose :8200)
    cfg = get_config()

    # External-push: probe the static external endpoints via the registry.
    if _is_external_push() and _external_registry is not None:
        try:
            await _external_registry.refresh_health(force=True)
        except Exception:
            pass
        healthy = set(_external_registry.healthy_ids())
        all_ids = _external_registry.all_ids()
        pod_status = {i: ("healthy" if i in healthy else "unhealthy") for i in all_ids}
        healthy_count = len(healthy)
        overall = "healthy" if healthy_count > 0 else "unhealthy"
        body = {
            "status": overall,
            "router": router_info,
            "backends": {
                "healthy": healthy_count,
                "total": len(all_ids),
                "pods": pod_status,
            },
        }
        return Response(
            content=json.dumps(body),
            status_code=200 if healthy_count > 0 else 503,
            media_type="application/json",
        )

    pods = _discover_vllm_leaders(cfg) or {}

    healthy_count = 0
    pod_status: Dict[str, str] = {}
    if pods:
        async def _probe(name: str, ip: str) -> Tuple[str, bool]:
            url = f"http://{ip}:{cfg.VLLM_PORT}/health"
            try:
                async with httpx.AsyncClient(timeout=3.0) as client:
                    r = await client.get(url)
                    return (name, r.status_code == 200)
            except Exception:
                return (name, False)

        results = await asyncio.gather(*[_probe(n, ip) for n, ip in pods.items()])
        pod_status = {name: "healthy" if ok else "unhealthy" for name, ok in results}
        healthy_count = sum(1 for _, ok in results if ok)

    overall = "healthy" if healthy_count > 0 else "unhealthy"
    code = 200 if healthy_count > 0 else 503

    body = {
        "status": overall,
        "router": router_info,
        "backends": {
            "healthy": healthy_count,
            "total": len(pods),
            "pods": pod_status,
        },
    }

    return Response(
        content=json.dumps(body),
        status_code=code,
        media_type="application/json",
    )


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


@app.get("/latency_log")
def latency_log(last: int = 100):
    """Return the most recent per-request latency records as JSON.

    Query params:
        last: number of records to return (default 100, max 2000)

    Works without Loki/Grafana -- just curl http://<router>:port/latency_log
    """
    n = min(max(1, last), _LATENCY_LOG_MAX)
    with _latency_ring_lock:
        records = list(_latency_ring)
    return records[-n:]


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
    model = _resolve_model(req.model)

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
        meta = _inject_affinity(req.meta or {}, model)
        is_pull_mode = False
    else:
        t_enq = req.t_enq_client or t_start
        meta = _inject_affinity(req.meta or {}, model)
        rid = router_state.enqueue(req.prompt, t_enq, meta, model=model)
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

    _kick_dispatch()

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
    model = _resolve_model(req.model)

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
        meta = _inject_affinity(req.meta or {}, model)
        is_pull_mode = False
    else:
        t_enq = req.t_enq_client or t_start
        meta = _inject_affinity(req.meta or {}, model)
        rid = router_state.enqueue(req.prompt, t_enq, meta, model=model)
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

    _kick_dispatch()

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


@app.post("/result_chunk")
async def result_chunk_callback(payload: dict):
    """
    Accept an SSE chunk from the sidecar for real-time streaming.

    Payload:
      {req_id, chunk_idx, delta, tool_calls?, is_final, finish_reason?, usage?}

    `delta` remains the backwards-compatible text delta field. `tool_calls`
    optionally carries the raw OpenAI delta.tool_calls array from the vLLM
    stream so upstream gateways can reconstruct tool-use SSE.

    Pushes to the asyncio.Queue registered for this req_id.
    If no queue exists, the chunk is silently dropped (non-streaming request).
    """
    req_id = payload.get("req_id")
    if not req_id:
        return {"status": "missing req_id"}

    pushed = router_state.push_chunk(str(req_id), payload)
    if not pushed:
        _log_api_req(
            f"result_chunk: no queue for req_id={req_id} (non-streaming?)",
            level="full",
        )
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
    model = _resolve_model(req.model)

    _log_api_req(
        f"/pull endpoint={req.endpoint} want={req.want} model={model}",
        level="full",
    )

    if req.kv_usage is not None:
        try:
            router_state.record_kv_usage(req.endpoint, req.kv_usage)
        except Exception:
            pass

    items = router_state.pull_for_endpoint(
        endpoint=req.endpoint,
        want=req.want,
        model=model,
        want_prefill_tokens=int(getattr(req, "want_prefill_tokens", 0) or 0),
    )

    if items:
        ids = [it.req_id for it in items]
        _log_api_req(
            f"/pull ASSIGN endpoint={req.endpoint} want={req.want} model={model} "
            f"-> {len(items)} items {ids}",
            level="summary",
        )
    else:
        _log_api_req(
            f"/pull IDLE endpoint={req.endpoint} want={req.want} model={model} -> 0 items",
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

# Claude Code prepends a standalone system text block that begins with
#   x-anthropic-billing-header: cc_version=<ver>; cch=<random>; cc_entrypoint=<...>;
# BOTH the rotating tail of cc_version AND the per-request cch counter change on
# every request, so byte-exact KV-cache prefix matching misses 100% of the time.
# The previous narrow regex only removed the `; cch=<hex>` fragment and left the
# rotating cc_version behind, so it never actually restored cross-request cache
# hits. We now drop the ENTIRE attribution block, matching BooM Gateway's
# rewrite::strip_cc_attribution_anthropic and vLLM PR #36829.
_ATTRIBUTION_PREFIX = "x-anthropic-billing-header"
# String-form system content: remove the whole attribution line.
_ATTRIBUTION_LINE_RE = re.compile(
    r"^[ \t]*x-anthropic-billing-header:[^\n]*\n?", re.MULTILINE
)
# Backward-compat fallback: legacy fragment removal for any residual `cch=`
# embedded mid-text (e.g. block not at the start of a line).
_CCH_RE = re.compile(r"; cch=[0-9a-f]+")


def _is_attribution_text(text: str) -> bool:
    """True if a text block is Claude Code's injected attribution block."""
    return text.startswith(_ATTRIBUTION_PREFIX)


def _strip_cch_inplace(req: "_ChatCompletionRequest") -> None:
    """Strip Claude Code's `x-anthropic-billing-header` attribution block.

    Claude Code injects a standalone system text block:
        x-anthropic-billing-header: cc_version=...; cch=<random>; cc_entrypoint=...;
    whose rotating cc_version tail and per-request cch counter invalidate
    byte-exact KV-cache prefix matching on every request. We drop the entire
    block (list form) or line (string form), mirroring BooM Gateway's
    strip_cc_attribution_anthropic and vLLM PR #36829.

    Router-side client compatibility seam, gated by `STRIP_CCH=1`. Intentionally
    does not modify BooM Gateway.
    """
    for msg in req.messages:
        if msg.role.strip().lower() != "system":
            continue
        c = msg.content
        if isinstance(c, str):
            # Remove the whole attribution line, then scrub any residual
            # fragment for safety.
            new_c = _ATTRIBUTION_LINE_RE.sub("", c)
            new_c = _CCH_RE.sub("", new_c)
            if new_c != c:
                msg.content = new_c
        elif isinstance(c, list):
            # Drop entire text parts that are the attribution block.
            new_parts = [
                part
                for part in c
                if not (
                    isinstance(part, dict)
                    and part.get("type") == "text"
                    and _is_attribution_text(part.get("text", ""))
                )
            ]
            if len(new_parts) != len(c):
                msg.content = new_parts
            # Scrub any residual fragment left in surviving text parts.
            for part in msg.content:
                if isinstance(part, dict) and part.get("type") == "text":
                    t = part.get("text", "")
                    nt = _CCH_RE.sub("", t)
                    if nt != t:
                        part["text"] = nt


def _messages_to_prompt(messages: List[_ChatMessage]) -> str:
    """
    Flatten OpenAI messages list into a single prompt string.
    Used for KV-hash computation; tool-calling requests also pass the
    full request body via meta so the sidecar can forward it to vLLM.
    """
    parts = []
    for msg in messages:
        role = msg.role.strip().lower()
        content = (msg.content or "").strip()
        if role == "system":
            parts.append(f"System: {content}")
        elif role == "user":
            parts.append(f"User: {content}")
        elif role == "assistant":
            parts.append(f"Assistant: {content}")
        elif role == "tool":
            parts.append(f"Tool: {content}")
        else:
            parts.append(content)
    return "\n".join(parts)


async def _enqueue_and_wait(
    prompt: str,
    model: str,
    source: str = "litellm",
    chat_request_body: Optional[Dict[str, Any]] = None,
    chat_messages: Optional[List[Dict[str, Any]]] = None,
    chat_tools: Optional[List[Any]] = None,
) -> tuple:
    """Shared enqueue-wait logic for both streaming and non-streaming chat completions."""
    t_start = time.time()
    inc_admission()
    resolved_model = _resolve_model(model)

    meta: Dict[str, Any] = {"__source__": source}
    if chat_request_body is not None:
        meta["__chat_request__"] = chat_request_body
        meta = _inject_affinity(meta, resolved_model, chat_request_body.get("messages"))
    if _is_push_mode():
        rid = router_state.next_req_id()
        is_pull_mode = False
    else:
        rid = router_state.enqueue(prompt, t_start, meta, model=resolved_model)
        is_pull_mode = True

    _log_api_req(
        f"chat_completions rid={rid} model={model} prompt_len={len(prompt)}",
        level="summary",
    )

    router_state.register_waiter(rid)

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
            messages=chat_messages,
            tools=chat_tools,
            model=resolved_model,
        )
        if _is_push_mode():
            if _push_router is None:
                raise HTTPException(500, "PushRouter not initialized")
            try:
                await _push_router.route_and_push(rid, prompt, meta)
            except Exception as e:
                raise HTTPException(503, f"push failed: {e}")

    _kick_dispatch()

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

    _log_api_req(
        f"chat_completions complete rid={rid} latency={router_latency:.3f}s",
        level="summary",
    )

    return rid, t_start, result


def _passthrough_usage(usage: Dict[str, Any]) -> Dict[str, Any]:
    """Forward the entire upstream usage dict, ensuring the three core fields have defaults."""
    out = dict(usage)
    out.setdefault("prompt_tokens", 0)
    out.setdefault("completion_tokens", 0)
    out.setdefault("total_tokens", 0)
    return out


def _build_sse_chunks(
    rid: str,
    model: str,
    created: int,
    output_text: str,
    finish_reason: str,
    usage: Dict[str, Any],
    endpoint_id: Optional[str] = None,
) -> str:
    """
    Build OpenAI-format SSE event stream from a completed response.
    Splits output into word-boundary chunks to simulate incremental delivery.
    """
    chunk_id = f"chatcmpl-{rid}"
    lines: List[str] = []

    preamble = {
        "id": chunk_id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
        "choices": [{"index": 0, "delta": {"role": "assistant", "content": ""}, "finish_reason": None}],
    }
    if endpoint_id:
        preamble["system_fingerprint"] = str(endpoint_id)
    lines.append(f"data: {json.dumps(preamble)}\n\n")

    # Split on whitespace boundaries, preserving the whitespace in front of each word
    # so that concatenating all deltas reproduces the original text exactly.
    tokens: List[str] = []
    buf = ""
    for ch in output_text:
        if ch in (" ", "\n", "\t") and buf:
            tokens.append(buf)
            buf = str(ch)
        else:
            buf += ch
    if buf:
        tokens.append(buf)

    if not tokens:
        tokens = [""]

    for tok in tokens:
        chunk = {
            "id": chunk_id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [{"index": 0, "delta": {"content": tok}, "finish_reason": None}],
        }
        if endpoint_id:
            chunk["system_fingerprint"] = str(endpoint_id)
        lines.append(f"data: {json.dumps(chunk)}\n\n")

    final = {
        "id": chunk_id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
        "choices": [{"index": 0, "delta": {}, "finish_reason": finish_reason}],
        "usage": _passthrough_usage(usage),
    }
    if endpoint_id:
        final["system_fingerprint"] = str(endpoint_id)
    lines.append(f"data: {json.dumps(final)}\n\n")
    lines.append("data: [DONE]\n\n")

    return "".join(lines)


def _check_api_key(request: Request) -> None:
    """Validate API key if one is configured (env API_KEY)."""
    key = _cfg.API_KEY
    if not key:
        return
    auth = request.headers.get("authorization", "")
    if auth.startswith("Bearer "):
        if auth[7:] == key:
            return
    if request.headers.get("api-key", "") == key:
        return
    if request.headers.get("x-api-key", "") == key:
        return
    raise HTTPException(status_code=401, detail="Invalid or missing API key")


def _build_chat_request_body(req: _ChatCompletionRequest) -> Dict[str, Any]:
    """
    Reconstruct the full OpenAI request body from the Pydantic model.
    extra='allow' on both _ChatCompletionRequest and _ChatMessage ensures
    fields like tools, tool_choice, and per-message tool_calls are preserved.
    """
    body = req.dict(exclude_none=True)
    body.pop("stream_options", None)
    return body


def _inject_affinity(
    meta: Dict[str, Any],
    model: str,
    messages: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Stamp the conversation affinity key + router-side timestamp into meta.

    No-op when affinity is disabled. Key precedence: an explicit
    ``meta["affinity_key"]`` (e.g. from a load-test client) wins; otherwise the
    key is auto-derived from the conversation's stable prefix (model + system +
    first user message). The router-stamped timestamp drives the hard-mode hold
    window (skew-free, independent of client clocks).

    Returns the (possibly mutated) meta dict.
    """
    if not _cfg.AFFINITY_ENABLED:
        return meta

    key = None
    explicit = meta.get("affinity_key")
    if explicit:
        key = str(explicit)
    elif messages:
        key = derive_affinity_key(model, messages)

    if key:
        meta["__affinity_key__"] = key
        meta["__affinity_ts__"] = time.time()
        # Read-on-arrival: warm this key from the durable store into the
        # in-memory cache (once per request; no-op unless persistence is on).
        try:
            router_state.affinity_prefetch(key)
        except Exception:
            pass
    return meta


def _extract_tool_calls(result: Dict[str, Any]) -> Optional[List[Any]]:
    """Extract tool_calls from the raw vLLM response if present."""
    direct = result.get("tool_calls")
    if isinstance(direct, list) and direct:
        return direct

    raw = result.get("raw")
    if not isinstance(raw, dict):
        return None
    choices = raw.get("choices")
    if not isinstance(choices, list) or not choices:
        return None
    choice = choices[0]
    if not isinstance(choice, dict):
        return None
    msg = choice.get("message")
    if not isinstance(msg, dict):
        return None
    tc = msg.get("tool_calls")
    if isinstance(tc, list) and tc:
        return tc
    return None


def _build_sse_chunks_with_tool_calls(
    rid: str,
    model: str,
    created: int,
    tool_calls: List[Any],
    finish_reason: str,
    usage: Dict[str, Any],
    endpoint_id: Optional[str] = None,
) -> str:
    """Build OpenAI SSE stream for a tool_calls response."""
    chunk_id = f"chatcmpl-{rid}"
    lines: List[str] = []

    preamble = {
        "id": chunk_id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
        "choices": [{"index": 0, "delta": {"role": "assistant", "content": None, "tool_calls": []}, "finish_reason": None}],
    }
    if endpoint_id:
        preamble["system_fingerprint"] = str(endpoint_id)
    lines.append(f"data: {json.dumps(preamble)}\n\n")

    for i, tc in enumerate(tool_calls):
        chunk = {
            "id": chunk_id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [{"index": 0, "delta": {"tool_calls": [{"index": i, **tc}]}, "finish_reason": None}],
        }
        if endpoint_id:
            chunk["system_fingerprint"] = str(endpoint_id)
        lines.append(f"data: {json.dumps(chunk)}\n\n")

    final = {
        "id": chunk_id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
        "choices": [{"index": 0, "delta": {}, "finish_reason": finish_reason}],
        "usage": _passthrough_usage(usage),
    }
    if endpoint_id:
        final["system_fingerprint"] = str(endpoint_id)
    lines.append(f"data: {json.dumps(final)}\n\n")
    lines.append("data: [DONE]\n\n")
    return "".join(lines)


def _upstream_error_response(result: Any) -> Optional[Response]:
    """Relay a passthrough upstream (vLLM) error to the caller verbatim.

    When the sidecar flags a non-2xx upstream response via
    ``result["upstream_error"]`` (currently 4xx only), return vLLM's original
    status code + body + Content-Type so the gateway sees the real error
    instead of a generic 200 wrapper. Only vLLM's own body is forwarded; no
    internal upstream URLs/headers/IPs are exposed. Returns None when the
    result is a normal (success or non-passthrough) result.
    """
    if not isinstance(result, dict):
        return None
    ue = result.get("upstream_error")
    if not isinstance(ue, dict) or "status" not in ue:
        return None
    try:
        status_code = int(ue["status"])
    except (TypeError, ValueError):
        return None
    body = ue.get("body")
    if not isinstance(body, str):
        try:
            body = json.dumps(body)
        except Exception:
            body = str(body)
    media_type = ue.get("content_type") or "application/json"
    return Response(content=body, status_code=status_code, media_type=str(media_type))


@app.post("/v1/chat/completions")
async def openai_chat_completions(req: _ChatCompletionRequest, request: Request):
    """
    OpenAI-compatible chat completions endpoint.

    Supports both non-streaming (default) and streaming (stream=true) modes.
    Streaming returns standard OpenAI SSE format so upstream gateways (e.g.
    BooM, LiteLLM) can parse it natively.

    When the sidecar has STREAMING_MODE enabled and stream=true is requested,
    chunks arrive via /result_chunk and are forwarded as real SSE events.
    Otherwise, falls back to fake-SSE from the completed result.

    When the request contains tools or tool-role messages, the full request
    body is forwarded through the sidecar pipeline to vLLM so that structured
    tool calling works end-to-end.
    """
    _check_api_key(request)

    if os.environ.get("STRIP_CCH") == "1":
        _strip_cch_inplace(req)

    prompt = _messages_to_prompt(req.messages)

    chat_request_body = _build_chat_request_body(req)
    chat_messages = chat_request_body.get("messages") if isinstance(chat_request_body, dict) else None
    chat_tools = chat_request_body.get("tools") if isinstance(chat_request_body, dict) else None

    # ---- real-time streaming path (Phase 2B) ----
    if req.stream:
        chunk_q = router_state.register_chunk_queue("__pending__")

        t_start = time.time()
        inc_admission()
        resolved_model = _resolve_model(req.model)

        meta: Dict[str, Any] = {"__source__": "litellm"}
        if chat_request_body is not None:
            meta["__chat_request__"] = chat_request_body
            meta = _inject_affinity(meta, resolved_model, chat_request_body.get("messages"))
        if _is_push_mode():
            rid = router_state.next_req_id()
        else:
            rid = router_state.enqueue(prompt, t_start, meta, model=resolved_model)

        router_state.remove_chunk_queue("__pending__")
        chunk_q = router_state.register_chunk_queue(rid)

        _log_api_req(
            f"chat_completions_stream rid={rid} model={req.model} prompt_len={len(prompt)}",
            level="summary",
        )

        router_state.register_waiter(rid)

        if _is_push_mode() and _push_dispatcher is not None:
            ok = _push_dispatcher.try_submit(rid, prompt, meta)
            if not ok:
                _store_and_maybe_publish_local_result(
                    req_id=rid,
                    result={"error": "push_dispatch_queue_full"},
                )
        else:
            meta = await _maybe_register_kv_blocks(
                rid, prompt, meta=meta,
                is_pull_mode=not _is_push_mode(),
                messages=chat_messages,
                tools=chat_tools,
                model=resolved_model,
            )
            if _is_push_mode():
                if _push_router is None:
                    raise HTTPException(500, "PushRouter not initialized")
                try:
                    await _push_router.route_and_push(rid, prompt, meta)
                except Exception as e:
                    raise HTTPException(503, f"push failed: {e}")

        _kick_dispatch()

        chunk_id = f"chatcmpl-{rid}"
        created = int(t_start)

        # Register the full-result fallback FIRST so a completed result
        # (including a non-2xx upstream error from a non-streaming sidecar)
        # is delivered onto chunk_q even when no SSE chunks are produced.
        async def _wait_and_push_fallback():
            """If full result arrives (non-streaming sidecar), push it to chunk_q."""
            result = await router_state.wait_for_result_async(rid, _cfg.RESULT_TIMEOUT_S)
            if result is not None:
                if not isinstance(result, dict):
                    result = {"output": result}
                router_state.push_chunk(rid, {"__full_result__": result})

        asyncio.ensure_future(_wait_and_push_fallback())

        # Peek the first item before committing to an SSE response: if vLLM
        # returned a 4xx before generation started, relay its status + body
        # verbatim instead of opening a 200 event-stream (the HTTP status can no
        # longer change once the stream has started).
        try:
            first_item = await asyncio.wait_for(
                chunk_q.get(), timeout=_cfg.RESULT_TIMEOUT_S
            )
        except asyncio.TimeoutError:
            first_item = None

        if first_item is not None and isinstance(first_item.get("__full_result__"), dict):
            _err = _upstream_error_response(first_item["__full_result__"])
            if _err is not None:
                router_state.remove_chunk_queue(rid)
                return _err

        async def _real_stream_generator(preloaded_first):
            """
            Yield real SSE from /result_chunk, with fallback to fake-SSE
            if the full result arrives before any chunks.
            """
            t_first_chunk: Optional[float] = None

            def _emit_latency_metrics(u: Dict[str, Any], finish_reason: str = "stop") -> Dict[str, Any]:
                t_end = time.time()
                e2e_s = t_end - t_start
                x_lat: Dict[str, Any] = {"e2e_ms": round(e2e_s * 1000, 2)}
                observe_request_e2e(e2e_s, model=req.model)
                if t_first_chunk is not None:
                    ttft_s = t_first_chunk - t_start
                    x_lat["ttft_ms"] = round(ttft_s * 1000, 2)
                    observe_request_ttft(ttft_s, model=req.model)
                    ct = int(u.get("completion_tokens", 0)) if u else 0
                    if ct > 1:
                        decode_s = e2e_s - ttft_s
                        if decode_s > 0:
                            tpot_s = decode_s / (ct - 1)
                            x_lat["tpot_avg_ms"] = round(tpot_s * 1000, 2)
                            observe_request_tpot_avg(tpot_s, model=req.model)
                if stream_endpoint_id:
                    x_lat["endpoint"] = str(stream_endpoint_id)
                _record_latency({
                    "ts": t_end, "t_start": t_start, "rid": rid, "model": req.model,
                    "stream": True, "finish_reason": finish_reason,
                    "prompt_tokens": int(u.get("prompt_tokens", 0)) if u else 0,
                    "completion_tokens": int(u.get("completion_tokens", 0)) if u else 0,
                    **x_lat,
                    **_routing_fields(rid),
                    **_request_body_field(chat_request_body),
                })
                return x_lat

            stream_endpoint_id: Optional[str] = None
            try:
                first = preloaded_first
                if first is None:
                    yield "data: [DONE]\n\n"
                    return

                if first.get("__full_result__"):
                    result = first["__full_result__"]
                    output_text = result.get("output", "")
                    finish_reason = result.get("finish_reason", "stop") or "stop"
                    u = result.get("usage") or {}
                    eid = result.get("endpoint_id")
                    stream_endpoint_id = str(eid) if eid else None
                    t_first_chunk = time.time()
                    _emit_latency_metrics(u, finish_reason=finish_reason)
                    tc = _extract_tool_calls(result)
                    if tc:
                        yield _build_sse_chunks_with_tool_calls(
                            rid=rid, model=req.model, created=created,
                            tool_calls=tc, finish_reason=finish_reason, usage=u,
                            endpoint_id=eid,
                        )
                    else:
                        yield _build_sse_chunks(
                            rid=rid, model=req.model, created=created,
                            output_text=output_text, finish_reason=finish_reason, usage=u,
                            endpoint_id=eid,
                        )
                    return

                t_first_chunk = time.time()
                stream_endpoint_id = first.get("endpoint_id")
                if stream_endpoint_id:
                    stream_endpoint_id = str(stream_endpoint_id)

                preamble = {
                    "id": chunk_id, "object": "chat.completion.chunk",
                    "created": created, "model": req.model,
                    "choices": [{"index": 0, "delta": {"role": "assistant", "content": ""}, "finish_reason": None}],
                }
                if stream_endpoint_id:
                    preamble["system_fingerprint"] = str(stream_endpoint_id)
                yield f"data: {json.dumps(preamble)}\n\n"

                delta_content = first.get("delta", "")
                delta_tool_calls = first.get("tool_calls")
                delta_payload: Dict[str, Any] = {}
                if delta_content:
                    delta_payload["content"] = delta_content
                if isinstance(delta_tool_calls, list) and delta_tool_calls:
                    delta_payload["tool_calls"] = delta_tool_calls
                if delta_payload:
                    c = {
                        "id": chunk_id, "object": "chat.completion.chunk",
                        "created": created, "model": req.model,
                        "choices": [{"index": 0, "delta": delta_payload, "finish_reason": None}],
                    }
                    if stream_endpoint_id:
                        c["system_fingerprint"] = str(stream_endpoint_id)
                    yield f"data: {json.dumps(c)}\n\n"

                if first.get("is_final"):
                    fr = first.get("finish_reason", "stop")
                    u = first.get("usage", {})
                    _emit_latency_metrics(u, finish_reason=fr)
                    final = {
                        "id": chunk_id, "object": "chat.completion.chunk",
                        "created": created, "model": req.model,
                        "choices": [{"index": 0, "delta": {}, "finish_reason": fr}],
                        "usage": u,
                    }
                    if stream_endpoint_id:
                        final["system_fingerprint"] = str(stream_endpoint_id)
                    yield f"data: {json.dumps(final)}\n\n"
                    yield "data: [DONE]\n\n"
                    return

                while True:
                    try:
                        chunk = await asyncio.wait_for(chunk_q.get(), timeout=_cfg.RESULT_TIMEOUT_S)
                    except asyncio.TimeoutError:
                        yield "data: [DONE]\n\n"
                        return

                    if chunk.get("__full_result__"):
                        yield "data: [DONE]\n\n"
                        return

                    eid = chunk.get("endpoint_id")
                    if eid and not stream_endpoint_id:
                        stream_endpoint_id = str(eid)

                    delta_content = chunk.get("delta", "")
                    delta_tool_calls = chunk.get("tool_calls")
                    delta_payload: Dict[str, Any] = {}
                    if delta_content:
                        delta_payload["content"] = delta_content
                    if isinstance(delta_tool_calls, list) and delta_tool_calls:
                        delta_payload["tool_calls"] = delta_tool_calls
                    if delta_payload:
                        c = {
                            "id": chunk_id, "object": "chat.completion.chunk",
                            "created": created, "model": req.model,
                            "choices": [{"index": 0, "delta": delta_payload, "finish_reason": None}],
                        }
                        if stream_endpoint_id:
                            c["system_fingerprint"] = str(stream_endpoint_id)
                        yield f"data: {json.dumps(c)}\n\n"

                    if chunk.get("is_final"):
                        fr = chunk.get("finish_reason", "stop")
                        u = chunk.get("usage", {})
                        _emit_latency_metrics(u, finish_reason=fr)
                        final = {
                            "id": chunk_id, "object": "chat.completion.chunk",
                            "created": created, "model": req.model,
                            "choices": [{"index": 0, "delta": {}, "finish_reason": fr}],
                            "usage": u,
                        }
                        if stream_endpoint_id:
                            final["system_fingerprint"] = str(stream_endpoint_id)
                        yield f"data: {json.dumps(final)}\n\n"
                        yield "data: [DONE]\n\n"
                        return

            finally:
                router_state.remove_chunk_queue(rid)

        return StreamingResponse(
            _real_stream_generator(first_item),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    # --- non-streaming path ---
    rid, t_start, result = await _enqueue_and_wait(
        prompt,
        req.model,
        chat_request_body=chat_request_body,
        chat_messages=chat_messages,
        chat_tools=chat_tools,
    )

    # Passthrough upstream 4xx (e.g. context-length overflow): relay vLLM's
    # real status + body instead of wrapping it in a 200 chat.completion.
    _err = _upstream_error_response(result)
    if _err is not None:
        return _err

    t_done = time.time()
    e2e_s = t_done - t_start

    output_text = result.get("output", "")
    finish_reason = result.get("finish_reason", "stop") or "stop"
    usage = result.get("usage") or {}
    endpoint_id = result.get("endpoint_id")
    tool_calls = _extract_tool_calls(result)

    ttft_s = result.get("ttft_sidecar_s")
    completion_tokens = int(usage.get("completion_tokens", 0))
    tpot_avg_s: Optional[float] = None
    if ttft_s is not None and completion_tokens > 1:
        decode_s = e2e_s - ttft_s
        if decode_s > 0:
            tpot_avg_s = decode_s / (completion_tokens - 1)

    observe_request_e2e(e2e_s, model=req.model)
    if ttft_s is not None:
        observe_request_ttft(ttft_s, model=req.model)
    if tpot_avg_s is not None:
        observe_request_tpot_avg(tpot_avg_s, model=req.model)

    x_latency: Dict[str, Any] = {"e2e_ms": round(e2e_s * 1000, 2)}
    if ttft_s is not None:
        x_latency["ttft_ms"] = round(ttft_s * 1000, 2)
    if tpot_avg_s is not None:
        x_latency["tpot_avg_ms"] = round(tpot_avg_s * 1000, 2)
    if endpoint_id:
        x_latency["endpoint"] = str(endpoint_id)

    _record_latency({
        "ts": time.time(), "t_start": t_start, "rid": rid, "model": req.model,
        "stream": False, "finish_reason": finish_reason,
        "prompt_tokens": int(usage.get("prompt_tokens", 0)),
        "completion_tokens": completion_tokens,
        **x_latency,
        **_routing_fields(rid),
        **_request_body_field(chat_request_body),
    })

    message: Dict[str, Any] = {
        "role": "assistant",
        "content": output_text if not tool_calls else None,
    }
    if tool_calls:
        message["tool_calls"] = tool_calls

    _usage = _passthrough_usage(usage)
    _usage["completion_tokens"] = completion_tokens

    resp_body: Dict[str, Any] = {
        "id": f"chatcmpl-{rid}",
        "object": "chat.completion",
        "created": int(t_start),
        "model": req.model,
        "choices": [
            {
                "index": 0,
                "message": message,
                "finish_reason": finish_reason,
            }
        ],
        "usage": _usage,
    }
    if endpoint_id:
        resp_body["system_fingerprint"] = str(endpoint_id)
    return resp_body
