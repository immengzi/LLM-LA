# router/external_endpoints.py
# -*- coding: utf-8 -*-
"""External (no-sidecar) vLLM endpoints for the ``external-push`` router mode.

Unlike every other mode, external-push does NOT discover Kubernetes pods and
does NOT talk to a per-pod sidecar. Instead the operator registers vLLM servers
that live outside the cluster (by IP/URL) via ``ROUTER_STATIC_ENDPOINTS``. This
module provides three pieces the mode needs:

  * ``ExternalRegistry`` — the static endpoint set + async /health readiness
    gating (replaces k8s pod discovery). Endpoint identity is the configured
    ``id`` string and is used everywhere the pod name would be (affinity,
    in-flight, metrics, KV owners).
  * ``ExternalVLLMClient`` — delivers a single request DIRECTLY to an external
    vLLM OpenAI ``/v1/chat/completions`` endpoint and shapes the response into
    the same ``result`` object the sidecar posts to ``/result`` (non-streaming).
  * ``RouterKVSubscriber`` — optional per-endpoint ZMQ subscriber that mirrors
    the sidecar's KV-events → Redis writer so prefix routing keeps working for
    external endpoints (owners keyed by the endpoint ``id``).
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, NewType

import httpx

from .config import get_config, get_model_registry

_cfg = get_config()


def _log(msg: str, *, level: str = "summary") -> None:
    mode = str(getattr(_cfg, "REQ_LOG_MODE", "off")).lower()
    if mode == "off":
        return
    if level == "summary" or (level == "full" and mode == "full"):
        print(f"[external] {msg}")


def _default_model() -> str:
    """Served model name to use when an endpoint omits ``model``.

    Prefer the single registered model (its key IS the served name) over the
    chart default MODEL_NAME, matching owner_lookup._key_prefix resolution.
    """
    try:
        registry = get_model_registry()
    except Exception:
        registry = None
    if registry and len(registry) == 1:
        return next(iter(registry))
    return _cfg.MODEL_NAME


# ============================================================
# Endpoint model + registry
# ============================================================

@dataclass
class ExternalEndpoint:
    id: str
    url: str                       # vLLM OpenAI base (no trailing /v1)
    model: str
    kv_events_endpoints: List[str] = field(default_factory=list)
    kv_events_topic: str = "kv@"


class ExternalRegistry:
    """Static external endpoint set + async /health readiness gating."""

    def __init__(self, endpoints: Optional[List[ExternalEndpoint]] = None):
        if endpoints is None:
            endpoints = load_external_endpoints()
        self._eps: List[ExternalEndpoint] = list(endpoints)
        self._by_id: Dict[str, ExternalEndpoint] = {e.id: e for e in self._eps}
        # id -> healthy?  (unknown endpoints are treated as healthy until proven
        # otherwise, so a probe outage never strands a live backend).
        self._healthy: Dict[str, bool] = {e.id: True for e in self._eps}
        self._last_probe = 0.0

        t = float(getattr(_cfg, "PUSH_HTTP_TIMEOUT_S", 2.0))
        self._health_client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=t, read=t, write=t, pool=t)
        )

    def all_ids(self) -> List[str]:
        return [e.id for e in self._eps]

    def get(self, ep_id: str) -> Optional[ExternalEndpoint]:
        return self._by_id.get(ep_id)

    def healthy_ids(self) -> List[str]:
        return [e.id for e in self._eps if self._healthy.get(e.id, True)]

    async def aclose(self) -> None:
        try:
            await self._health_client.aclose()
        except Exception:
            pass

    async def refresh_health(self, *, force: bool = False) -> None:
        """Probe each endpoint's vLLM /health, gating dispatch on readiness."""
        interval = float(getattr(_cfg, "EXTERNAL_HEALTH_INTERVAL_S", 5.0))
        now = time.time()
        if not force and interval > 0 and (now - self._last_probe) < interval:
            return
        self._last_probe = now

        async def one(ep: ExternalEndpoint) -> None:
            try:
                r = await self._health_client.get(f"{ep.url}/health")
                self._healthy[ep.id] = bool(r.status_code == 200)
            except Exception:
                self._healthy[ep.id] = False

        import asyncio
        await asyncio.gather(*(one(e) for e in self._eps), return_exceptions=True)


def load_external_endpoints() -> List[ExternalEndpoint]:
    """Build ExternalEndpoint objects from the parsed config, filling defaults."""
    out: List[ExternalEndpoint] = []
    for item in getattr(_cfg, "STATIC_ENDPOINTS_PARSED", []) or []:
        try:
            out.append(
                ExternalEndpoint(
                    id=str(item["id"]),
                    url=str(item["url"]).rstrip("/"),
                    model=str(item.get("model") or "").strip() or _default_model(),
                    kv_events_endpoints=list(item.get("kv_events_endpoints") or []),
                    kv_events_topic=str(item.get("kv_events_topic") or "kv@"),
                )
            )
        except Exception as e:
            _log(f"skipping malformed static endpoint {item!r}: {e}", level="full")
    return out


# ============================================================
# Direct vLLM delivery client (non-streaming)
# ============================================================

class ExternalVLLMClient:
    """Delivers one request directly to an external vLLM OpenAI endpoint.

    Mirrors the sidecar's non-streaming path (sidecar/vllm_client.py) and shapes
    the response into the same ``result`` object the sidecar posts to /result,
    so the router's ``_ingest_result_payload`` handles it unchanged.
    """

    def __init__(self, registry: ExternalRegistry):
        self._registry = registry
        t = float(getattr(_cfg, "EXTERNAL_VLLM_TIMEOUT_S", 300.0))
        # Generous read timeout (full inference); short connect timeout.
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=min(t, 10.0), read=t, write=t, pool=t)
        )

    async def aclose(self) -> None:
        try:
            await self._client.aclose()
        except Exception:
            pass

    def _build_payload(self, ep: ExternalEndpoint, prompt: str, meta: dict) -> Dict[str, Any]:
        chat_req = (meta or {}).get("__chat_request__")
        if isinstance(chat_req, dict) and chat_req:
            payload = dict(chat_req)
            payload["model"] = ep.model
            payload["stream"] = False
        else:
            payload = {
                "model": ep.model,
                "messages": [{"role": "user", "content": str(prompt)}],
                "max_tokens": int((meta or {}).get("max_tokens", 128)),
                "temperature": float((meta or {}).get("temperature", 0.0)),
                "chat_template_kwargs": {
                    "enable_thinking": bool((meta or {}).get("enable_thinking", False)),
                },
            }
            if "min_tokens" in (meta or {}):
                try:
                    payload["min_tokens"] = int(meta["min_tokens"])
                except Exception:
                    pass
        if (meta or {}).get("ignore_eos"):
            payload["ignore_eos"] = True
        # External endpoints never stream to the router in this mode.
        payload["stream"] = False
        return payload

    async def deliver(self, ep_id: str, req_id: str, prompt: str, meta: dict) -> Dict[str, Any]:
        """Call the external vLLM and return a router ``result`` object.

        Always returns a dict (never raises): failures come back as an error
        result so the caller can ingest it and release in-flight.
        """
        ep = self._registry.get(ep_id)
        if ep is None:
            return {"output": f"[external error: unknown endpoint {ep_id}]",
                    "finish_reason": "error", "error": "unknown_endpoint",
                    "endpoint_id": ep_id}

        payload = self._build_payload(ep, prompt, meta)
        url = f"{ep.url}/v1/chat/completions"
        t0 = time.time()
        try:
            resp = await self._client.post(url, json=payload)
        except Exception as e:
            return {"output": f"[external error: {e}]", "finish_reason": "error",
                    "error": str(e), "endpoint_id": ep_id}

        latency_s = time.time() - t0
        result_obj: Dict[str, Any] = {"endpoint_id": ep_id, "latency_s": latency_s}

        if resp.status_code != 200:
            body = ""
            try:
                body = resp.text[:300]
            except Exception:
                pass
            result_obj.update(
                {"output": f"[vLLM error {resp.status_code}]",
                 "finish_reason": "error",
                 "error": f"http_{resp.status_code}: {body}"}
            )
            return result_obj

        try:
            data = resp.json()
        except Exception as e:
            result_obj.update(
                {"output": "[parse error in vLLM response]",
                 "finish_reason": "error", "error": f"parse: {e}"}
            )
            return result_obj

        result_obj["raw"] = data
        choices = data.get("choices") or []
        if choices:
            first = choices[0]
            msg = first.get("message") or {}
            result_obj["output"] = msg.get("content") or str(first)
            fr = first.get("finish_reason") or data.get("finish_reason")
            if fr is not None:
                result_obj["finish_reason"] = fr
            tc = msg.get("tool_calls")
            if isinstance(tc, list) and tc:
                result_obj["tool_calls"] = tc
        else:
            result_obj["output"] = str(data)

        if isinstance(data.get("usage"), dict):
            result_obj["usage"] = data["usage"]

        return result_obj


# ============================================================
# Router-side KV-events subscriber (mirrors sidecar/zmq_subscriber.py)
# ============================================================

BlockHash = NewType("BlockHash", int)

try:
    import msgspec

    class _EventBatch(msgspec.Struct, array_like=True, omit_defaults=True, gc=False):
        ts: float
        events: list

    class _KVCacheEvent(msgspec.Struct, array_like=True, omit_defaults=True, gc=False, tag=True):
        pass

    class _BlockStored(_KVCacheEvent):
        block_hashes: list
        parent_block_hash: Optional[int]
        token_ids: list
        block_size: int
        lora_id: Optional[int]

    class _BlockRemoved(_KVCacheEvent):
        block_hashes: list

    class _AllBlocksCleared(_KVCacheEvent):
        pass

    class _KVEventBatch(_EventBatch):
        events: list

    _MSGSPEC_OK = True
except Exception:  # pragma: no cover - msgspec always present with vLLM stack
    _MSGSPEC_OK = False


class RouterKVSubscriber:
    """Subscribe to one external vLLM's KV-cache-events ZMQ and write Redis
    block-owner data keyed by the endpoint id, so prefix routing (owner_lookup)
    finds owners for external endpoints just like it does for sidecar pods.

    Redis schema (matches sidecar/zmq_subscriber.py, keyed by endpoint id):
      {model}:kvblock:{hash}   HASH  hash -> { endpoint_id: ts }
      {model}:podblocks:{id}   SET   endpoint -> { hashes }
      {model}:kvblocks         HASH  index of all hashes
    """

    def __init__(self, endpoint: ExternalEndpoint):
        import redis

        self._ep = endpoint
        self._endpoints = list(endpoint.kv_events_endpoints)
        self._topic = endpoint.kv_events_topic or "kv@"
        self._model = endpoint.model
        self._owner = endpoint.id
        self._redis = redis.Redis(
            host=_cfg.REDIS_HOST, port=_cfg.REDIS_PORT, decode_responses=True
        )
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        if _MSGSPEC_OK:
            self._decoder = msgspec.msgpack.Decoder(type=_KVEventBatch)
        else:
            self._decoder = None

    def start(self) -> None:
        if self._thread is not None or not self._endpoints or not _MSGSPEC_OK:
            if not self._endpoints:
                _log(f"KV subscriber skipped for {self._owner} (no kv_events_endpoints)")
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True,
                                        name=f"ext-kvsub-{self._owner}")
        self._thread.start()
        _log(f"KV subscriber started for {self._owner}: {self._endpoints} "
             f"(topic {self._topic!r}, model {self._model!r})")

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None

    def _loop(self) -> None:
        import zmq

        ctx = zmq.Context()
        sub = ctx.socket(zmq.SUB)
        for ep in self._endpoints:
            try:
                sub.connect(ep)
                _log(f"[{self._owner}] KV-SUB connected to {ep}")
            except Exception as e:
                _log(f"[{self._owner}] KV-SUB connect failed {ep}: {e}", level="full")
        sub.setsockopt_string(zmq.SUBSCRIBE, self._topic)

        try:
            while not self._stop.is_set():
                try:
                    frames = sub.recv_multipart(flags=zmq.NOBLOCK)
                except zmq.Again:
                    time.sleep(0.01)
                    continue
                except Exception as e:
                    _log(f"[{self._owner}] KV-SUB recv error: {e}", level="full")
                    time.sleep(1.0)
                    continue

                if len(frames) != 3:
                    continue
                _topic, _seq, payload = frames
                try:
                    batch = self._decoder.decode(payload)
                except Exception as e:
                    _log(f"[{self._owner}] KV-SUB decode error: {e}", level="full")
                    continue
                try:
                    self._handle_batch(batch)
                except Exception as e:
                    _log(f"[{self._owner}] KV-SUB batch error: {e}", level="full")
        finally:
            try:
                sub.close(0)
            except Exception:
                pass
            ctx.term()

    def _handle_batch(self, batch: Any) -> None:
        key_prefix = f"{self._model}:" if self._model else ""
        kvblocks_key = f"{key_prefix}kvblocks"
        podblocks_key = f"{key_prefix}podblocks:{self._owner}"

        pipe = self._redis.pipeline(transaction=False)
        ts = int(time.time())

        for ev in batch.events:
            if isinstance(ev, _BlockStored):
                for bh in ev.block_hashes:
                    bh_str = str(bh)
                    kvblock_key = f"{key_prefix}kvblock:{bh_str}"
                    pipe.hset(kvblock_key, self._owner, ts)
                    pipe.sadd(podblocks_key, bh_str)
                    pipe.hset(kvblocks_key, bh_str, kvblock_key)
            elif isinstance(ev, _BlockRemoved):
                for bh in ev.block_hashes:
                    bh_str = str(bh)
                    kvblock_key = f"{key_prefix}kvblock:{bh_str}"
                    pipe.hdel(kvblock_key, self._owner)
                    pipe.srem(podblocks_key, bh_str)
            elif isinstance(ev, _AllBlocksCleared):
                for bh_str in self._redis.sscan_iter(podblocks_key):
                    kvblock_key = f"{key_prefix}kvblock:{bh_str}"
                    pipe.hdel(kvblock_key, self._owner)
                pipe.delete(podblocks_key)

        try:
            pipe.execute()
        except Exception as e:
            _log(f"[{self._owner}] KV-SUB redis error: {e}", level="full")


class RouterKVSubscriberPool:
    """Manages one RouterKVSubscriber per external endpoint that declares
    kv_events_endpoints. No-op when EXTERNAL_KV_EVENTS is off."""

    def __init__(self, registry: ExternalRegistry):
        self._subs: List[RouterKVSubscriber] = []
        if not bool(getattr(_cfg, "EXTERNAL_KV_EVENTS", True)):
            return
        for ep_id in registry.all_ids():
            ep = registry.get(ep_id)
            if ep and ep.kv_events_endpoints:
                self._subs.append(RouterKVSubscriber(ep))

    def start(self) -> None:
        for s in self._subs:
            try:
                s.start()
            except Exception as e:
                _log(f"KV subscriber start failed: {e}", level="full")

    def stop(self) -> None:
        for s in self._subs:
            try:
                s.stop()
            except Exception:
                pass
        self._subs = []
