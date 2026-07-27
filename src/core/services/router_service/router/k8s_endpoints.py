# router/k8s_endpoints.py
# -*- coding: utf-8 -*-
"""Kubernetes vLLM registry for sidecar-less central-push.

When ``ROUTER_MODE=central-push`` and ``ROUTER_SIDECAR_ENABLED=false``, the
router keeps its k8s pod discovery + central queue (KV-affinity / fairness / SLO
all still apply via ``pull_for_endpoint``), but delivers each request DIRECTLY to
the pod's vLLM OpenAI endpoint instead of the per-pod sidecar ``/push``.

To maximize reuse this presents the SAME interface as
``external_endpoints.ExternalRegistry`` (``all_ids`` / ``get`` / ``healthy_ids``
/ ``refresh_health`` / ``aclose``), so it can drive the existing
``ExternalPushDispatcher`` + ``ExternalVLLMClient`` unchanged. The only
difference vs the static external registry: endpoints come from live k8s pod
discovery (not ``ROUTER_STATIC_ENDPOINTS``) and can churn, so this registry also
owns a per-pod ``RouterKVSubscriber`` that it starts/stops as pods appear/vanish.

Endpoint identity IS the pod name -- identical to the sidecar push path
(``PushRouter``) -- so affinity, in-flight bookkeeping, metrics and Redis KV
owners are all keyed the same way whether or not the sidecar is in the loop.
"""
from __future__ import annotations

import asyncio
import time
from typing import Dict, List, Optional

import httpx

from .config import get_config
from .k8s_discovery import discover_running_pods
from .external_endpoints import (
    ExternalEndpoint,
    RouterKVSubscriber,
    _default_model,
)

_cfg = get_config()


def _log(msg: str, *, level: str = "summary") -> None:
    mode = str(getattr(_cfg, "REQ_LOG_MODE", "off")).lower()
    if mode == "off":
        return
    if level == "summary" or (level == "full" and mode == "full"):
        print(f"[k8s-registry] {msg}")


class K8sVLLMRegistry:
    """Live k8s pod set + async /health gating + per-pod KV subscribers.

    Drop-in for ``ExternalRegistry`` in the direct-delivery dispatch path.
    """

    def __init__(self) -> None:
        self._by_id: Dict[str, ExternalEndpoint] = {}
        self._healthy: Dict[str, bool] = {}
        self._subs: Dict[str, RouterKVSubscriber] = {}

        self._last_discovery = 0.0
        self._last_probe = 0.0
        self._discovery_interval_s = float(getattr(_cfg, "KV_DISCOVERY_INTERVAL_S", 5.0))

        # KV-events subscriber only matters for prefix routing (KV_AWARE). For
        # affinity-only / none strategies the router needs no block-owner data,
        # so we skip the ZMQ subscribers entirely (nothing to publish/consume).
        self._kv_enabled = bool(getattr(_cfg, "KV_AWARE", False))
        self._model = _default_model()

        t = float(getattr(_cfg, "PUSH_HTTP_TIMEOUT_S", 2.0))
        self._health_client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=t, read=t, write=t, pool=t)
        )

        # Initial (synchronous) discovery so the first dispatch pass has pods.
        self._discover(force=True)

    # ------------------------------------------------------------------
    # ExternalRegistry-compatible surface
    # ------------------------------------------------------------------
    def all_ids(self) -> List[str]:
        return list(self._by_id.keys())

    def get(self, ep_id: str) -> Optional[ExternalEndpoint]:
        return self._by_id.get(ep_id)

    def healthy_ids(self) -> List[str]:
        return [pod for pod in self._by_id if self._healthy.get(pod, True)]

    async def refresh_health(self, *, force: bool = False) -> None:
        # 1) Re-discover pods (throttled) so the endpoint set tracks scale
        #    up/down and pod restarts; starts/stops KV subscribers as needed.
        self._discover(force=force)

        # 2) Probe vLLM /health per pod (throttled) to gate dispatch.
        interval = float(getattr(_cfg, "EXTERNAL_HEALTH_INTERVAL_S", 5.0))
        now = time.time()
        if not force and interval > 0 and (now - self._last_probe) < interval:
            return
        self._last_probe = now

        eps = list(self._by_id.values())

        async def one(ep: ExternalEndpoint) -> None:
            try:
                r = await self._health_client.get(f"{ep.url}/health")
                self._healthy[ep.id] = bool(r.status_code == 200)
            except Exception:
                self._healthy[ep.id] = False

        await asyncio.gather(*(one(e) for e in eps), return_exceptions=True)

    async def aclose(self) -> None:
        for sub in list(self._subs.values()):
            try:
                sub.stop()
            except Exception:
                pass
        self._subs.clear()
        try:
            await self._health_client.aclose()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Discovery internals
    # ------------------------------------------------------------------
    def _vllm_url(self, ip: str) -> str:
        return f"http://{ip}:{int(_cfg.VLLM_PORT)}"

    def _make_endpoint(self, pod: str, ip: str) -> ExternalEndpoint:
        kv_eps: List[str] = []
        if self._kv_enabled:
            kv_eps = [f"tcp://{ip}:{int(_cfg.VLLM_KV_EVENTS_PORT)}"]
        return ExternalEndpoint(
            id=pod,
            url=self._vllm_url(ip),
            model=self._model,
            kv_events_endpoints=kv_eps,
            kv_events_topic=str(getattr(_cfg, "VLLM_KV_EVENTS_TOPIC", "kv@")),
        )

    def _start_sub(self, ep: ExternalEndpoint) -> None:
        if not self._kv_enabled or not ep.kv_events_endpoints:
            return
        try:
            sub = RouterKVSubscriber(ep)
            sub.start()
            self._subs[ep.id] = sub
        except Exception as e:
            _log(f"KV subscriber start failed for {ep.id}: {e}", level="full")

    def _stop_sub(self, pod: str) -> None:
        sub = self._subs.pop(pod, None)
        if sub is not None:
            try:
                sub.stop()
            except Exception:
                pass

    def _discover(self, *, force: bool = False) -> None:
        now = time.time()
        if (
            not force
            and self._by_id
            and (now - self._last_discovery) < self._discovery_interval_s
        ):
            return
        self._last_discovery = now

        pods = discover_running_pods(log_prefix="[k8s-registry]")
        if not pods and self._by_id:
            # Transient discovery failure: keep the last known set rather than
            # dropping every endpoint (matches PushRouter's sticky behavior).
            return

        new_ids = set(pods.keys())
        old_ids = set(self._by_id.keys())

        # Added / changed pods.
        for pod, ip in pods.items():
            existing = self._by_id.get(pod)
            if existing is not None and existing.url == self._vllm_url(ip):
                continue  # unchanged
            ep = self._make_endpoint(pod, ip)
            self._by_id[pod] = ep
            self._healthy.setdefault(pod, True)
            # (Re)start the subscriber if the pod is new or its IP changed.
            self._stop_sub(pod)
            self._start_sub(ep)

        # Removed pods.
        for pod in old_ids - new_ids:
            self._by_id.pop(pod, None)
            self._healthy.pop(pod, None)
            self._stop_sub(pod)

        if new_ids != old_ids:
            _log(f"discovered {len(new_ids)} vLLM pods: {sorted(new_ids)}")
