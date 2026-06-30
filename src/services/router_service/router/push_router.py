# router/push_router.py
# -*- coding: utf-8 -*-
import os
import random
import time
import asyncio
from collections import defaultdict
from typing import Dict, List, Optional
from threading import RLock

import httpx
from kubernetes import client as k8s_client, config as k8s_config

from .config import get_config
from .metrics import inc_dispatch
from .kv_aware import get_request_blocks, prefix_len, record_routing

_cfg = get_config()


def _log_req(msg: str, *, level: str = "summary") -> None:
    """
    Centralized logging for push-routing decisions.
    Uses _cfg.REQ_LOG_MODE directly.
    """
    mode = str(_cfg.REQ_LOG_MODE).lower()

    if mode == "off":
        return

    if level == "summary":
        print(f"[PushRouter] {msg}")
    elif level == "full" and mode == "full":
        print(f"[PushRouter] {msg}")


class PushRouter:
    """
    Push-mode endpoint selector + sidecar push client.

    IMPORTANT for decoupled dispatch:
      - route_and_push() can be called concurrently by many background workers.
      - so endpoint discovery / endpoint lists / logical inflight bookkeeping must be guarded.
    """

    def __init__(self, mode: str):
        self.mode = mode  # "push-rr", "push-random", "push-leastq"

        # shared mutable state guarded by _lock
        self._lock = RLock()
        self._eps: List[str] = []        # pod names
        self._urls: Dict[str, str] = {}  # pod_name -> sidecar base URL
        self._rr_idx: int = 0
        self._last_discovery = 0.0
        self._discovery_interval_s = float(getattr(_cfg, "KV_DISCOVERY_INTERVAL_S", 5.0))
        self._leastq_mode: str = getattr(_cfg, "PUSH_LEASTQ_MODE", "health")
        # local logical queue lengths: sent - completed
        self._logical_inflight = defaultdict(int)

        # ------------------------------------------------------------------
        # Long-lived httpx clients (important for decoupled dispatch)
        #
        # IMPORTANT: httpx.Timeout must include either a default timeout
        # or explicitly set all four: connect/read/write/pool.
        # ------------------------------------------------------------------
        t = float(getattr(_cfg, "PUSH_HTTP_TIMEOUT_S", 2.0))
        timeout = httpx.Timeout(connect=t, read=t, write=t, pool=t)

        limits_health = httpx.Limits(
            max_keepalive_connections=int(getattr(_cfg, "PUSH_MAX_KEEPALIVE", 200)),
            max_connections=int(getattr(_cfg, "PUSH_MAX_KEEPALIVE", 200)),
            keepalive_expiry=float(getattr(_cfg, "PUSH_KEEPALIVE_EXPIRY_S", 30.0)),
        )
        limits_push = httpx.Limits(
            max_keepalive_connections=int(getattr(_cfg, "PUSH_MAX_KEEPALIVE", 200)),
            max_connections=int(getattr(_cfg, "PUSH_MAX_KEEPALIVE", 200)),
            keepalive_expiry=float(getattr(_cfg, "PUSH_KEEPALIVE_EXPIRY_S", 30.0)),
        )

        self._health_client = httpx.AsyncClient(timeout=timeout, limits=limits_health)
        self._push_client = httpx.AsyncClient(timeout=timeout, limits=limits_push)

    async def aclose(self) -> None:
        """
        Close underlying httpx clients. Safe to call multiple times.
        """
        try:
            await self._health_client.aclose()
        except Exception:
            pass
        try:
            await self._push_client.aclose()
        except Exception:
            pass

    # ---------------------------------------------------------
    # Pod discovery
    # ---------------------------------------------------------

    def _discover_pods(self) -> Dict[str, str]:
        running_in_cluster = os.getenv("KUBERNETES_SERVICE_HOST") is not None
        try:
            if running_in_cluster:
                k8s_config.load_incluster_config()
            else:
                k8s_config.load_kube_config()
        except Exception as e:
            print(f"[PushRouter] failed to load K8s config: {e}")
            return {}

        v1 = k8s_client.CoreV1Api()
        try:
            pods = v1.list_namespaced_pod(
                namespace=_cfg.NAMESPACE,
                label_selector=_cfg.LABEL_SELECTOR,
            ).items
        except Exception as e:
            print(f"[PushRouter] list_namespaced_pod failed: {e}")
            return {}

        out: Dict[str, str] = {}
        for pod in pods:
            if pod.status.phase == "Running" and pod.status.pod_ip:
                out[pod.metadata.name] = pod.status.pod_ip
        return out

    def _refresh_endpoints_locked(self, *, force: bool = False) -> None:
        """
        Refresh endpoint list/URLs if stale, or always if force=True.
        Caller must hold self._lock.
        """
        now = time.time()
        if (not force) and self._eps and (now - self._last_discovery) < self._discovery_interval_s:
            return

        pods = self._discover_pods()
        eps = list(pods.keys())
        urls = {pod: f"http://{ip}:{_cfg.SIDECAR_PORT}" for pod, ip in pods.items()}

        self._eps = eps
        self._urls = urls
        self._last_discovery = now

        # Keep rr index sane if endpoint set changes
        if self._eps:
            self._rr_idx = self._rr_idx % len(self._eps)
        else:
            self._rr_idx = 0

        _log_req(f"discovered {len(self._eps)} pods: {self._eps}", level="summary")

    def _ensure_endpoints(self) -> None:
        with self._lock:
            self._refresh_endpoints_locked(force=False)

    # ---------------------------------------------------------
    # Endpoint selection
    # ---------------------------------------------------------

    def _pick_endpoint_rr(self) -> Optional[str]:
        with self._lock:
            if not self._eps:
                return None
            ep = self._eps[self._rr_idx % len(self._eps)]
            self._rr_idx = (self._rr_idx + 1) % len(self._eps)

        _log_req(f"RR pick → {ep}", level="full")
        return ep

    def _pick_endpoint_random(self) -> Optional[str]:
        with self._lock:
            if not self._eps:
                return None
            eps = list(self._eps)

        ep = random.choice(eps)
        _log_req(f"Random pick → {ep}", level="full")
        return ep

    async def _pick_endpoint_leastq_health(self) -> Optional[str]:
        with self._lock:
            if not self._eps:
                return None
            eps = list(self._eps)
            urls = dict(self._urls)

        async def fetch_score(ep: str):
            url = urls.get(ep)
            if not url:
                return ep, None
            try:
                r = await self._health_client.get(f"{url}/health")
                if r.status_code != 200:
                    return ep, None
                data = r.json()
                score = int(data.get("logical", data.get("queue_len", 0)))
                return ep, score
            except Exception:
                return ep, None

        results = await asyncio.gather(*(fetch_score(ep) for ep in eps), return_exceptions=False)

        best_ep = None
        best_score = None
        for ep, score in results:
            if score is None:
                continue
            if best_score is None or score < best_score:
                best_score = score
                best_ep = ep

        _log_req(f"LeastQ(health) pick → {best_ep} (score={best_score})", level="full")
        return best_ep

    def _pick_endpoint_leastq_local(self) -> Optional[str]:
        with self._lock:
            if not self._eps:
                return None
            eps = list(self._eps)
            inflight = dict(self._logical_inflight)

        best_ep = None
        best_score = None
        for ep in eps:
            score = int(inflight.get(ep, 0))
            if best_score is None or score < best_score:
                best_score = score
                best_ep = ep

        _log_req(f"LeastQ(local) pick → {best_ep} (score={best_score})", level="full")
        return best_ep

    async def _pick_endpoint_leastq(self) -> Optional[str]:
        if self._leastq_mode == "local":
            return self._pick_endpoint_leastq_local()
        return await self._pick_endpoint_leastq_health()

    async def _pick_endpoint(self) -> Optional[str]:
        if self.mode == "push-rr":
            return self._pick_endpoint_rr()
        if self.mode == "push-random":
            return self._pick_endpoint_random()
        if self.mode == "push-leastq":
            return await self._pick_endpoint_leastq()
        return self._pick_endpoint_rr()

    # ---------------------------------------------------------
    # Push operation (trace added)
    # ---------------------------------------------------------

    async def route_and_push(self, req_id: str, prompt: str, meta: dict) -> None:
        """
        Dispatch a request to a sidecar in PUSH mode.
        Injects trace info into meta["__trace__"] if TRACE_ENABLED.

        Concurrency notes:
          - called by background dispatch workers concurrently
          - protects shared endpoint lists and leastq-local counters
        """
        # Ensure we have an endpoint snapshot
        self._ensure_endpoints()

        # We'll retry once after forcing endpoint refresh (handles pod churn / stale IPs)
        last_err: Optional[Exception] = None
        for attempt in (0, 1):
            if attempt == 1:
                with self._lock:
                    self._refresh_endpoints_locked(force=True)

            with self._lock:
                if not self._eps:
                    raise RuntimeError("No endpoints available for push routing")

            ep = await self._pick_endpoint()
            if not ep:
                raise RuntimeError("Failed to pick endpoint")

            with self._lock:
                url = self._urls.get(ep)

            if not url:
                last_err = RuntimeError(f"No sidecar URL for endpoint {ep}")
                continue

            # Prom: outgoing dispatch (router -> sidecar)
            inc_dispatch(ep)

            # ---------------------------------------------------------
            # Logical queue length (for leastq-local)
            # ---------------------------------------------------------
            logical_before: Optional[int] = None
            if self.mode == "push-leastq" and self._leastq_mode == "local":
                with self._lock:
                    logical_before = int(self._logical_inflight.get(ep, 0))
                    self._logical_inflight[ep] = logical_before + 1

            dispatch_ts = time.time()

            # Capture routing decision per request (independent of TRACE) so the
            # /latency_log ring can be enriched at completion time.
            _blocks = get_request_blocks(req_id)
            record_routing(
                req_id,
                endpoint=ep,
                kv_hits_len=prefix_len(ep, req_id) if _cfg.KV_AWARE else 0,
                total_blocks=len(_blocks),
                affinity_key=(meta or {}).get("__affinity_key__"),
                block_hashes=_blocks if getattr(_cfg, "ROUTER_LOG_BLOCK_HASHES", False) else None,
            )

            # ---------------------------------------------------------
            # Inject trace into meta["__trace__"]
            # ---------------------------------------------------------
            if getattr(_cfg, "TRACE_ENABLED", False):
                meta = dict(meta or {})
                tr = dict(meta.get("__trace__") or {})

                tr.setdefault("endpoint", ep)
                tr.setdefault("router_mode", self.mode)
                tr["t_dispatch_router"] = dispatch_ts

                if _cfg.KV_AWARE:
                    tr["kv_block_hashes"] = get_request_blocks(req_id)

                if logical_before is not None:
                    tr["router_logical_inflight_before"] = logical_before
                    tr["router_logical_inflight_after"] = logical_before + 1

                meta["__trace__"] = tr

            payload = {
                "req_id": req_id,
                "prompt": str(prompt),
                "meta": meta or {},
            }

            # Helps leastq-local debugging (optional field)
            if self._leastq_mode == "local":
                payload["endpoint"] = ep

            _log_req(f"push req_id={req_id} → {ep} ({url})", level="summary")

            try:
                r = await self._push_client.post(f"{url}/push", json=payload)
            except Exception as e:
                last_err = e
                _log_req(f"push failed for {ep}: {e}", level="full")
                if self.mode == "push-leastq" and self._leastq_mode == "local":
                    with self._lock:
                        if self._logical_inflight[ep] > 0:
                            self._logical_inflight[ep] -= 1
                continue

            if r.status_code != 200:
                last_err = RuntimeError(f"push to {ep} failed: {r.status_code} {r.text}")
                _log_req(f"push to {ep} failed: {r.status_code} {r.text}", level="full")
                if self.mode == "push-leastq" and self._leastq_mode == "local":
                    with self._lock:
                        if self._logical_inflight[ep] > 0:
                            self._logical_inflight[ep] -= 1
                continue

            # success
            return

        # if we got here, both attempts failed
        if last_err is not None:
            raise last_err
        raise RuntimeError("push failed")

    # ---------------------------------------------------------
    # Result notification (for local leastq mode)
    # ---------------------------------------------------------

    def notify_result(self, endpoint: Optional[str]) -> None:
        if not endpoint:
            return
        with self._lock:
            if endpoint not in self._eps:
                return
            current = self._logical_inflight.get(endpoint, 0)
            if current > 0:
                self._logical_inflight[endpoint] = current - 1
