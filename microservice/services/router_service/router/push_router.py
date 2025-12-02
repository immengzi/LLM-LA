# -*- coding: utf-8 -*-
import os
import random
import time
from typing import Dict, List, Optional

import httpx
from kubernetes import client as k8s_client, config as k8s_config

from .config import get_config

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
    def __init__(self, mode: str):
        self.mode = mode  # "push-rr", "push-random", "push-leastq"
        self._eps: List[str] = []       # pod names
        self._urls: Dict[str, str] = {} # pod_name -> sidecar base URL
        self._rr_idx: int = 0
        self._last_discovery = 0.0
        self._discovery_interval_s = float(getattr(_cfg, "KV_DISCOVERY_INTERVAL_S", 5.0))

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

    def _ensure_endpoints(self):
        now = time.time()
        if self._eps and (now - self._last_discovery) < self._discovery_interval_s:
            return

        pods = self._discover_pods()
        self._eps = list(pods.keys())
        self._urls = {
            pod: f"http://{ip}:{_cfg.SIDECAR_PORT}"
            for pod, ip in pods.items()
        }
        self._last_discovery = now

        _log_req(f"discovered {len(self._eps)} pods: {self._eps}", level="summary")

    # ---------------------------------------------------------
    # Endpoint selection
    # ---------------------------------------------------------

    def _pick_endpoint_rr(self) -> Optional[str]:
        if not self._eps:
            return None
        ep = self._eps[self._rr_idx % len(self._eps)]
        self._rr_idx = (self._rr_idx + 1) % len(self._eps)

        _log_req(f"RR pick → {ep}", level="full")
        return ep

    def _pick_endpoint_random(self) -> Optional[str]:
        if not self._eps:
            return None
        ep = random.choice(self._eps)

        _log_req(f"Random pick → {ep}", level="full")
        return ep

    async def _pick_endpoint_leastq(self) -> Optional[str]:
        if not self._eps:
            return None

        best_ep = None
        best_score = None

        async with httpx.AsyncClient(timeout=2.0) as client:
            for ep in self._eps:
                url = self._urls.get(ep)
                if not url:
                    continue

                try:
                    r = await client.get(f"{url}/health")
                    if r.status_code != 200:
                        continue
                    data = r.json()
                    score = int(data.get("logical", data.get("queue_len", 0)))
                except Exception:
                    continue

                if best_score is None or score < best_score:
                    best_score = score
                    best_ep = ep

        _log_req(f"LeastQ pick → {best_ep} (score={best_score})", level="full")
        return best_ep

    async def _pick_endpoint(self) -> Optional[str]:
        if self.mode == "push-rr":
            return self._pick_endpoint_rr()
        if self.mode == "push-random":
            return self._pick_endpoint_random()
        if self.mode == "push-leastq":
            return await self._pick_endpoint_leastq()
        # fallback
        return self._pick_endpoint_rr()

    # ---------------------------------------------------------
    # Push operation
    # ---------------------------------------------------------

    async def route_and_push(self, req_id: int, prompt: str, meta: dict) -> None:
        self._ensure_endpoints()
        if not self._eps:
            raise RuntimeError("No endpoints available for push routing")

        ep = await self._pick_endpoint()
        if not ep:
            raise RuntimeError("Failed to pick endpoint")

        url = self._urls.get(ep)
        if not url:
            raise RuntimeError(f"No sidecar URL for endpoint {ep}")

        payload = {
            "req_id": int(req_id),
            "prompt": str(prompt),
            "meta": meta or {},
        }

        _log_req(
            f"push req_id={req_id} → {ep} ({url})",
            level="summary",
        )

        async with httpx.AsyncClient(timeout=2.0) as client:
            try:
                r = await client.post(f"{url}/push", json=payload)
            except Exception as e:
                _log_req(f"push failed for {ep}: {e}", level="full")
                raise

            if r.status_code != 200:
                _log_req(
                    f"push to {ep} failed: {r.status_code} {r.text}",
                    level="full",
                )
                raise RuntimeError(f"push to {ep} failed: {r.status_code} {r.text}")
