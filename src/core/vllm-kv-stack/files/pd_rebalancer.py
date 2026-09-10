#!/usr/bin/env python3
"""Quiescent, fixed-budget Prefill/Decode Deployment rebalancer.

``warmstandby`` mode keeps a pre-warm pool: each card hosts one dual-engine pod
(prefill + decode), and cards not serving the target topology keep BOTH engines
asleep (weights on CPU, KV dropped) so any role can be woken in seconds. A
scale-up wakes a role engine on a fully-asleep pool card without draining the
proxy; a scale-down or role flip sleeps/flips awake engines after a drain
handshake.
"""

import concurrent.futures
import dataclasses
import json
import os
import ssl
import threading
import time
import urllib.request
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Optional, get_type_hints
from urllib.parse import quote
from urllib.error import HTTPError
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class Replicas:
    prefill: int
    decode: int


@dataclass(frozen=True)
class ModelConfig:
    name: str
    prefill_deployment: str
    decode_deployment: str
    proxy_service: str
    min_prefill: int
    min_decode: int
    max_total: int
    prefill_tp: int = 1
    decode_tp: int = 1
    mode: str = "scale"          # scale (cold /scale) | warmstandby (per-card sleep/wake)
    deployment: str = ""         # warmstandby: single dual-engine Deployment name
    replicas: int = 0            # warmstandby: dual-engine pod (card) count
    prefill_port: int = 8200
    decode_port: int = 8201
    sleep_level: int = 1


class CapacityError(RuntimeError):
    """The target topology cannot fit the cluster's accelerator budget."""


class ApiError(RuntimeError):
    """Kubernetes API failure carrying the HTTP status code."""

    def __init__(self, message: str, code: int) -> None:
        super().__init__(message)
        self.code = code


def transition_plan(current: Replicas, target: Replicas, config: ModelConfig) -> list[tuple[str, int]]:
    if target.prefill < config.min_prefill or target.decode < config.min_decode:
        raise ValueError("target violates the configured minimum replicas")
    if target.prefill + target.decode > config.max_total:
        raise ValueError("target exceeds the fixed Prefill/Decode replica budget")
    if config.mode == "warmstandby" and target.prefill + target.decode > config.replicas:
        raise ValueError("target exceeds the dual-engine pod (card) budget")

    plan: list[tuple[str, int]] = []
    if target.prefill < current.prefill:
        plan.append(("prefill", target.prefill))
    if target.decode < current.decode:
        plan.append(("decode", target.decode))
    if target.prefill > current.prefill:
        plan.append(("prefill", target.prefill))
    if target.decode > current.decode:
        plan.append(("decode", target.decode))
    return plan


class KubernetesApi:
    def __init__(self, namespace: str) -> None:
        host = os.environ["KUBERNETES_SERVICE_HOST"]
        port = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")
        self.base_url = f"https://{host}:{port}"
        with open("/var/run/secrets/kubernetes.io/serviceaccount/token", encoding="utf-8") as token_file:
            self.token = token_file.read().strip()
        self.context = ssl.create_default_context(
            cafile="/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
        )
        self.namespace = namespace

    def request(
        self,
        method: str,
        path: str,
        body: Optional[dict[str, Any]] = None,
        content_type: str = "application/merge-patch+json",
        allow_404: bool = False,
    ) -> Optional[dict[str, Any]]:
        payload = None if body is None else json.dumps(body).encode()
        request = Request(
            f"{self.base_url}{path}",
            data=payload,
            method=method,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/json",
                "Content-Type": content_type,
            },
        )
        try:
            with urlopen(request, context=self.context, timeout=15) as response:
                return json.loads(response.read())
        except HTTPError as error:
            if allow_404 and error.code == 404:
                return None
            detail = error.read().decode(errors="replace")
            raise ApiError(
                f"Kubernetes API {method} {path} failed: {error.code} {detail}",
                error.code,
            ) from error

    def deployment(self, name: str) -> dict[str, Any]:
        return self.request("GET", f"/apis/apps/v1/namespaces/{self.namespace}/deployments/{name}")

    def pod_patch_labels(self, name: str, labels: dict[str, str]) -> None:
        """Merge-patch pod labels (per-card warmstandby pd-*-awake toggles)."""
        self.request(
            "PATCH",
            f"/api/v1/namespaces/{self.namespace}/pods/{name}",
            {"metadata": {"labels": labels}},
        )

    def scale(self, name: str, replicas: int) -> None:
        self.request(
            "PATCH",
            f"/apis/apps/v1/namespaces/{self.namespace}/deployments/{name}/scale",
            {"spec": {"replicas": replicas}},
        )

    def state(self, name: str) -> dict[str, Any]:
        state = self.request(
            "GET",
            f"/api/v1/namespaces/{self.namespace}/configmaps/{name}",
            allow_404=True,
        )
        if state is None:
            # The rebalancer owns this ConfigMap: Helm no longer renders it, so
            # a fresh install (or a pruned/force-upgraded release) bootstraps
            # it here instead of letting the chart stamp "{}" over live state.
            try:
                state = self.create_state(name)
            except ApiError as error:
                if error.code != 409:
                    raise
                # Another worker won the create race; read the winner.
                state = self.request(
                    "GET",
                    f"/api/v1/namespaces/{self.namespace}/configmaps/{name}",
                    allow_404=True,
                )
                if state is None:  # pragma: no cover - a 409 winner must exist
                    raise error
            print(f"[pd-rebalancer] created state configmap {name}", flush=True)
        return state

    def create_state(self, name: str) -> dict[str, Any]:
        body = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": name, "namespace": self.namespace},
            "data": {"targets.json": "{}"},
        }
        created = self.request(
            "POST",
            f"/api/v1/namespaces/{self.namespace}/configmaps",
            body,
            content_type="application/json",
        )
        assert created is not None
        return created

    def write_state(self, name: str, targets: dict[str, Any]) -> None:
        self.patch_data(name, {"targets.json": json.dumps(targets, sort_keys=True)})

    def patch_data(self, name: str, data: dict[str, str]) -> None:
        self.request(
            "PATCH",
            f"/api/v1/namespaces/{self.namespace}/configmaps/{name}",
            {"data": data},
        )

    def pods(self, label_selector: str) -> list[dict[str, Any]]:
        path = (
            f"/api/v1/namespaces/{self.namespace}/pods"
            f"?labelSelector={quote(label_selector)}"
        )
        return self.request("GET", path).get("items", [])

    def nodes(self) -> list[dict[str, Any]]:
        return self.request("GET", "/api/v1/nodes").get("items", [])

    def pods_on_node(self, node: str) -> list[dict[str, Any]]:
        path = f"/api/v1/pods?fieldSelector={quote('spec.nodeName=' + node)}"
        return self.request("GET", path).get("items", [])


class Rebalancer:
    def __init__(self) -> None:
        self.namespace = os.environ["POD_NAMESPACE"]
        self.state_configmap = os.environ["PD_REBALANCER_STATE_CONFIGMAP"]
        self.poll_seconds = float(os.environ.get("PD_REBALANCER_POLL_SECONDS", "2"))
        self.ready_timeout = float(os.environ.get("PD_REBALANCER_READY_TIMEOUT_SECONDS", "900"))
        self.dry_run = os.environ.get("PD_REBALANCER_DRY_RUN", "").lower() in ("1", "true", "yes")
        self.drain_timeout = float(os.environ.get("PD_REBALANCER_DRAIN_TIMEOUT_SECONDS", "300"))
        self.capacity_preflight = os.environ.get(
            "PD_REBALANCER_CAPACITY_PREFLIGHT", ""
        ).lower() in ("1", "true", "yes")
        self.capacity_resource = os.environ.get(
            "PD_REBALANCER_CAPACITY_RESOURCE", "accelerator.example.com/device"
        )
        self.proxy_port = int(os.environ.get("PD_REBALANCER_PROXY_METRICS_PORT", "8200"))
        self.models = {
            item["name"]: ModelConfig(
                name=item["name"],
                prefill_deployment=item["prefillDeployment"],
                decode_deployment=item["decodeDeployment"],
                proxy_service=item.get("proxyService", f"vllm-{item['name']}"),
                min_prefill=int(item["minPrefillReplicas"]),
                min_decode=int(item["minDecodeReplicas"]),
                max_total=int(item["maxTotalReplicas"]),
                prefill_tp=int(item.get("prefillTp", 1)),
                decode_tp=int(item.get("decodeTp", 1)),
                mode=item.get("mode", "scale"),
                deployment=item.get("deployment", ""),
                replicas=int(item.get("replicas", 0)),
                prefill_port=int(item.get("prefillPort", 8200)),
                decode_port=int(item.get("decodePort", 8201)),
                sleep_level=int(item.get("sleepLevel", 1)),
            )
            for item in json.loads(os.environ["PD_REBALANCER_MODELS_JSON"])
        }
        self.api = KubernetesApi(self.namespace)
        self.lock = threading.Lock()
        self.last_error = ""
        self.planner_error = ""
        # Pods whose wake-up failed mid-flip (engine may have crashed). They
        # are taken out of service (labels idle) and excluded from flip plans;
        # the only safe recovery is pod recreation, because waking the peer on
        # the same card while the failed engine may still hold NPU memory is
        # what turns a wake failure into a crash cascade.
        self.needs_recreate: set[str] = set()
        self.heartbeat_timeout = float(
            os.environ.get("PD_REBALANCER_HEARTBEAT_TIMEOUT_SECONDS", "60")
        )
        self.heartbeats: dict[str, float] = {
            "rebalancer": time.monotonic(),
            "planner": time.monotonic(),
        }
        self.heartbeat_lock = threading.Lock()
        # KV-path warm-up gate: waking an engine into a role makes every
        # engine of the other role its "cold KV pair": the first decode->
        # prefill ADXL comm creation can collide on the HCCL ra port (CANN
        # 503900 / -800) and the retry storm pushes request latency from
        # seconds to minutes. The gate therefore runs INSIDE the proxy
        # drain window (traffic paused, new requests wait at the proxy) so
        # the probes are isolated and warm the pair deterministically in a
        # few seconds; on budget exhaustion the transition raises and rolls
        # back instead of leaving a degraded topology serving 100s+ latency.
        self.kv_warmup_enabled = os.environ.get(
            "PD_REBALANCER_KV_WARMUP", "1"
        ).lower() not in ("0", "false", "no")
        self.kv_warmup_attempts = int(
            os.environ.get("PD_REBALANCER_KV_WARMUP_ATTEMPTS", "3")
        )
        self.kv_warmup_timeout = float(
            os.environ.get("PD_REBALANCER_KV_WARMUP_TIMEOUT_SECONDS", "20")
        )
        self.kv_warmup_gap = float(
            os.environ.get("PD_REBALANCER_KV_WARMUP_GAP_SECONDS", "2")
        )
        # First pass after a pod restart: an active transition marker cannot be
        # resumed safely (we do not know which steps completed), so roll back.
        self._first_pass = True

    # ---- worker-loop heartbeats -------------------------------------------

    def stamp_heartbeat(self, component: str) -> None:
        """Record liveness for one worker loop (e.g. ``rebalancer``/``planner``).

        The loops are daemon threads, so a crashed loop would otherwise leave
        the pod healthy forever. Each loop stamps while it is actually running
        (iteration boundaries plus blocking wait/scrape polls); /healthz fails
        when a stamp goes stale.
        """
        with self.heartbeat_lock:
            self.heartbeats[component] = time.monotonic()

    def heartbeat_age(self, component: str) -> float:
        with self.heartbeat_lock:
            return time.monotonic() - self.heartbeats[component]

    def heartbeat_stale(self, component: str) -> bool:
        return self.heartbeat_age(component) > self.heartbeat_timeout

    @staticmethod
    def replicas(deployment: dict[str, Any]) -> int:
        return int(deployment.get("spec", {}).get("replicas", 0))

    @staticmethod
    def ready_replicas(deployment: dict[str, Any]) -> int:
        return int(deployment.get("status", {}).get("readyReplicas", 0))

    def current(self, config: ModelConfig) -> Replicas:
        if config.mode == "warmstandby":
            return self.awake(config)
        return Replicas(
            prefill=self.replicas(self.api.deployment(config.prefill_deployment)),
            decode=self.replicas(self.api.deployment(config.decode_deployment)),
        )

    # ---- warmstandby: per-card dual-engine awake/sleep executor -----------

    @staticmethod
    def _awake_label(role: str) -> str:
        if role == "prefill":
            return "pd-prefill-awake"
        if role == "decode":
            return "pd-decode-awake"
        raise ValueError(f"unknown P/D role: {role}")

    @classmethod
    def _pod_active_role(cls, pod: dict[str, Any]) -> Optional[str]:
        labels = (pod.get("metadata") or {}).get("labels") or {}
        for role in ("prefill", "decode"):
            if labels.get(cls._awake_label(role)) == "true":
                return role
        return None

    @staticmethod
    def _pod_ip(pod: dict[str, Any]) -> Optional[str]:
        return (pod.get("status") or {}).get("podIP")

    def _card_pods(self, config: ModelConfig) -> list[dict[str, Any]]:
        return self.api.pods(f"app={config.deployment}")

    def awake(self, config: ModelConfig) -> Replicas:
        pods = [p for p in self._card_pods(config) if self._pod_ip(p)]
        return Replicas(
            prefill=sum(1 for p in pods if self._pod_active_role(p) == "prefill"),
            decode=sum(1 for p in pods if self._pod_active_role(p) == "decode"),
        )

    def pool(self, config: ModelConfig) -> int:
        """Dual-engine cards whose BOTH engines are asleep (pool depth).

        A card is in the pre-warm pool only when neither engine is awake
        (pd-prefill-awake and pd-decode-awake are both false). Waking a role
        engine on such a card is the scale-up path; sleeping an excess engine
        returns the card to the pool.
        """
        return sum(
            1
            for p in self._card_pods(config)
            if self._pod_ip(p) and self._pod_active_role(p) is None
        )

    def _engine_base(self, pod: dict[str, Any], port: int) -> str:
        ip = self._pod_ip(pod)
        if not ip:
            raise RuntimeError(
                f"pod {(pod.get('metadata') or {}).get('name')} has no podIP"
            )
        return f"http://{ip}:{port}"

    def _role_port(self, config: ModelConfig, role: str) -> int:
        if role == "prefill":
            return config.prefill_port
        if role == "decode":
            return config.decode_port
        raise ValueError(f"unknown P/D role: {role}")

    def _sleep_engine(self, config: ModelConfig, pod: dict[str, Any], role: str) -> None:
        """POST /sleep level=N and wait /is_sleeping=true (no label change).

        A prefill (KV producer) engine that has served requests can transiently
        return HTTP 500 on /sleep: vLLM's ``reset_prefix_cache`` refuses to
        discard KV while a finished request still has remote KV transfer in
        flight, and ``finished_sending`` is only consumed while the engine keeps
        stepping. Retry with backoff so the engine drains its delayed connector
        frees between attempts. This relies on the upstream "keep scheduler
        alive for delayed KV connector frees" fix (vLLM #43433, commit
        82536acc54) so an idle engine keeps stepping to consume ``finished_sending``.
        """
        base = self._engine_base(pod, self._role_port(config, role))
        name = (pod.get("metadata") or {}).get("name", "")
        # Idempotent: an engine that already reports sleeping (label/runtime
        # divergence after a pod restart or a partial transition) needs no POST.
        try:
            with urlopen(f"{base}/is_sleeping", timeout=5) as response:
                if json.loads(response.read()).get("is_sleeping") is True:
                    print(
                        f"[pd-rebalancer] {config.name}: {name} {role} already sleeping",
                        flush=True,
                    )
                    return
        except Exception:  # noqa: BLE001
            pass
        sleep_retries = int(os.environ.get("PD_REBALANCER_SLEEP_RETRIES", "5"))
        sleep_backoff = float(os.environ.get("PD_REBALANCER_SLEEP_BACKOFF_SECONDS", "2"))
        last_error: Optional[Exception] = None
        for attempt in range(sleep_retries):
            self.stamp_heartbeat("rebalancer")
            request = Request(
                f"{base}/sleep",
                data=json.dumps({"level": config.sleep_level}).encode(),
                method="POST",
                headers={"Content-Type": "application/json"},
            )
            try:
                with urlopen(request, timeout=120) as response:
                    response.read()
            except HTTPError as error:
                last_error = error
                print(
                    f"[pd-rebalancer] {config.name}: {name} {role} sleep returned "
                    f"HTTP {error.code}, retry {attempt + 1}/{sleep_retries}",
                    flush=True,
                )
                time.sleep(sleep_backoff * (attempt + 1))
                continue

            deadline = time.monotonic() + self.ready_timeout
            while time.monotonic() < deadline:
                self.stamp_heartbeat("rebalancer")
                try:
                    with urlopen(f"{base}/is_sleeping", timeout=5) as response:
                        data = json.loads(response.read())
                    if data.get("is_sleeping") is True:
                        print(
                            f"[pd-rebalancer] {config.name}: {name} {role} sleeping",
                            flush=True,
                        )
                        return
                except Exception:  # noqa: BLE001
                    pass
                time.sleep(self.poll_seconds)
            last_error = TimeoutError(
                f"{name} {role} engine did not reach sleeping state"
            )
            print(
                f"[pd-rebalancer] {config.name}: {name} {role} not sleeping after "
                f"attempt {attempt + 1}/{sleep_retries}; retrying",
                flush=True,
            )
        raise RuntimeError(
            f"{name} {role} engine failed to sleep after {sleep_retries} attempts: "
            f"{last_error}"
        )

    def _wake_engine(self, config: ModelConfig, pod: dict[str, Any], role: str) -> None:
        """POST /wake_up (retrying transient HTTP errors) and wait /health 200.

        Mirrors ``_sleep_engine``: a wake can transiently return HTTP 500 while
        the engine drains background connector work after a long sleep, so a
        single failure must not immediately tear the card down. Health is
        re-checked between retries because a 500 may still have woken the
        engine.
        """
        base = self._engine_base(pod, self._role_port(config, role))
        name = (pod.get("metadata") or {}).get("name", "")
        # Idempotent: an engine that is already awake (label/runtime divergence
        # after a pod restart or a partial transition) only needs a health check.
        already_awake = False
        try:
            with urlopen(f"{base}/is_sleeping", timeout=5) as response:
                if json.loads(response.read()).get("is_sleeping") is False:
                    already_awake = True
        except Exception:  # noqa: BLE001
            pass
        if not already_awake:
            wake_retries = int(os.environ.get("PD_REBALANCER_WAKE_RETRIES", "3"))
            wake_backoff = float(
                os.environ.get("PD_REBALANCER_WAKE_BACKOFF_SECONDS", "5")
            )
            last_error: Optional[Exception] = None
            for attempt in range(wake_retries):
                self.stamp_heartbeat("rebalancer")
                try:
                    with urlopen(
                        Request(f"{base}/wake_up", method="POST"), timeout=120
                    ) as response:
                        response.read()
                    break
                except HTTPError as error:
                    last_error = error
                    print(
                        f"[pd-rebalancer] {config.name}: {name} {role} wake returned "
                        f"HTTP {error.code}, retry {attempt + 1}/{wake_retries}",
                        flush=True,
                    )
                    time.sleep(wake_backoff * (attempt + 1))
            else:
                raise RuntimeError(
                    f"{name} {role} wake_up failed after {wake_retries} attempts: "
                    f"{last_error}"
                )
        deadline = time.monotonic() + self.ready_timeout
        while time.monotonic() < deadline:
            self.stamp_heartbeat("rebalancer")
            try:
                with urlopen(f"{base}/health", timeout=5) as response:
                    if response.status == 200:
                        break
            except Exception:  # noqa: BLE001
                pass
            time.sleep(self.poll_seconds)
        else:
            raise TimeoutError(f"{name} {role} engine not healthy after wake")
        print(f"[pd-rebalancer] {config.name}: {name} {role} awake", flush=True)

    def _patch_awake_labels(
        self,
        config: ModelConfig,
        pod: dict[str, Any],
        active_role: Optional[str],
    ) -> None:
        name = (pod.get("metadata") or {}).get("name", "")
        self.api.pod_patch_labels(
            name,
            {
                "pd-prefill-awake": "true" if active_role == "prefill" else "false",
                "pd-decode-awake": "true" if active_role == "decode" else "false",
            },
        )
        print(
            f"[pd-rebalancer] {config.name}: {name} labels -> "
            f"pd-prefill-awake={'true' if active_role == 'prefill' else 'false'},"
            f"pd-decode-awake={'true' if active_role == 'decode' else 'false'}",
            flush=True,
        )

    def _flip_pod(
        self,
        config: ModelConfig,
        pod: dict[str, Any],
        to_role: Optional[str],
    ) -> None:
        """Flip one card's active role: sleep the current engine, wake the peer.

        Mutual exclusion is a hard constraint on the shared card: the current
        engine must be sleeping (NPU released) before the peer is woken,
        otherwise CaMem OOM can kill EngineCore.

        Labels are patched after every engine step (not only at the end) so
        Services and ``awake()`` always reflect reality: once the old role is
        asleep the card is reported idle, and only after the peer reports
        healthy is the new role published. If waking the peer fails the card is
        left idle and recorded in ``needs_recreate``; the peer must NOT be
        woken on a card whose engine may have crashed mid-wake (that is the
        crash-cascade path), so rollback skips such cards.
        """
        current = self._pod_active_role(pod)
        if current == to_role:
            return
        flip_started = time.monotonic()
        sleep_seconds = 0.0
        wake_seconds = 0.0
        if current is not None:
            _t0 = time.monotonic()
            self._sleep_engine(config, pod, current)
            sleep_seconds = time.monotonic() - _t0
            self._patch_awake_labels(config, pod, None)
            # Short grace between confirmed sleep and peer wake: driver-side
            # physical page release can lag the is_sleeping=true response.
            wake_grace = float(
                os.environ.get("PD_REBALANCER_WAKE_AFTER_SLEEP_SECONDS", "2")
            )
            if wake_grace > 0:
                time.sleep(wake_grace)
        if to_role is not None:
            _t0 = time.monotonic()
            try:
                self._wake_engine(config, pod, to_role)
            except Exception as error:  # noqa: BLE001
                name = (pod.get("metadata") or {}).get("name", "")
                self.needs_recreate.add(name)
                raise RuntimeError(
                    f"{name} wake to {to_role} failed ({error}); card left idle, "
                    f"pod needs recreation"
                ) from error
            wake_seconds = time.monotonic() - _t0
            self._patch_awake_labels(config, pod, to_role)
        name = (pod.get("metadata") or {}).get("name", "")
        print(
            f"[pd-rebalancer] {config.name}: {name} role flip "
            f"{current if current else 'idle'} -> {to_role if to_role else 'idle'} "
            f"sleep={sleep_seconds:.2f}s wake={wake_seconds:.2f}s "
            f"total={time.monotonic() - flip_started:.2f}s",
            flush=True,
        )

    def _flip_plan(
        self,
        config: ModelConfig,
        target: Replicas,
    ) -> list[tuple[dict[str, Any], Optional[str]]]:
        """Deterministic per-pod plan minimizing flips.

        Keeps pods already in a needed role; remaining pods are flipped to
        prefill first, then decode, then returned to the pre-warm pool (both
        engines asleep).
        """
        pods = sorted(
            (
                p
                for p in self._card_pods(config)
                if self._pod_ip(p)
                and self._pod_ready(p)
                and (p.get("metadata") or {}).get("name") not in self.needs_recreate
            ),
            key=lambda p: (p.get("metadata") or {}).get("name", ""),
        )
        if len(pods) < target.prefill + target.decode:
            flagged = sorted(self.needs_recreate)
            raise RuntimeError(
                f"only {len(pods)} ready dual-engine pods, need "
                f"{target.prefill + target.decode} for P{target.prefill},D{target.decode}"
                + (
                    f"; pods awaiting recreation after failed wake: {flagged}"
                    if flagged
                    else ""
                )
            )
        prefill_pods = [p for p in pods if self._pod_active_role(p) == "prefill"]
        decode_pods = [p for p in pods if self._pod_active_role(p) == "decode"]
        keep_prefill = prefill_pods[: target.prefill]
        keep_decode = decode_pods[: target.decode]
        keep_names = {
            (p.get("metadata") or {}).get("name")
            for p in keep_prefill + keep_decode
        }
        candidates = [p for p in pods if (p.get("metadata") or {}).get("name") not in keep_names]
        need_prefill = max(0, target.prefill - len(keep_prefill))
        need_decode = max(0, target.decode - len(keep_decode))
        plan: list[tuple[dict[str, Any], Optional[str]]] = []
        for pod in candidates[:need_prefill]:
            plan.append((pod, "prefill"))
        for pod in candidates[need_prefill : need_prefill + need_decode]:
            plan.append((pod, "decode"))
        for pod in candidates[need_prefill + need_decode :]:
            if self._pod_active_role(pod) is not None:
                plan.append((pod, None))
        return plan

    def _validate_awake_target(self, config: ModelConfig, target: Replicas) -> None:
        if target.prefill < config.min_prefill or target.decode < config.min_decode:
            raise ValueError("target violates the configured minimum awake replicas")
        if target.prefill + target.decode > config.max_total:
            raise ValueError("target exceeds the fixed Prefill/Decode awake budget")
        if target.prefill + target.decode > config.replicas:
            raise ValueError(
                f"target P{target.prefill},D{target.decode} exceeds the "
                f"{config.replicas} dual-engine pods"
            )

    def _rollback_awake(
        self,
        config: ModelConfig,
        target: Replicas,
        previous: Replicas,
        previous_roles: dict[str, Optional[str]],
        reason: str,
    ) -> None:
        print(
            f"[pd-rebalancer] {config.name}: awake transition to "
            f"P{target.prefill},D{target.decode} failed ({reason}); "
            f"rolling back to P{previous.prefill},D{previous.decode}",
            flush=True,
        )
        try:
            self.drain_proxy(config, False)
            for pod in self._card_pods(config):
                name = (pod.get("metadata") or {}).get("name", "")
                if name in self.needs_recreate:
                    # Wake failed on this card and the engine may have crashed:
                    # never re-wake the peer here. Leave it idle (already done
                    # by _flip_pod) and require pod recreation.
                    continue
                want = previous_roles.get(name)
                if self._pod_active_role(pod) != want:
                    self._flip_pod(config, pod, want)
            with self.lock:
                state = self.targets()
                entry = self._ensure_entry(state, config.name)
                entry["target"] = {"prefill": previous.prefill, "decode": previous.decode}
                entry.pop("transition", None)
                self.api.write_state(self.state_configmap, state)
            print(
                f"[pd-rebalancer] {config.name}: rolled back awake to "
                f"P{previous.prefill},D{previous.decode}",
                flush=True,
            )
        except Exception as rollback_error:  # noqa: BLE001
            self.last_error = (
                f"{config.name} awake transition failed ({reason}); "
                f"awake rollback failed: {rollback_error}"
            )
            with self.lock:
                state = self.targets()
                entry = self._ensure_entry(state, config.name)
                if "transition" in entry:
                    entry["transition"]["rollbackFailed"] = str(rollback_error)
                self.api.write_state(self.state_configmap, state)
            print(
                f"[pd-rebalancer] {config.name}: AWAKE ROLLBACK FAILED: {rollback_error}",
                flush=True,
            )

    def _newly_active_engines(
        self,
        config: ModelConfig,
        previous_roles: dict[str, Optional[str]],
    ) -> list[tuple[str, str]]:
        """(name, role) pairs whose engine became active this transition.

        A freshly woken engine is a cold KV pair for every already-active
        engine of the other role (comms are torn down on sleep and recreated
        lazily), so the gate must run for prefill wake-ups and decode wake-ups
        alike.
        """
        woken: list[tuple[str, str]] = []
        for pod in self._card_pods(config):
            name = (pod.get("metadata") or {}).get("name", "")
            current = self._pod_active_role(pod)
            if current is not None and previous_roles.get(name) != current:
                woken.append((name, current))
        return woken

    def _resolve_served_model(self, base: str, config: ModelConfig) -> str:
        """Discover the model id the proxy/engines accept in the chat body.

        ``config.name`` is the rebalancer's short key (e.g. "qwen"), which can
        differ from the vLLM ``--served-model-name`` ("qwen3-8b") validated by
        the engines; a probe with the wrong id 404s on every attempt and would
        force an avoidable rollback. The proxy relays GET /v1/models to a
        decode engine, so ask it for the served id instead.
        """
        last_error: Optional[Exception] = None
        for attempt in range(3):
            self.stamp_heartbeat("rebalancer")
            try:
                with urlopen(f"{base}/v1/models", timeout=5) as response:
                    data = json.loads(response.read())
                ids = [m.get("id") for m in (data.get("data") or []) if m.get("id")]
                if config.name in ids:
                    return config.name
                if ids:
                    return ids[0]
            except Exception as error:  # noqa: BLE001
                last_error = error
            if attempt < 2:
                time.sleep(1)
        raise RuntimeError(
            f"{config.name} served-model discovery via {base}/v1/models "
            f"failed: {last_error}"
        )

    @staticmethod
    def _chat_body(served_model: str, content: str, max_tokens: int) -> bytes:
        return json.dumps(
            {
                "model": served_model,
                "messages": [{"role": "user", "content": content}],
                "max_tokens": max_tokens,
                "stream": False,
            }
        ).encode()

    def _warmup_kv_path(
        self,
        config: ModelConfig,
        woken: list[tuple[str, str]],
    ) -> None:
        """Pin each freshly woken engine's cold KV pair warm with direct calls.

        The proxy load-balances/sticks across PREFILL endpoints, so a probe
        through the proxy can "succeed" against an already-hot prefill and
        never exercise the woken engine (false positive). Instead, drive the
        real two-phase flow DIRECTLY at the specific pod pair:
          phase 1 -> POST the prompt (max_tokens=1) to the prefill engine that
                     must serve it. A unique per-attempt prompt prefix forces a
                     KV-cache miss so the prefill really computes and publishes
                     into ITS pool;
          phase 2 -> POST the same prompt to the peer decode engine, which then
                     pulls the KV from that prefill. The pull creates the
                     ADXL/HCCL comm lazily; right after wake the two TP workers
                     of a cold prefill can race on HCCL ra port 16666 (CANN
                     503900 / -800 retry storm), so one successful pair proves
                     the comm is warm.
        Budget exhaustion raises -> the caller rolls the flip back instead of
        leaving a degraded topology serving 100s+ latency.
        """
        if not woken:
            return
        proxy_base = self._proxy_base(config)
        served_model = self._resolve_served_model(proxy_base, config)
        pods_by_name = {
            (pod.get("metadata") or {}).get("name"): pod
            for pod in self._card_pods(config)
        }
        active_prefills = [
            pod for pod in self._card_pods(config)
            if self._pod_active_role(pod) == "prefill"
        ]
        active_decodes = [
            pod for pod in self._card_pods(config)
            if self._pod_active_role(pod) == "decode"
        ]
        # One job per woken engine: phase 1 always lands on a prefill engine,
        # phase 2 always lands on a decode engine, so the exercised pair is
        # exactly (woken engine, its peer).
        jobs: list[tuple[dict[str, Any], list[dict[str, Any]]]] = []
        for name, role in sorted(woken):
            pod = pods_by_name.get(name)
            if pod is None:
                raise RuntimeError(f"{name} not found for KV warm-up")
            if role == "prefill":
                if not active_decodes:
                    raise RuntimeError(f"{name} prefill has no active decode to warm")
                jobs.append((pod, active_decodes))
            else:
                if not active_prefills:
                    raise RuntimeError(f"{name} decode has no active prefill to warm")
                for prefill in active_prefills:
                    jobs.append((prefill, [pod]))
        last_error: Optional[Exception] = None
        for job_index, (phase1_pod, phase2_pods) in enumerate(jobs, start=1):
            label = (phase1_pod.get("metadata") or {}).get("name", "?")
            warmed = False
            for attempt in range(1, self.kv_warmup_attempts + 1):
                self.stamp_heartbeat("rebalancer")
                rid = f"pd-warmup-{config.name}-{int(time.time() * 1000)}"
                content = (
                    f"[warm {rid}] "
                    + "The quick brown fox jumps over the lazy dog. " * 138
                )
                started = time.monotonic()
                try:
                    phase1_base = self._engine_base(
                        phase1_pod, self._role_port(config, "prefill")
                    )
                    self._post_chat(
                        f"{phase1_base}/v1/chat/completions",
                        self._chat_body(served_model, content, 1),
                        rid,
                    )
                    for peer in phase2_pods:
                        peer_base = self._engine_base(
                            peer, self._role_port(config, "decode")
                        )
                        self._post_chat(
                            f"{peer_base}/v1/chat/completions",
                            self._chat_body(served_model, content, 16),
                            rid,
                        )
                    elapsed = time.monotonic() - started
                    print(
                        f"[pd-rebalancer] {config.name}: KV warm-up job {job_index}/"
                        f"{len(jobs)} attempt {attempt}/{self.kv_warmup_attempts} "
                        f"OK ({elapsed:.1f}s, prefill={label})",
                        flush=True,
                    )
                    warmed = True
                    break
                except Exception as error:  # noqa: BLE001
                    last_error = error
                    elapsed = time.monotonic() - started
                    print(
                        f"[pd-rebalancer] {config.name}: KV warm-up job {job_index}/"
                        f"{len(jobs)} attempt {attempt}/{self.kv_warmup_attempts} "
                        f"failed after {elapsed:.1f}s "
                        f"({error.__class__.__name__}: {str(error)[:160]})",
                        flush=True,
                    )
                    if attempt < self.kv_warmup_attempts:
                        time.sleep(self.kv_warmup_gap)
            if not warmed:
                raise RuntimeError(
                    f"{config.name} KV path did not warm up (prefill={label}) "
                    f"after {self.kv_warmup_attempts} attempts: {last_error}"
                )

    def _post_chat(self, url: str, body: bytes, rid: str) -> None:
        request = Request(
            url,
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "X-Request-Id": rid,
            },
        )
        with urlopen(request, timeout=self.kv_warmup_timeout) as response:
            response.read()

    def reconcile_awake(self, config: ModelConfig, target: Replicas) -> bool:
        """Warm-standby transition: per-card sleep/wake flips -> verify.

        Any transition that sleeps or flips an awake engine drains first so
        in-flight requests finish before the flip. Waking an engine into a
        role (pure scale-up or a role swap) also drains when the KV warm-up
        gate is enabled: the gate probes the woken engine's cold KV pairs in
        isolation while traffic is paused, and traffic resumes only after the
        pairs are warm (or the flip rolls back). With the gate disabled, pure
        scale-ups keep their old drain-free behaviour.
        """
        previous = self.awake(config)
        if previous == target:
            return True
        self._validate_awake_target(config, target)
        plan = self._flip_plan(config, target)
        needs_drain = any(self._pod_active_role(pod) is not None for pod, _ in plan)
        # A flip that wakes an engine into a role (idle->role, or a role swap)
        # creates cold KV pairs whose first comm creation can race (503900 /
        # ra port 16666). The warm-up gate must run in isolation, so such
        # transitions pause new requests too; traffic resumes only after every
        # woken engine's pairs are provably warm (or the flip rolled back).
        wakes_engine = any(
            to_role is not None and self._pod_active_role(pod) != to_role
            for pod, to_role in plan
        )
        if self.kv_warmup_enabled and wakes_engine:
            needs_drain = True
        previous_roles = {
            (p.get("metadata") or {}).get("name"): self._pod_active_role(p)
            for p in self._card_pods(config)
        }
        if self.dry_run:
            print(
                f"[DRY-RUN] {config.name}: awake P{previous.prefill},D{previous.decode} -> "
                f"P{target.prefill},D{target.decode} would flip "
                + ("(drain-free scale-up) " if plan and not needs_drain else "")
                + ", ".join(
                    f"{p.get('metadata', {}).get('name')}->{r if r else 'idle'}"
                    for p, r in plan
                )
                or "nothing",
                flush=True,
            )
            return True
        self._begin_transition(config.name, target, previous, previous_roles)
        try:
            if needs_drain:
                self.drain_proxy(config, True)
                self.wait_drained(config)
            for pod, to_role in plan:
                self._flip_pod(config, pod, to_role)
            now = self.awake(config)
            if now != target:
                raise RuntimeError(f"awake after transition {now} != {target}")
            # KV-path warm-up gate: still drained (new P/D requests wait at
            # the proxy), so probes are isolated from real traffic and warm
            # each woken engine's cold pairs deterministically. Failure raises
            # -> rollback to the previous topology (never leave a degraded P/D
            # mix serving 100s+ latency). Only after the pairs are warm does
            # traffic resume, and only then is the new topology committed.
            newly_active = self._newly_active_engines(config, previous_roles)
            if self.kv_warmup_enabled and newly_active:
                print(
                    f"[pd-rebalancer] {config.name}: warming KV path after "
                    f"engine wake(s) {', '.join(f'{n}:{r}' for n, r in sorted(newly_active))}",
                    flush=True,
                )
                self._warmup_kv_path(config, newly_active)
            if needs_drain:
                self.drain_proxy(config, False)
            with self.lock:
                state = self.targets()
                entry = self._ensure_entry(state, config.name)
                entry["target"] = {"prefill": target.prefill, "decode": target.decode}
                entry.pop("transition", None)
                self.api.write_state(self.state_configmap, state)
            self.last_error = ""
            print(
                f"[pd-rebalancer] {config.name}: awake P{previous.prefill},D{previous.decode} "
                f"-> P{target.prefill},D{target.decode} applied",
                flush=True,
            )
            return True
        except Exception as error:  # noqa: BLE001
            self.last_error = f"{config.name} awake transition failed: {error}"
            self._rollback_awake(config, target, previous, previous_roles, str(error))
            return False

    @staticmethod
    def target_for(entry: Any) -> Optional[dict[str, Any]]:
        """Resolve the committed target from a state entry (new or legacy shape)."""
        if not isinstance(entry, dict):
            return None
        if "target" in entry:
            target = entry["target"]
            return target if isinstance(target, dict) and target else None
        if "prefill" in entry and "decode" in entry:
            return entry
        return None

    @staticmethod
    def _pod_ready(pod: dict[str, Any]) -> bool:
        status = pod.get("status") or {}
        if status.get("phase") != "Running":
            return False
        for condition in status.get("conditions") or []:
            if condition.get("type") == "Ready":
                return condition.get("status") == "True"
        return False

    def wait_for_role(self, deployment_name: str, replicas: int) -> None:
        """Wait until the Deployment fully converges on ``replicas``.

        Convergence requires the Deployment's own status to settle
        (spec.replicas == ready/updated/available and observedGeneration
        current) AND the live Pod list to match exactly: no terminating Pods
        and every Pod Ready. The Pod-level check matters because Deployment
        status can report readyReplicas == desired while an old terminating
        Pod still occupies its accelerator, which would leave the next
        scale-up Pod Pending.
        """
        deadline = time.monotonic() + self.ready_timeout
        while time.monotonic() < deadline:
            self.stamp_heartbeat("rebalancer")
            deployment = self.api.deployment(deployment_name)
            metadata = deployment.get("metadata") or {}
            status = deployment.get("status") or {}
            desired = self.replicas(deployment)
            generation = int(metadata.get("generation", 0))
            observed = int(status.get("observedGeneration", 0))
            ready = int(status.get("readyReplicas", 0))
            updated = int(status.get("updatedReplicas", ready))
            available = int(status.get("availableReplicas", ready))
            pods = self.api.pods(f"app={deployment_name}")
            live = [
                pod
                for pod in pods
                if not (pod.get("metadata") or {}).get("deletionTimestamp")
            ]
            ready_pods = sum(1 for pod in live if self._pod_ready(pod))
            if (
                desired == replicas
                and ready == replicas
                and updated == replicas
                and available == replicas
                and observed >= generation
            ):
                if (
                    len(pods) == replicas
                    and len(live) == replicas
                    and all(self._pod_ready(pod) for pod in live)
                ):
                    return
            time.sleep(self.poll_seconds)
        raise TimeoutError(
            f"Deployment {deployment_name} did not converge to {replicas} replicas "
            f"(spec={desired} ready={ready} updated={updated} available={available} "
            f"observedGeneration={observed}/{generation} pods={len(pods)} "
            f"live={len(live)} readyPods={ready_pods})"
        )

    # ---- proxy drain handshake ---------------------------------------------

    def _proxy_base(self, config: ModelConfig) -> str:
        return f"http://{config.proxy_service}:{self.proxy_port}"

    def drain_proxy(self, config: ModelConfig, enabled: bool) -> None:
        payload = json.dumps({"enabled": enabled}).encode()
        request = Request(
            f"{self._proxy_base(config)}/drain",
            data=payload,
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            with urlopen(request, timeout=10) as response:
                response.read()
        except Exception as error:  # noqa: BLE001
            raise RuntimeError(
                f"{config.name} proxy drain enabled={enabled} failed: {error}"
            ) from error

    def proxy_status(self, config: ModelConfig) -> dict[str, Any]:
        try:
            with urlopen(f"{self._proxy_base(config)}/status", timeout=10) as response:
                return json.loads(response.read())
        except Exception as error:  # noqa: BLE001
            raise RuntimeError(f"{config.name} proxy status failed: {error}") from error

    def wait_drained(self, config: ModelConfig) -> None:
        """Wait until the proxy has no in-flight prefill or decode requests."""
        deadline = time.monotonic() + self.drain_timeout
        while time.monotonic() < deadline:
            self.stamp_heartbeat("rebalancer")
            status = self.proxy_status(config)
            if (
                float(status.get("prefill_inflight", 1)) == 0
                and float(status.get("decode_inflight", 1)) == 0
            ):
                return
            time.sleep(self.poll_seconds)
        raise TimeoutError(
            f"{config.name} proxy did not drain in-flight requests "
            f"within {self.drain_timeout:.0f}s"
        )

    # ---- capacity preflight -------------------------------------------------

    def _resource_capacity(self) -> list[tuple[str, float]]:
        """Per-node free accelerator cards (allocatable minus requests)."""
        result: list[tuple[str, float]] = []
        for node in self.api.nodes():
            name = (node.get("metadata") or {}).get("name", "")
            if not name:
                continue
            if (node.get("spec") or {}).get("unschedulable"):
                continue
            ready = False
            for condition in (node.get("status") or {}).get("conditions") or []:
                if condition.get("type") == "Ready":
                    ready = condition.get("status") == "True"
            if not ready:
                continue
            allocatable = float(
                ((node.get("status") or {}).get("allocatable") or {})
                .get(self.capacity_resource, 0)
                or 0
            )
            if allocatable <= 0:
                continue
            used = 0.0
            for pod in self.api.pods_on_node(name):
                for container in (pod.get("spec") or {}).get("containers") or []:
                    requests = ((container.get("resources") or {}).get("requests") or {})
                    used += float(requests.get(self.capacity_resource, 0) or 0)
            result.append((name, max(0.0, allocatable - used)))
        return result

    @staticmethod
    def _target_cards(config: ModelConfig, target: Replicas) -> int:
        return target.prefill * config.prefill_tp + target.decode * config.decode_tp

    def preflight(self, config: ModelConfig, target: Replicas) -> None:
        """Reject a target that cannot fit the cluster's accelerator budget."""
        if config.mode == "warmstandby":
            # Flips happen in place on already-scheduled dual-engine pods; no
            # new accelerator cards are consumed.
            return
        if not self.capacity_preflight:
            return
        try:
            free = self._resource_capacity()
        except Exception as error:  # noqa: BLE001
            raise RuntimeError(
                f"{config.name} capacity preflight unavailable (nodes/pods RBAC?): {error}"
            ) from error
        if not free:
            raise CapacityError(
                f"{config.name} capacity preflight: no schedulable Ready node reports "
                f"{self.capacity_resource} capacity"
            )
        free_total = sum(cards for _, cards in free)
        current = self.current(config)
        extra = max(0, self._target_cards(config, target) - self._target_cards(config, current))
        if extra > free_total:
            raise CapacityError(
                f"{config.name} capacity preflight: need {extra:.0f} extra "
                f"{self.capacity_resource} cards but only {free_total:.0f} free "
                f"across {len(free)} nodes"
            )
        # TP>1 roles must co-locate their cards on one node (HCCL heuristic).
        if target.prefill > current.prefill and config.prefill_tp > 1:
            if not any(cards >= config.prefill_tp for _, cards in free):
                raise CapacityError(
                    f"{config.name} capacity preflight: no node has "
                    f"{config.prefill_tp} free cards for a prefill TP replica"
                )
        if target.decode > current.decode and config.decode_tp > 1:
            if not any(cards >= config.decode_tp for _, cards in free):
                raise CapacityError(
                    f"{config.name} capacity preflight: no node has "
                    f"{config.decode_tp} free cards for a decode TP replica"
                )

    # ---- transition lock / rollback ------------------------------------------

    @staticmethod
    def _transition(entry: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
        if isinstance(entry, dict):
            transition = entry.get("transition")
            if isinstance(transition, dict) and transition.get("active"):
                return transition
        return None

    def transition_active(self, model: str) -> bool:
        if model not in self.models:
            raise ValueError(f"unknown P/D model: {model}")
        with self.lock:
            state = self.targets()
            entry = state.get(model)
        return self._transition(entry) is not None

    def _stale_transition(self, transition: Optional[dict[str, Any]]) -> bool:
        if not transition:
            return False
        if transition.get("rollbackFailed"):
            return True
        started = float(transition.get("startedAt", 0))
        horizon = self.drain_timeout + 2 * self.ready_timeout + 60
        return time.time() - started > horizon

    def _begin_transition(
        self,
        model: str,
        target: Replicas,
        previous: Replicas,
        previous_roles: Optional[dict[str, Optional[str]]] = None,
    ) -> None:
        if self.dry_run:
            return
        with self.lock:
            state = self.targets()
            entry = self._ensure_entry(state, model)
            if self._transition(entry):
                raise RuntimeError(f"transition already in progress for {model}")
            entry["transition"] = {
                "active": True,
                "target": {"prefill": target.prefill, "decode": target.decode},
                "previous": {"prefill": previous.prefill, "decode": previous.decode},
                "startedAt": time.time(),
            }
            if previous_roles is not None:
                entry["transition"]["previousRoles"] = previous_roles
            self.api.write_state(self.state_configmap, state)

    def _end_transition(self, model: str) -> None:
        if self.dry_run:
            return
        with self.lock:
            state = self.targets()
            entry = self._ensure_entry(state, model)
            entry.pop("transition", None)
            self.api.write_state(self.state_configmap, state)

    @staticmethod
    def _deployment(config: ModelConfig, role: str) -> str:
        if role == "prefill":
            return config.prefill_deployment
        if role == "decode":
            return config.decode_deployment
        raise ValueError(f"unknown P/D role: {role}")

    def _rollback_transition(
        self,
        config: ModelConfig,
        target: Replicas,
        previous: Replicas,
        reason: str,
    ) -> None:
        print(
            f"[pd-rebalancer] {config.name}: transition to "
            f"P{target.prefill},D{target.decode} failed ({reason}); "
            f"rolling back to P{previous.prefill},D{previous.decode}",
            flush=True,
        )
        try:
            self.drain_proxy(config, False)
            current = self.current(config)
            if current != previous:
                plan = transition_plan(current, previous, config)
                for role, replicas in plan:
                    self.api.scale(self._deployment(config, role), replicas)
                    self.wait_for_role(self._deployment(config, role), replicas)
            with self.lock:
                state = self.targets()
                entry = self._ensure_entry(state, config.name)
                entry["target"] = {"prefill": previous.prefill, "decode": previous.decode}
                entry.pop("transition", None)
                self.api.write_state(self.state_configmap, state)
            print(
                f"[pd-rebalancer] {config.name}: rolled back to "
                f"P{previous.prefill},D{previous.decode}",
                flush=True,
            )
        except Exception as rollback_error:  # noqa: BLE001
            self.last_error = (
                f"{config.name} transition failed ({reason}); rollback failed: {rollback_error}"
            )
            with self.lock:
                state = self.targets()
                entry = self._ensure_entry(state, config.name)
                if "transition" in entry:
                    entry["transition"]["rollbackFailed"] = str(rollback_error)
                self.api.write_state(self.state_configmap, state)
            print(
                f"[pd-rebalancer] {config.name}: ROLLBACK FAILED: {rollback_error}",
                flush=True,
            )

    def reconcile_model(self, config: ModelConfig, target: Replicas) -> bool:
        """Apply one P/D transition with drain, capacity preflight and rollback."""
        if config.mode == "warmstandby":
            return self.reconcile_awake(config, target)
        previous = self.current(config)
        if previous == target:
            return True
        plan = transition_plan(previous, target, config)
        if self.dry_run:
            print(
                f"[DRY-RUN] {config.name}: P{previous.prefill},D{previous.decode} -> "
                f"P{target.prefill},D{target.decode} would apply {plan}",
                flush=True,
            )
            return True
        self._begin_transition(config.name, target, previous)
        try:
            self.drain_proxy(config, True)
            self.wait_drained(config)
            now = previous
            for role, replicas in plan:
                deployment = self._deployment(config, role)
                scaling_up = replicas > getattr(now, role)
                if scaling_up:
                    self.preflight(config, target)
                self.api.scale(deployment, replicas)
                try:
                    self.wait_for_role(deployment, replicas)
                except TimeoutError as error:
                    if scaling_up:
                        print(
                            f"[pd-rebalancer] WARNING {config.name}: {role} scale-up "
                            f"to {replicas} did not converge within "
                            f"{self.ready_timeout:.0f}s ({error}); temporary capacity "
                            f"dip for the target topology - rolling back and retrying "
                            f"on the next poll",
                            flush=True,
                        )
                    raise
                now = self.current(config)
            self.drain_proxy(config, False)
            with self.lock:
                state = self.targets()
                entry = self._ensure_entry(state, config.name)
                entry["target"] = {"prefill": target.prefill, "decode": target.decode}
                entry.pop("transition", None)
                self.api.write_state(self.state_configmap, state)
            self.last_error = ""
            print(
                f"[pd-rebalancer] {config.name}: P{previous.prefill},D{previous.decode} "
                f"-> P{target.prefill},D{target.decode} applied",
                flush=True,
            )
            return True
        except Exception as error:  # noqa: BLE001
            self.last_error = f"{config.name} transition failed: {error}"
            self._rollback_transition(config, target, previous, str(error))
            return False

    def targets(self) -> dict[str, Any]:
        state = self.api.state(self.state_configmap)
        return json.loads(state.get("data", {}).get("targets.json", "{}"))

    @staticmethod
    def _ensure_entry(state: dict[str, Any], model: str) -> dict[str, Any]:
        """Return a dict entry with a committed ``target`` key, migrating legacy state."""
        entry = state.get(model)
        if not isinstance(entry, dict):
            entry = {}
            state[model] = entry
        if "target" not in entry:
            legacy = {key: value for key, value in entry.items() if key in ("prefill", "decode")}
            entry = {"target": legacy}
            state[model] = entry
        return entry

    def propose(self, model: str, target: Replicas, reason: str = "") -> None:
        if model not in self.models:
            raise ValueError(f"unknown P/D model: {model}")
        if self.transition_active(model):
            raise RuntimeError(f"transition already in progress for {model}")
        transition_plan(self.current(self.models[model]), target, self.models[model])
        with self.lock:
            state = self.targets()
            entry = self._ensure_entry(state, model)
            entry["proposed"] = {
                "prefill": target.prefill,
                "decode": target.decode,
                "reason": reason,
                "proposedAt": time.time(),
            }
            self.api.write_state(self.state_configmap, state)

    def commit(self, model: str) -> None:
        if model not in self.models:
            raise ValueError(f"unknown P/D model: {model}")
        if self.transition_active(model):
            raise RuntimeError(f"transition already in progress for {model}")
        with self.lock:
            state = self.targets()
            entry = state.get(model)
            proposed = entry.get("proposed") if isinstance(entry, dict) else None
            if not isinstance(proposed, dict) or "prefill" not in proposed or "decode" not in proposed:
                raise ValueError(f"no proposed target for P/D model: {model}")
            target = Replicas(prefill=int(proposed["prefill"]), decode=int(proposed["decode"]))
            transition_plan(self.current(self.models[model]), target, self.models[model])
            if self.models[model].mode == "warmstandby":
                self._validate_awake_target(self.models[model], target)
        self.preflight(self.models[model], target)
        with self.lock:
            state = self.targets()
            entry = state.get(model)
            proposed = entry.get("proposed") if isinstance(entry, dict) else None
            if not isinstance(proposed, dict) or "prefill" not in proposed or "decode" not in proposed:
                raise ValueError(f"no proposed target for P/D model: {model}")
            current_target = Replicas(prefill=int(proposed["prefill"]), decode=int(proposed["decode"]))
            if current_target != target:
                raise RuntimeError(
                    f"proposed target changed while committing for {model}: "
                    f"{current_target} != {target}"
                )
            entry = self._ensure_entry(state, model)
            entry["target"] = {"prefill": target.prefill, "decode": target.decode}
            entry.pop("proposed", None)
            self.api.write_state(self.state_configmap, state)

    def discard(self, model: str) -> None:
        if model not in self.models:
            raise ValueError(f"unknown P/D model: {model}")
        if self.transition_active(model):
            raise RuntimeError(f"transition already in progress for {model}")
        with self.lock:
            state = self.targets()
            entry = state.get(model)
            if isinstance(entry, dict) and "proposed" in entry:
                entry.pop("proposed")
                self.api.write_state(self.state_configmap, state)

    def status(self, model: str) -> dict[str, Any]:
        if model not in self.models:
            raise ValueError(f"unknown P/D model: {model}")
        config = self.models[model]
        current = self.current(config)
        entry = self.targets().get(model)
        proposed = entry.get("proposed") if isinstance(entry, dict) else None
        return {
            "model": model,
            "current": {"prefill": current.prefill, "decode": current.decode},
            "pool": {"cards": self.pool(config)},
            "proposed": proposed,
            "target": self.target_for(entry),
            "transition": self._transition(entry),
        }

    def run(self, max_iters: Optional[int] = None) -> None:
        iterations = 0
        while True:
            self.stamp_heartbeat("rebalancer")
            first_pass = self._first_pass
            self._first_pass = False
            try:
                ok = True
                for name, raw_target in self.targets().items():
                    if name not in self.models:
                        continue
                    target_entry = self.target_for(raw_target)
                    if target_entry is None:
                        continue
                    target = Replicas(
                        prefill=int(target_entry["prefill"]),
                        decode=int(target_entry["decode"]),
                    )
                    with self.lock:
                        state = self.targets()
                        entry = state.get(name)
                        transition = self._transition(entry) if isinstance(entry, dict) else None
                    if transition and (first_pass or self._stale_transition(transition)):
                        previous_raw = transition.get("previous") or {}
                        previous = Replicas(
                            prefill=int(previous_raw.get("prefill", target.prefill)),
                            decode=int(previous_raw.get("decode", target.decode)),
                        )
                        print(
                            f"[pd-rebalancer] {name}: stale transition detected; "
                            f"rolling back to P{previous.prefill},D{previous.decode}",
                            flush=True,
                        )
                        model = self.models[name]
                        reason = f"stale transition (startedAt={transition.get('startedAt')})"
                        if model.mode == "warmstandby":
                            previous_roles = {
                                str(k): v for k, v in (transition.get("previousRoles") or {}).items()
                            }
                            self._rollback_awake(model, target, previous, previous_roles, reason)
                        else:
                            self._rollback_transition(model, target, previous, reason)
                        ok = False
                        continue
                    if transition:
                        continue  # in progress (this instance or a live peer)
                    if not self.reconcile_model(self.models[name], target):
                        ok = False
                if ok:
                    self.last_error = ""
            except Exception as error:
                self.last_error = str(error)
                print(f"pd-rebalancer reconcile failed: {error}", flush=True)
            time.sleep(self.poll_seconds)
            iterations += 1
            if max_iters is not None and iterations >= max_iters:
                return


class PlannerLoop:
    """Advisory/automatic Planner integration for the rebalancer.

    Scrapes role-level engine metrics, runs ``pd_planner.decide()`` and, unless
    advisory mode is on, proposes + commits the recommended target through the
    same two-phase path a human would use. Advisory mode only logs the
    recommendation without proposing or committing a target.

    Signal derivation follows ``docs/architecture/pd-metrics-contract.md``:
      decode_kv_usage_percent = max(kv_cache_usage_perc) * 100 over decode
                                pods, clamped into [0, 100] so a stray
                                out-of-range gauge cannot abort the poll cycle
      prefill_backlog_tokens  = sum(pd_proxy_prefill_inflight) over P/D proxy
                                pods * mean prompt tokens observed by the proxy
                                (prompt_tokens_total / requests_total)

    The prefill signal is intentionally taken from the LLM-LA P/D proxy rather
    than the engine: the proxy is the component that holds requests while they
    wait for prefill, and the engine-side queue gauges are step-boundary
    snapshots that do not reflect proxy-side waiting requests.
    """

    def __init__(self, rebalancer: "Rebalancer") -> None:
        self.rebalancer = rebalancer
        self.advisory = os.environ.get("PD_REBALANCER_ADVISORY", "true").lower() in (
            "1", "true", "yes",
        )
        self.poll_seconds = float(os.environ.get("PD_REBALANCER_PLANNER_POLL_SECONDS", "5"))
        self.metrics_port = int(os.environ.get("PD_REBALANCER_METRICS_PORT", "8200"))
        self.proxy_metrics_port = int(
            os.environ.get("PD_REBALANCER_PROXY_METRICS_PORT", "8200")
        )
        # Whole-scrape cap for one planner poll. Fetches run concurrently, so
        # this keeps the poll inside its interval even when a pod hangs until
        # its own per-request timeout.
        self.scrape_deadline = float(
            os.environ.get(
                "PD_REBALANCER_PLANNER_SCRAPE_DEADLINE_SECONDS",
                str(self.poll_seconds),
            )
        )
        self.config_overrides = json.loads(os.environ.get("PD_REBALANCER_PLANNER_CONFIG", "{}") or "{}")
        self.last_error = ""

    # ---- metrics scraping -------------------------------------------------

    @staticmethod
    def _gauge_values(text: str, name: str) -> list[float]:
        # Engine metrics carry labels ("vllm:...{engine=\"0\",...} 1.0"), while
        # the P/D proxy metrics are unlabeled ("pd_proxy_prefill_inflight 43").
        labeled_prefix = f"{name}{{"
        unlabeled_prefix = f"{name} "
        values: list[float] = []
        for line in text.splitlines():
            stripped = line.strip()
            if not (
                stripped.startswith(labeled_prefix)
                or stripped.startswith(unlabeled_prefix)
            ):
                continue
            try:
                values.append(float(stripped.rsplit(" ", 1)[1]))
            except (ValueError, IndexError):
                continue
        return values

    def _fetch(self, ip: str, port: Optional[int] = None) -> Optional[str]:
        """Fetch /metrics from one pod; None on connection/timeout errors so a
        single terminating pod cannot abort the whole planner poll. Batch calls
        go through ``_fetch_many`` so one poll stays inside its deadline."""
        self.rebalancer.stamp_heartbeat("planner")
        url = f"http://{ip}:{port or self.metrics_port}/metrics"
        try:
            with urllib.request.urlopen(url, timeout=8) as response:
                return response.read().decode("utf-8", errors="replace")
        except Exception:  # noqa: BLE001
            return None

    def _fetch_many(
        self, targets: list[tuple[str, Optional[int]]]
    ) -> dict[tuple[str, Optional[int]], Optional[str]]:
        """Fetch /metrics from several pods concurrently, bounded by one
        deadline.

        Each request keeps its own timeout, but the whole batch is capped at
        ``self.scrape_deadline`` so a few slow or terminating pods cannot push
        one planner poll far past its interval.  Targets that miss the deadline
        are treated as unavailable for that round and skipped by the callers;
        their background requests finish on their own timeout and are
        discarded.
        """
        deadline = time.monotonic() + max(0.0, self.scrape_deadline)
        results: dict[tuple[str, Optional[int]], Optional[str]] = {}
        pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=max(1, min(32, len(targets)))
        )
        try:
            futures = {
                pool.submit(self._fetch, ip, port): (ip, port)
                for ip, port in targets
            }
            try:
                for future in concurrent.futures.as_completed(
                    futures, timeout=deadline - time.monotonic()
                ):
                    key = futures[future]
                    try:
                        results[key] = future.result()
                    except Exception:  # noqa: BLE001
                        results[key] = None
            except concurrent.futures.TimeoutError:
                pass
        finally:
            # Never block the poll on stragglers: pending fetches are cancelled
            # and in-flight HTTP requests finish on their own request timeout.
            pool.shutdown(wait=False, cancel_futures=True)
        return results

    def _scrape(self, ip: str, metric: str, port: Optional[int] = None) -> Optional[float]:
        text = self._fetch(ip, port)
        if text is None:
            return None
        values = self._gauge_values(text, metric)
        # A pod can expose one series per engine (dual-engine warm standby).
        # Max keeps the value a fraction so decode_kv = max(...) * 100 stays
        # within 0..100; sum could inflate it past 100 and trip validation.
        return max(values) if values else None

    def _proxy_signal(self, ips: list[str]) -> tuple[float, float, float, int]:
        """Aggregate P/D proxy prefill metrics over proxy pods.

        One concurrent HTTP fetch per pod (all series parsed from the same
        snapshot), bounded by the per-poll scrape deadline. Returns
        (in_flight_requests, effective_mean_prompt_tokens, requests_total,
        ok_pods).

        mean_prompt_tokens is a per-pod sliding-window mean, so the aggregate
        is the inflight-weighted average: sum(mean_i * inflight_i) /
        sum(inflight_i).  That is equivalent to summing each pod's token
        backlog directly and keeps a pod that is actually holding requests
        from being diluted (or inflated) by an idle peer's window mean.
        requests_total stays an unweighted sum because it only gates the
        config fallback when no pod has measured usage yet.
        """
        inflight = 0.0
        weighted_tokens = 0.0
        requests = 0.0
        ok = 0
        for (_ip, _port), text in self._fetch_many(
            [(ip, self.proxy_metrics_port) for ip in ips]
        ).items():
            if text is None:
                continue
            ok += 1
            pod_inflight = sum(self._gauge_values(text, "pd_proxy_prefill_inflight"))
            pod_mean = sum(self._gauge_values(text, "pd_proxy_prefill_mean_prompt_tokens"))
            pod_requests = sum(self._gauge_values(text, "pd_proxy_prefill_requests_total"))
            inflight += pod_inflight
            weighted_tokens += pod_inflight * pod_mean
            requests += pod_requests
        mean = weighted_tokens / inflight if inflight else 0.0
        return inflight, mean, requests, ok

    def _endpoint_ips(self, service: str) -> list[str]:
        data = self.rebalancer.api.request(
            "GET", f"/api/v1/namespaces/{self.rebalancer.namespace}/endpoints/{service}"
        )
        ips: list[str] = []
        for subset in data.get("subsets", []):
            for address in subset.get("addresses", []):
                ip = address.get("ip")
                if ip:
                    ips.append(ip)
        return ips

    # ---- planner state ------------------------------------------------------

    @staticmethod
    def _state_key(model: str) -> str:
        return f"planner_state.{model}.json"

    def _load_state(self, model: str):
        from pd_planner import PlannerState

        cm = self.rebalancer.api.state(self.rebalancer.state_configmap)
        data = cm.get("data", {})
        # Per-model key; fall back to the legacy single-model key for smooth
        # upgrades of clusters that already have planner_state.json.
        raw = data.get(self._state_key(model)) or data.get("planner_state.json", "{}")
        try:
            return PlannerState.from_dict(json.loads(raw))
        except Exception:  # noqa: BLE001
            return PlannerState()

    def _save_state(self, model: str, state) -> None:
        key = self._state_key(model)
        cm = self.rebalancer.api.state(self.rebalancer.state_configmap)
        data = cm.get("data", {})
        if key in data:
            try:
                if json.loads(data[key]) == state.to_dict():
                    return  # no change: avoid a ConfigMap PATCH every poll
            except Exception:  # noqa: BLE001
                pass
        self.rebalancer.api.patch_data(
            self.rebalancer.state_configmap,
            {key: json.dumps(state.to_dict(), sort_keys=True)},
        )

    def _config(self):
        from pd_planner import PlannerConfig

        config = PlannerConfig()
        field_types = get_type_hints(PlannerConfig)
        warned = getattr(self, "_warned_config_keys", None)
        if warned is None:
            warned = self._warned_config_keys = set()
        for key, value in self.config_overrides.items():
            expected = field_types.get(key)
            if expected is None:
                marker = (key, repr(value))
                if marker not in warned:
                    warned.add(marker)
                    print(
                        f"[planner] ignoring unknown planner config key {key!r}={value!r}; "
                        "check pdRebalancer.plannerConfig for typos",
                        flush=True,
                    )
                continue
            try:
                converted = self._coerce_planner_value(value, expected)
            except (TypeError, ValueError) as error:
                marker = (key, repr(value))
                if marker not in warned:
                    warned.add(marker)
                    print(
                        f"[planner] ignoring planner config {key!r}={value!r}: {error}",
                        flush=True,
                    )
                continue
            config = dataclasses.replace(config, **{key: converted})
        return config

    @staticmethod
    def _coerce_planner_value(value: Any, expected: type) -> Any:
        """Coerce one planner override strictly so bad values fail loudly
        instead of silently truncating (e.g. 2.9 -> 2 for an int field)."""
        if expected is bool:
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                normalized = value.strip().lower()
                if normalized in ("true", "1", "yes", "on"):
                    return True
                if normalized in ("false", "0", "no", "off"):
                    return False
            raise ValueError(f"expected a boolean, got {value!r}")
        if expected is int:
            if isinstance(value, bool):  # bool is an int subclass
                raise ValueError(f"expected an integer, got boolean {value!r}")
            if isinstance(value, int):
                return value
            if isinstance(value, float):
                if not value.is_integer():
                    raise ValueError(
                        f"expected an integer, got non-integral {value!r}"
                    )
                return int(value)
            if isinstance(value, str):
                return int(value)
            raise ValueError(f"expected an integer, got {value!r}")
        if expected is float:
            if isinstance(value, bool):
                raise ValueError(f"expected a number, got boolean {value!r}")
            if isinstance(value, (int, float)):
                return float(value)
            if isinstance(value, str):
                return float(value)
            raise ValueError(f"expected a number, got {value!r}")
        if expected is str:
            return str(value)
        raise ValueError(f"unsupported planner config field type {expected!r}")

    # ---- decision loop ------------------------------------------------------

    def run_once(self) -> None:
        from pd_planner import MetricsSnapshot, decide

        self.rebalancer.stamp_heartbeat("planner")
        config = self._config()
        for name, model in self.rebalancer.models.items():
            proxy_ips = self._endpoint_ips(model.proxy_service)
            decode_ips = self._endpoint_ips(model.decode_deployment)
            if not proxy_ips or not decode_ips:
                print(
                    f"[planner] {name}: no ready endpoints "
                    f"(proxy={len(proxy_ips)} d={len(decode_ips)}), skip",
                    flush=True,
                )
                continue

            p_inflight, p_mean, p_requests, p_ok = self._proxy_signal(proxy_ips)
            if p_ok == 0:
                print(
                    f"[planner] {name}: proxy metrics unavailable "
                    f"({p_ok}/{len(proxy_ips)} pods), skip",
                    flush=True,
                )
                continue
            mean_tokens = (
                p_mean
                if p_requests
                else config.prefill_mean_prompt_tokens_fallback
            )
            backlog = p_inflight * mean_tokens

            kv_values: list[float] = []
            for (_ip, _port), text in self._fetch_many(
                [(ip, model.decode_port) for ip in decode_ips]
            ).items():
                if text is None:
                    continue
                values = self._gauge_values(text, "vllm:kv_cache_usage_perc")
                if values:
                    kv_values.append(max(values))
            if not kv_values:
                print(
                    f"[planner] {name}: decode metrics unavailable "
                    f"({len(kv_values)}/{len(decode_ips)} pods), skip",
                    flush=True,
                )
                continue
            # A broken gauge (e.g. above 1.0) must not kill the whole poll
            # cycle: clamp into 0..100 before building the snapshot so the
            # planner still runs and treats the value as full pressure.
            decode_kv = max(0.0, min(max(kv_values) * 100.0, 100.0))

            current = self.rebalancer.current(model)
            state = self._load_state(name)
            decision = decide(
                current,
                MetricsSnapshot(
                    prefill_backlog_tokens=backlog,
                    decode_kv_usage_percent=decode_kv,
                ),
                config,
                state,
                time.time(),
            )
            self._save_state(name, decision.state)
            self.last_error = ""

            if decision.target is None:
                print(
                    f"[planner] {name}: {decision.reason} "
                    f"(inflight={p_inflight:.0f} mean_tokens={mean_tokens:.0f} "
                    f"backlog={backlog:.0f} kv={decode_kv:.1f}%)",
                    flush=True,
                )
                continue

            target = decision.target
            if self.advisory:
                print(
                    f"[planner:advisory] {name}: recommend P{target.prefill},D{target.decode} "
                    f"({decision.reason}); not applied",
                    flush=True,
                )
                continue

            if self.rebalancer.transition_active(name):
                print(
                    f"[planner] {name}: transition in progress, deferring "
                    f"P{target.prefill},D{target.decode}",
                    flush=True,
                )
                continue

            print(
                f"[planner:auto] {name}: applying P{target.prefill},D{target.decode} "
                f"({decision.reason})",
                flush=True,
            )
            self.rebalancer.propose(name, target, decision.reason)
            self.rebalancer.commit(name)

    def run(self) -> None:
        while True:
            started = time.monotonic()
            try:
                self.run_once()
                self.last_error = ""
                self.rebalancer.planner_error = ""
            except Exception as error:  # noqa: BLE001
                self.last_error = str(error)
                self.rebalancer.planner_error = str(error)
                print(f"pd-planner failed: {error}", flush=True)
            elapsed = time.monotonic() - started
            # Fixed cadence: skip the sleep entirely when a poll already ran
            # past its interval instead of compounding the drift.
            time.sleep(max(0.0, self.poll_seconds - elapsed))


def handler_for(rebalancer: Rebalancer):
    api_token = os.environ.get("PD_REBALANCER_API_TOKEN", "")

    class Handler(BaseHTTPRequestHandler):
        def send_json(self, status: int, payload: dict[str, Any]) -> None:
            encoded = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def authorized(self) -> bool:
            return not api_token or self.headers.get("X-Auth-Token", "") == api_token

        def do_GET(self) -> None:
            if self.path != "/healthz":
                if not self.authorized():
                    self.send_json(401, {"error": "unauthorized"})
                    return
                prefix = "/v1/targets/"
                if not self.path.startswith(prefix):
                    self.send_json(404, {"error": "not found"})
                    return
                model = self.path[len(prefix):]
                try:
                    self.send_json(200, rebalancer.status(model))
                except ValueError as error:
                    self.send_json(404, {"error": str(error)})
                return
            stale = [
                name
                for name in ("rebalancer", "planner")
                if rebalancer.heartbeat_stale(name)
            ]
            self.send_json(200 if not stale else 503, {
                "status": "ok" if not stale else "unhealthy",
                "dryRun": rebalancer.dry_run,
                "lastError": rebalancer.last_error,
                "plannerError": rebalancer.planner_error,
                "heartbeatTimeout": rebalancer.heartbeat_timeout,
                "heartbeats": {
                    name: round(rebalancer.heartbeat_age(name), 1)
                    for name in ("rebalancer", "planner")
                },
                "staleLoops": stale,
            })

        def do_POST(self) -> None:
            if not self.authorized():
                self.send_json(401, {"error": "unauthorized"})
                return
            prefix = "/v1/targets/"
            if not self.path.startswith(prefix):
                self.send_json(404, {"error": "not found"})
                return
            parts = [part for part in self.path[len(prefix):].split("/") if part]
            if len(parts) < 1 or len(parts) > 2:
                self.send_json(404, {"error": "not found"})
                return
            model, action = parts[0], (parts[1] if len(parts) == 2 else "propose")
            try:
                if action == "propose":
                    length = int(self.headers.get("Content-Length", "0"))
                    payload = json.loads(self.rfile.read(length))
                    target = Replicas(prefill=int(payload["prefill"]), decode=int(payload["decode"]))
                    reason = str(payload.get("reason", ""))
                    rebalancer.propose(model, target, reason)
                    self.send_json(202, {
                        "status": "accepted",
                        "model": model,
                        "proposed": {"prefill": target.prefill, "decode": target.decode},
                    })
                elif action == "commit":
                    rebalancer.commit(model)
                    self.send_json(202, {"status": "accepted", "model": model, "committed": True})
                elif action == "discard":
                    rebalancer.discard(model)
                    self.send_json(202, {"status": "accepted", "model": model, "discarded": True})
                else:
                    self.send_json(404, {"error": "not found"})
            except (KeyError, TypeError, ValueError) as error:
                self.send_json(400, {"error": str(error)})
            except CapacityError as error:
                self.send_json(409, {"error": str(error)})
            except RuntimeError as error:
                self.send_json(409, {"error": str(error)})
            except Exception as error:
                self.send_json(500, {"error": str(error)})

        def log_message(self, *_: Any) -> None:
            return

    return Handler


def main() -> None:
    rebalancer = Rebalancer()
    threading.Thread(target=rebalancer.run, daemon=True).start()
    planner = PlannerLoop(rebalancer)
    threading.Thread(target=planner.run, daemon=True).start()
    port = int(os.environ.get("PD_REBALANCER_PORT", "8081"))
    ThreadingHTTPServer(("0.0.0.0", port), handler_for(rebalancer)).serve_forever()


if __name__ == "__main__":
    main()
