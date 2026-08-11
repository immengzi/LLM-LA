#!/usr/bin/env python3
"""Quiescent, fixed-budget Prefill/Decode Deployment rebalancer."""

import json
import os
import ssl
import threading
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Optional
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
    min_prefill: int
    min_decode: int
    max_total: int


def transition_plan(current: Replicas, target: Replicas, config: ModelConfig) -> list[tuple[str, int]]:
    if target.prefill < config.min_prefill or target.decode < config.min_decode:
        raise ValueError("target violates the configured minimum replicas")
    if target.prefill + target.decode > config.max_total:
        raise ValueError("target exceeds the fixed Prefill/Decode replica budget")

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

    def request(self, method: str, path: str, body: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        payload = None if body is None else json.dumps(body).encode()
        request = Request(
            f"{self.base_url}{path}",
            data=payload,
            method=method,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/json",
                "Content-Type": "application/merge-patch+json",
            },
        )
        try:
            with urlopen(request, context=self.context, timeout=15) as response:
                return json.loads(response.read())
        except HTTPError as error:
            detail = error.read().decode(errors="replace")
            raise RuntimeError(f"Kubernetes API {method} {path} failed: {error.code} {detail}") from error

    def deployment(self, name: str) -> dict[str, Any]:
        return self.request("GET", f"/apis/apps/v1/namespaces/{self.namespace}/deployments/{name}")

    def scale(self, name: str, replicas: int) -> None:
        self.request(
            "PATCH",
            f"/apis/apps/v1/namespaces/{self.namespace}/deployments/{name}/scale",
            {"spec": {"replicas": replicas}},
        )

    def state(self, name: str) -> dict[str, Any]:
        return self.request("GET", f"/api/v1/namespaces/{self.namespace}/configmaps/{name}")

    def write_state(self, name: str, targets: dict[str, Any]) -> None:
        self.request(
            "PATCH",
            f"/api/v1/namespaces/{self.namespace}/configmaps/{name}",
            {"data": {"targets.json": json.dumps(targets, sort_keys=True)}},
        )


class Rebalancer:
    def __init__(self) -> None:
        self.namespace = os.environ["POD_NAMESPACE"]
        self.state_configmap = os.environ["PD_REBALANCER_STATE_CONFIGMAP"]
        self.poll_seconds = float(os.environ.get("PD_REBALANCER_POLL_SECONDS", "2"))
        self.ready_timeout = float(os.environ.get("PD_REBALANCER_READY_TIMEOUT_SECONDS", "900"))
        self.dry_run = os.environ.get("PD_REBALANCER_DRY_RUN", "").lower() in ("1", "true", "yes")
        self.models = {
            item["name"]: ModelConfig(
                name=item["name"],
                prefill_deployment=item["prefillDeployment"],
                decode_deployment=item["decodeDeployment"],
                min_prefill=int(item["minPrefillReplicas"]),
                min_decode=int(item["minDecodeReplicas"]),
                max_total=int(item["maxTotalReplicas"]),
            )
            for item in json.loads(os.environ["PD_REBALANCER_MODELS_JSON"])
        }
        self.api = KubernetesApi(self.namespace)
        self.lock = threading.Lock()
        self.last_error = ""

    @staticmethod
    def replicas(deployment: dict[str, Any]) -> int:
        return int(deployment.get("spec", {}).get("replicas", 0))

    @staticmethod
    def ready_replicas(deployment: dict[str, Any]) -> int:
        return int(deployment.get("status", {}).get("readyReplicas", 0))

    def current(self, config: ModelConfig) -> Replicas:
        return Replicas(
            prefill=self.replicas(self.api.deployment(config.prefill_deployment)),
            decode=self.replicas(self.api.deployment(config.decode_deployment)),
        )

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

    def wait_for_role(self, deployment_name: str, replicas: int) -> None:
        deadline = time.monotonic() + self.ready_timeout
        while time.monotonic() < deadline:
            deployment = self.api.deployment(deployment_name)
            desired = self.replicas(deployment)
            ready = self.ready_replicas(deployment)
            if desired == replicas and ready == replicas:
                return
            time.sleep(self.poll_seconds)
        raise TimeoutError(f"Deployment {deployment_name} did not converge to {replicas} replicas")

    def reconcile_model(self, config: ModelConfig, target: Replicas) -> None:
        current = self.current(config)
        plan = transition_plan(current, target, config)
        if self.dry_run:
            print(
                f"[DRY-RUN] {config.name}: P{current.prefill},D{current.decode} -> "
                f"P{target.prefill},D{target.decode} would apply {plan}",
                flush=True,
            )
            return
        for role, replicas in plan:
            deployment = config.prefill_deployment if role == "prefill" else config.decode_deployment
            self.api.scale(deployment, replicas)
            self.wait_for_role(deployment, replicas)
            current = self.current(config)

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
        with self.lock:
            state = self.targets()
            entry = state.get(model)
            proposed = entry.get("proposed") if isinstance(entry, dict) else None
            if not isinstance(proposed, dict) or "prefill" not in proposed or "decode" not in proposed:
                raise ValueError(f"no proposed target for P/D model: {model}")
            target = Replicas(prefill=int(proposed["prefill"]), decode=int(proposed["decode"]))
            transition_plan(self.current(self.models[model]), target, self.models[model])
            entry = self._ensure_entry(state, model)
            entry["target"] = {"prefill": target.prefill, "decode": target.decode}
            entry.pop("proposed", None)
            self.api.write_state(self.state_configmap, state)

    def discard(self, model: str) -> None:
        if model not in self.models:
            raise ValueError(f"unknown P/D model: {model}")
        with self.lock:
            state = self.targets()
            entry = state.get(model)
            if isinstance(entry, dict) and "proposed" in entry:
                entry.pop("proposed")
                self.api.write_state(self.state_configmap, state)

    def status(self, model: str) -> dict[str, Any]:
        if model not in self.models:
            raise ValueError(f"unknown P/D model: {model}")
        current = self.current(self.models[model])
        entry = self.targets().get(model)
        proposed = entry.get("proposed") if isinstance(entry, dict) else None
        return {
            "model": model,
            "current": {"prefill": current.prefill, "decode": current.decode},
            "proposed": proposed,
            "target": self.target_for(entry),
        }

    def run(self) -> None:
        while True:
            try:
                for name, raw_target in self.targets().items():
                    if name not in self.models:
                        continue
                    target_entry = self.target_for(raw_target)
                    if target_entry is None:
                        continue
                    self.reconcile_model(
                        self.models[name],
                        Replicas(prefill=int(target_entry["prefill"]), decode=int(target_entry["decode"])),
                    )
                self.last_error = ""
            except Exception as error:
                self.last_error = str(error)
                print(f"pd-rebalancer reconcile failed: {error}", flush=True)
            time.sleep(self.poll_seconds)


def handler_for(rebalancer: Rebalancer):
    class Handler(BaseHTTPRequestHandler):
        def send_json(self, status: int, payload: dict[str, Any]) -> None:
            encoded = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def do_GET(self) -> None:
            if self.path != "/healthz":
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
            self.send_json(200, {"status": "ok", "dryRun": rebalancer.dry_run, "lastError": rebalancer.last_error})

        def do_POST(self) -> None:
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
            except Exception as error:
                self.send_json(500, {"error": str(error)})

        def log_message(self, *_: Any) -> None:
            return

    return Handler


def main() -> None:
    rebalancer = Rebalancer()
    threading.Thread(target=rebalancer.run, daemon=True).start()
    port = int(os.environ.get("PD_REBALANCER_PORT", "8081"))
    ThreadingHTTPServer(("0.0.0.0", port), handler_for(rebalancer)).serve_forever()


if __name__ == "__main__":
    main()
