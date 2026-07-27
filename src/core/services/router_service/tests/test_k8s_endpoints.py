# tests/test_k8s_endpoints.py
# -*- coding: utf-8 -*-
"""Unit tests for the sidecar-less central-push k8s registry (K8sVLLMRegistry).

Discovery and the KV-events subscriber are mocked so these run with no
Kubernetes cluster, Redis or ZMQ. They verify:
  * endpoints are built from discovery keyed by POD NAME (same identity the
    sidecar push path uses), with the right vLLM URL / KV-events endpoint;
  * KV subscribers run only when prefix routing (KV_AWARE) is on;
  * pod churn (scale up/down, IP change) adds/removes endpoints + subscribers.
"""
import asyncio

import pytest

from router import k8s_endpoints
from router.external_endpoints import ExternalEndpoint


class _FakeSub:
    """Records start/stop instead of touching Redis/ZMQ."""
    instances = []

    def __init__(self, ep):
        self.ep = ep
        self.started = False
        self.stopped = False
        _FakeSub.instances.append(self)

    def start(self):
        self.started = True

    def stop(self):
        self.stopped = True


@pytest.fixture
def patched(monkeypatch):
    """Patch discovery + subscriber; return a mutable dict driving discovery."""
    _FakeSub.instances = []
    state = {"pods": {}}

    def fake_discover(*args, **kwargs):
        return dict(state["pods"])

    monkeypatch.setattr(k8s_endpoints, "discover_running_pods", fake_discover)
    monkeypatch.setattr(k8s_endpoints, "RouterKVSubscriber", _FakeSub)
    # Deterministic model name regardless of registry state.
    monkeypatch.setattr(k8s_endpoints, "_default_model", lambda: "served-model")
    # Prefix routing ON by default so subscribers are exercised.
    monkeypatch.setattr(k8s_endpoints._cfg, "KV_AWARE", True, raising=False)
    monkeypatch.setattr(k8s_endpoints._cfg, "VLLM_PORT", 8200, raising=False)
    monkeypatch.setattr(k8s_endpoints._cfg, "VLLM_KV_EVENTS_PORT", 5557, raising=False)
    monkeypatch.setattr(k8s_endpoints._cfg, "VLLM_KV_EVENTS_TOPIC", "kv@", raising=False)
    return state


def _close(reg):
    asyncio.run(reg.aclose())


def test_builds_endpoints_from_discovery(patched):
    patched["pods"] = {"vllm-a": "10.0.0.1", "vllm-b": "10.0.0.2"}
    reg = k8s_endpoints.K8sVLLMRegistry()
    try:
        assert sorted(reg.all_ids()) == ["vllm-a", "vllm-b"]
        ep = reg.get("vllm-a")
        assert isinstance(ep, ExternalEndpoint)
        assert ep.id == "vllm-a"
        assert ep.url == "http://10.0.0.1:8200"
        assert ep.model == "served-model"
        assert ep.kv_events_endpoints == ["tcp://10.0.0.1:5557"]
        assert ep.kv_events_topic == "kv@"
        # Unknown endpoints are healthy until a probe proves otherwise.
        assert sorted(reg.healthy_ids()) == ["vllm-a", "vllm-b"]
        # One subscriber started per pod (prefix routing on).
        assert len(_FakeSub.instances) == 2
        assert all(s.started for s in _FakeSub.instances)
    finally:
        _close(reg)


def test_affinity_only_starts_no_kv_subscribers(patched, monkeypatch):
    monkeypatch.setattr(k8s_endpoints._cfg, "KV_AWARE", False, raising=False)
    patched["pods"] = {"vllm-a": "10.0.0.1"}
    reg = k8s_endpoints.K8sVLLMRegistry()
    try:
        ep = reg.get("vllm-a")
        assert ep.kv_events_endpoints == []
        assert _FakeSub.instances == []
    finally:
        _close(reg)


def test_churn_add_remove_and_ip_change(patched):
    patched["pods"] = {"vllm-a": "10.0.0.1", "vllm-b": "10.0.0.2"}
    reg = k8s_endpoints.K8sVLLMRegistry()
    try:
        first_a = reg.get("vllm-a")
        subs_after_init = list(_FakeSub.instances)

        # Scale down b, add c, and change a's IP.
        patched["pods"] = {"vllm-a": "10.9.9.9", "vllm-c": "10.0.0.3"}
        reg._discover(force=True)

        assert sorted(reg.all_ids()) == ["vllm-a", "vllm-c"]
        # a's endpoint rebuilt with the new IP.
        assert reg.get("vllm-a").url == "http://10.9.9.9:8200"
        assert reg.get("vllm-a") is not first_a
        # b removed (its subscriber stopped).
        b_sub = next(s for s in subs_after_init if s.ep.id == "vllm-b")
        assert b_sub.stopped is True
        # c added with a fresh subscriber.
        assert any(s.ep.id == "vllm-c" and s.started for s in _FakeSub.instances)
    finally:
        _close(reg)


def test_transient_discovery_failure_keeps_last_set(patched):
    patched["pods"] = {"vllm-a": "10.0.0.1"}
    reg = k8s_endpoints.K8sVLLMRegistry()
    try:
        assert reg.all_ids() == ["vllm-a"]
        # Discovery returns empty (transient k8s error): keep the known set.
        patched["pods"] = {}
        reg._discover(force=True)
        assert reg.all_ids() == ["vllm-a"]
    finally:
        _close(reg)
