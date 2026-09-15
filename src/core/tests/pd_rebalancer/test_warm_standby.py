"""Unit tests for the per-card dual-engine warm-standby executor.

Each dual-engine pod hosts a prefill and a decode engine on one card; exactly
one is awake at a time. A rebalance is a per-card sleep/wake flip that never
touches Deployment `/scale`.
"""

from __future__ import annotations

import importlib.util
import json
import threading
import time
import types
from pathlib import Path
from unittest import mock
from urllib.error import URLError

import pytest


MODULE_PATH = Path(__file__).parents[2] / "vllm-kv-stack" / "files" / "pd_rebalancer.py"
SPEC = importlib.util.spec_from_file_location("pd_rebalancer", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(MODULE)


def warm_config() -> object:
    return MODULE.ModelConfig(
        "qwen",
        "vllm-qwen-prefill",
        "vllm-qwen-decode",
        "vllm-qwen",
        1,
        1,
        3,
        mode="warmstandby",
        deployment="vllm-qwen-pd",
        replicas=3,
        prefill_port=8200,
        decode_port=8201,
        sleep_level=1,
    )


def card(name: str, role: str | None, ip: str = "10.0.0.1", ready: bool = True) -> dict:
    labels = {
        "app": "vllm-qwen-pd",
        "pd-prefill-awake": "true" if role == "prefill" else "false",
        "pd-decode-awake": "true" if role == "decode" else "false",
    }
    status = {
        "phase": "Running",
        "podIP": ip,
        "conditions": [{"type": "Ready", "status": "True" if ready else "False"}],
    }
    return {"metadata": {"name": name, "labels": labels}, "status": status}


class FakeKubernetesApi:
    def __init__(self, pods: list[dict]) -> None:
        self.pods_list = pods
        self.state: dict = {}
        self.label_patches: list[tuple[str, dict]] = []

    def pods(self, label_selector: str) -> list[dict]:
        return [dict(p) for p in self.pods_list]

    def pod_patch_labels(self, name: str, labels: dict[str, str]) -> None:
        self.label_patches.append((name, dict(labels)))
        for pod in self.pods_list:
            if (pod.get("metadata") or {}).get("name") == name:
                pod["metadata"]["labels"].update(labels)
                return
        raise AssertionError(f"pod {name} not found")

    def write_state(self, name: str, state: dict) -> None:
        self.state = state


class _FakeResponse:
    def __init__(self, payload: bytes = b"", status: int = 200) -> None:
        self.status = status
        self._payload = payload
        self._sent = False

    def read(self, *_args) -> bytes:  # noqa: ANN002 - http.client read(size) shape
        # A real http.client response answers b"" at EOF; the streaming probe
        # keeps reading until then, so the stub must not repeat the payload.
        if self._sent:
            return b""
        self._sent = True
        return self._payload

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, *exc) -> None:
        return None


def make_rebalancer(pods: list[dict], api: FakeKubernetesApi | None = None) -> object:
    api = api or FakeKubernetesApi(pods)
    rebalancer = MODULE.Rebalancer.__new__(MODULE.Rebalancer)
    rebalancer.api = api
    rebalancer.state_configmap = "state"
    rebalancer.poll_seconds = 0.01
    rebalancer.ready_timeout = 5
    rebalancer.drain_timeout = 5
    rebalancer.dry_run = False
    rebalancer.lock = threading.Lock()
    rebalancer.last_error = ""
    rebalancer._first_pass = True
    rebalancer.heartbeat_timeout = 60.0
    rebalancer.proxy_port = 8200
    rebalancer.heartbeats = {
        "rebalancer": time.monotonic(),
        "planner": time.monotonic(),
    }
    rebalancer.heartbeat_lock = threading.Lock()
    rebalancer.needs_recreate = set()
    rebalancer.kv_warmup_enabled = True
    rebalancer.kv_warmup_attempts = 3
    rebalancer.kv_warmup_timeout = 1.0
    rebalancer.kv_warmup_gap = 0.0
    rebalancer.kv_warmup_filler_repeats = 24
    rebalancer.kv_warmup_max_tokens = 1
    rebalancer.kv_warmup_peer_max_tokens = 1
    # Most tests below exercise the drained (blocking) form of the gate: traffic
    # resumes only after the probe finishes. The background form (drain released
    # first, probe runs afterwards) has its own tests further down.
    rebalancer.kv_warmup_blocking = True
    rebalancer.kv_warmup_blocking_budget = 20.0
    rebalancer.targets = lambda: api.state
    rebalancer._ensure_entry = lambda state, name: state.setdefault(name, {})
    rebalancer.drain_proxy = lambda config, enabled: None
    rebalancer.wait_drained = lambda config: None
    rebalancer.engine_calls: list[tuple[str, str]] = []
    rebalancer.fail_wake_at_call: int | None = None
    rebalancer._wake_count = 0

    def sleep_engine(config, pod, role) -> None:
        rebalancer.engine_calls.append(("sleep", role))

    def wake_engine(config, pod, role) -> None:
        rebalancer.engine_calls.append(("wake", role))
        rebalancer._wake_count += 1
        if rebalancer.fail_wake_at_call == rebalancer._wake_count:
            raise RuntimeError(f"wake {role} failed (test)")

    rebalancer._sleep_engine = sleep_engine  # type: ignore[method-assign]
    rebalancer._wake_engine = wake_engine  # type: ignore[method-assign]
    return rebalancer


def active_roles(rebalancer, config) -> dict[str, str | None]:
    return {
        (p.get("metadata") or {}).get("name"): rebalancer._pod_active_role(p)
        for p in rebalancer._card_pods(config)
    }


def test_awake_counts_roles_by_label_and_skips_pods_without_ip() -> None:
    api = FakeKubernetesApi(
        [card("a", "decode"), card("b", "prefill"), card("c", None), card("d", "decode", ip="")]
    )
    rebalancer = make_rebalancer([], api)
    config = warm_config()
    assert rebalancer.awake(config) == MODULE.Replicas(prefill=1, decode=1)


def test_flip_plan_keeps_matching_roles_and_minimizes_flips() -> None:
    pods = [card("a", "decode"), card("b", "prefill"), card("c", "decode")]
    rebalancer = make_rebalancer(pods)
    config = warm_config()

    plan = rebalancer._flip_plan(config, MODULE.Replicas(2, 1))
    assert [(p["metadata"]["name"], r) for p, r in plan] == [("c", "prefill")]

    plan = rebalancer._flip_plan(config, MODULE.Replicas(1, 1))
    assert [(p["metadata"]["name"], r) for p, r in plan] == [("c", None)]

    plan = rebalancer._flip_plan(config, MODULE.Replicas(3, 0))
    assert [(p["metadata"]["name"], r) for p, r in plan] == [("a", "prefill"), ("c", "prefill")]


def test_sleep_engine_retries_http_500_then_succeeds(monkeypatch) -> None:
    config = warm_config()
    pod = card("a", "prefill")
    rebalancer = make_rebalancer([pod])
    rebalancer._sleep_engine = types.MethodType(
        MODULE.Rebalancer._sleep_engine, rebalancer
    )

    calls = {"sleep": 0, "status": 0}
    backoffs: list[float] = []

    class FakeResponse:
        def __init__(self, payload: bytes = b""):
            self._payload = payload
            self._sent = False

        def _read_once(self) -> bytes:
            if self._sent:
                return b""
            self._sent = True
            return self._payload

        def read(self, *_args) -> bytes:  # noqa: ANN002 - http.client read(size) shape
            return self._read_once()

        def __enter__(self) -> "FakeResponse":
            return self

        def __exit__(self, *exc) -> None:
            return None

    def fake_urlopen(request, timeout=None):
        url = request.full_url if isinstance(request, MODULE.Request) else str(request)
        if url.endswith("/sleep"):
            calls["sleep"] += 1
            if calls["sleep"] == 1:
                raise MODULE.HTTPError(url, 500, "sleep failed", {}, None)
            return FakeResponse()
        if url.endswith("/is_sleeping"):
            calls["status"] += 1
            if calls["status"] == 1:
                return FakeResponse(b'{"is_sleeping": false}')
            return FakeResponse(b'{"is_sleeping": true}')
        raise AssertionError(f"unexpected url {url}")

    monkeypatch.setattr(MODULE, "urlopen", fake_urlopen)
    monkeypatch.setattr(MODULE.time, "sleep", lambda seconds: backoffs.append(seconds))
    monkeypatch.setenv("PD_REBALANCER_SLEEP_RETRIES", "3")
    monkeypatch.setenv("PD_REBALANCER_SLEEP_BACKOFF_SECONDS", "2")

    rebalancer._sleep_engine(config, pod, "prefill")

    assert calls["sleep"] == 2
    assert calls["status"] == 2
    assert backoffs == [2.0]


def test_sleep_engine_raises_after_exhausting_retries(monkeypatch) -> None:
    config = warm_config()
    pod = card("a", "prefill")
    rebalancer = make_rebalancer([pod])
    rebalancer._sleep_engine = types.MethodType(
        MODULE.Rebalancer._sleep_engine, rebalancer
    )

    calls = {"sleep": 0, "status": 0}
    backoffs: list[float] = []

    def fake_urlopen(request, timeout=None):
        url = request.full_url if isinstance(request, MODULE.Request) else str(request)
        if url.endswith("/is_sleeping"):
            calls["status"] += 1
            return _FakeResponse(b'{"is_sleeping": false}')
        assert url.endswith("/sleep")
        calls["sleep"] += 1
        raise MODULE.HTTPError(url, 500, "sleep failed", {}, None)

    monkeypatch.setattr(MODULE, "urlopen", fake_urlopen)
    monkeypatch.setattr(MODULE.time, "sleep", lambda seconds: backoffs.append(seconds))
    monkeypatch.setenv("PD_REBALANCER_SLEEP_RETRIES", "3")
    monkeypatch.setenv("PD_REBALANCER_SLEEP_BACKOFF_SECONDS", "2")

    with pytest.raises(RuntimeError, match="after 3 attempts"):
        rebalancer._sleep_engine(config, pod, "prefill")

    assert calls["sleep"] == 3
    assert calls["status"] == 1
    assert backoffs == [2.0, 4.0, 6.0]


def test_sleep_engine_skips_post_when_already_sleeping(monkeypatch) -> None:
    config = warm_config()
    pod = card("a", "prefill")
    rebalancer = make_rebalancer([pod])
    rebalancer._sleep_engine = types.MethodType(
        MODULE.Rebalancer._sleep_engine, rebalancer
    )

    calls = {"status": 0}

    def fake_urlopen(request, timeout=None):
        url = request.full_url if isinstance(request, MODULE.Request) else str(request)
        assert url.endswith("/is_sleeping")
        calls["status"] += 1
        return _FakeResponse(b'{"is_sleeping": true}')

    monkeypatch.setattr(MODULE, "urlopen", fake_urlopen)
    rebalancer._sleep_engine(config, pod, "prefill")
    assert calls["status"] == 1


def test_wake_engine_skips_post_when_already_awake(monkeypatch) -> None:
    config = warm_config()
    pod = card("a", "prefill")
    rebalancer = make_rebalancer([pod])
    rebalancer._wake_engine = types.MethodType(
        MODULE.Rebalancer._wake_engine, rebalancer
    )

    calls = {"health": 0}

    def fake_urlopen(request, timeout=None):
        url = request.full_url if isinstance(request, MODULE.Request) else str(request)
        if url.endswith("/is_sleeping"):
            return _FakeResponse(b'{"is_sleeping": false}')
        if url.endswith("/health"):
            calls["health"] += 1
            return _FakeResponse()
        raise AssertionError(f"unexpected url {url}")

    monkeypatch.setattr(MODULE, "urlopen", fake_urlopen)
    monkeypatch.setattr(MODULE.time, "sleep", lambda seconds: None)
    rebalancer._wake_engine(config, pod, "prefill")
    assert calls["health"] == 1


def test_flip_plan_skips_unready_pods() -> None:
    ready = card("a", "decode")
    unready = card("b", "decode")
    unready["status"]["conditions"] = [{"type": "Ready", "status": "False"}]
    rebalancer = make_rebalancer([ready, unready])
    with pytest.raises(RuntimeError, match="ready dual-engine pods"):
        rebalancer._flip_plan(warm_config(), MODULE.Replicas(2, 0))
    plan = rebalancer._flip_plan(warm_config(), MODULE.Replicas(1, 0))
    assert [(p["metadata"]["name"], r) for p, r in plan] == [("a", "prefill")]


def test_flip_pod_sleeps_current_before_waking_peer() -> None:
    pods = [card("a", "decode")]
    rebalancer = make_rebalancer(pods)
    config = warm_config()
    rebalancer._flip_pod(config, pods[0], "prefill")
    assert rebalancer.engine_calls == [("sleep", "decode"), ("wake", "prefill")]
    assert active_roles(rebalancer, config) == {"a": "prefill"}


def test_flip_to_idle_sleeps_without_waking() -> None:
    pods = [card("a", "prefill")]
    rebalancer = make_rebalancer(pods)
    config = warm_config()
    rebalancer._flip_pod(config, pods[0], None)
    assert rebalancer.engine_calls == [("sleep", "prefill")]
    assert active_roles(rebalancer, config) == {"a": None}


@pytest.mark.parametrize(
    "target,expected",
    [
        (MODULE.Replicas(0, 1), "minimum"),
        (MODULE.Replicas(1, 0), "minimum"),
        (MODULE.Replicas(3, 1), "budget"),
        (MODULE.Replicas(2, 2), "budget"),
        (MODULE.Replicas(4, 0), "minimum"),
    ],
)
def test_validate_awake_target_rejects_invalid_targets(target, expected) -> None:
    rebalancer = make_rebalancer([])
    with pytest.raises(ValueError, match=expected):
        rebalancer._validate_awake_target(warm_config(), target)


def test_reconcile_awake_applies_flips_and_commits() -> None:
    pods = [card("a", "decode"), card("b", "decode"), card("c", "decode")]
    api = FakeKubernetesApi(pods)
    rebalancer = make_rebalancer(pods, api)
    rebalancer.kv_warmup_enabled = False
    config = warm_config()
    drain_calls: list[bool] = []
    rebalancer.drain_proxy = lambda config, enabled: drain_calls.append(enabled)  # type: ignore[method-assign]

    assert rebalancer.reconcile_awake(config, MODULE.Replicas(2, 1)) is True
    assert active_roles(rebalancer, config) == {"a": "decode", "b": "prefill", "c": "prefill"}
    assert rebalancer.engine_calls == [
        ("sleep", "decode"),
        ("wake", "prefill"),
        ("sleep", "decode"),
        ("wake", "prefill"),
    ]
    assert drain_calls == [True, False]
    entry = api.state["qwen"]
    assert entry["target"] == {"prefill": 2, "decode": 1}
    assert "transition" not in entry
    assert rebalancer.last_error == ""


def test_reconcile_awake_pure_wake_skips_drain() -> None:
    pods = [card("a", None), card("b", None), card("c", None)]
    api = FakeKubernetesApi(pods)
    rebalancer = make_rebalancer(pods, api)
    rebalancer.kv_warmup_enabled = False
    config = warm_config()
    drain_calls: list[bool] = []
    rebalancer.drain_proxy = lambda config, enabled: drain_calls.append(enabled)  # type: ignore[method-assign]

    assert rebalancer.reconcile_awake(config, MODULE.Replicas(2, 1)) is True
    assert active_roles(rebalancer, config) == {
        "a": "prefill",
        "b": "prefill",
        "c": "decode",
    }
    assert rebalancer.engine_calls == [
        ("wake", "prefill"),
        ("wake", "prefill"),
        ("wake", "decode"),
    ]
    assert drain_calls == []
    assert rebalancer.pool(config) == 0


def test_reconcile_awake_scale_down_still_drains() -> None:
    pods = [card("a", "prefill"), card("b", "prefill"), card("c", "decode")]
    api = FakeKubernetesApi(pods)
    rebalancer = make_rebalancer(pods, api)
    config = warm_config()
    drain_calls: list[bool] = []
    rebalancer.drain_proxy = lambda config, enabled: drain_calls.append(enabled)  # type: ignore[method-assign]

    assert rebalancer.reconcile_awake(config, MODULE.Replicas(1, 1)) is True
    assert active_roles(rebalancer, config) == {
        "a": "prefill",
        "b": None,
        "c": "decode",
    }
    assert rebalancer.engine_calls == [("sleep", "prefill")]
    assert drain_calls == [True, False]


def test_reconcile_awake_runs_kv_warmup_when_prefill_woken() -> None:
    pods = [card("a", "decode"), card("b", "decode"), card("c", "decode")]
    api = FakeKubernetesApi(pods)
    rebalancer = make_rebalancer(pods, api)
    config = warm_config()
    events: list[str] = []
    rebalancer.drain_proxy = lambda config, enabled: events.append(  # type: ignore[method-assign]
        "drain:on" if enabled else "drain:off"
    )
    rebalancer._warmup_kv_path = lambda config, woken: events.append(  # type: ignore[method-assign]
        "warmup:" + ",".join(f"{n}:{r}" for n, r in sorted(woken))
    )

    assert rebalancer.reconcile_awake(config, MODULE.Replicas(2, 1)) is True
    # Role-swap wake (decode -> prefill) drains first; the gate warms the
    # woken prefills INSIDE the drain window; traffic resumes only after.
    assert events == ["drain:on", "warmup:b:prefill,c:prefill", "drain:off"]
    assert api.state["qwen"]["target"] == {"prefill": 2, "decode": 1}


def test_reconcile_awake_pure_wake_drains_for_kv_warmup() -> None:
    pods = [card("a", "prefill"), card("b", "decode"), card("c", None)]
    api = FakeKubernetesApi(pods)
    rebalancer = make_rebalancer(pods, api)
    config = warm_config()
    events: list[str] = []
    rebalancer.drain_proxy = lambda config, enabled: events.append(  # type: ignore[method-assign]
        "drain:on" if enabled else "drain:off"
    )
    rebalancer._warmup_kv_path = lambda config, woken: events.append(  # type: ignore[method-assign]
        "warmup:" + ",".join(f"{n}:{r}" for n, r in sorted(woken))
    )

    # P1,D1 -> P1,D2 pure scale-up: card c wakes as decode. Even though no
    # awake engine sleeps, the flip creates a cold KV pair (c <-> a), so it
    # must drain and warm that pair before traffic resumes.
    assert rebalancer.reconcile_awake(config, MODULE.Replicas(1, 2)) is True
    assert events == ["drain:on", "warmup:c:decode", "drain:off"]
    assert rebalancer.engine_calls == [("wake", "decode")]
    assert api.state["qwen"]["target"] == {"prefill": 1, "decode": 2}


def test_reconcile_awake_skips_kv_warmup_when_no_engine_woken() -> None:
    pods = [card("a", "prefill"), card("b", "decode"), card("c", "decode")]
    api = FakeKubernetesApi(pods)
    rebalancer = make_rebalancer(pods, api)
    config = warm_config()
    events: list[str] = []
    rebalancer.drain_proxy = lambda config, enabled: events.append(  # type: ignore[method-assign]
        "drain:on" if enabled else "drain:off"
    )
    rebalancer._warmup_kv_path = lambda config, woken: events.append(  # type: ignore[method-assign]
        "warmup:unexpected"
    )

    # P1,D2 -> P1,D1: only a decode engine sleeps; nothing new joins service,
    # so no KV warm-up runs (but the sleep of an awake engine still drains).
    assert rebalancer.reconcile_awake(config, MODULE.Replicas(1, 1)) is True
    assert events == ["drain:on", "drain:off"]


def test_reconcile_awake_background_warmup_when_not_blocking() -> None:
    """kv_warmup_blocking=0: the flip returns and traffic resumes while the
    probe keeps running after the drain is released."""
    pods = [card("a", "decode"), card("b", "decode"), card("c", "decode")]
    api = FakeKubernetesApi(pods)
    rebalancer = make_rebalancer(pods, api)
    rebalancer.kv_warmup_blocking = False
    config = warm_config()
    events: list[str] = []
    rebalancer.drain_proxy = lambda config, enabled: events.append(  # type: ignore[method-assign]
        "drain:on" if enabled else "drain:off"
    )
    warmed = threading.Event()

    def slow_warmup(config, woken) -> None:  # type: ignore[no-untyped-def]
        events.append("warmup:" + ",".join(f"{n}:{r}" for n, r in sorted(woken)))
        warmed.set()

    rebalancer._warmup_kv_path = slow_warmup  # type: ignore[method-assign]

    assert rebalancer.reconcile_awake(config, MODULE.Replicas(2, 1)) is True
    assert warmed.wait(2), "background warm-up never ran"
    # Traffic is released before the probe is reported: the flip never waits.
    assert events[:2] == ["drain:on", "drain:off"]
    assert events[2].startswith("warmup:")
    assert api.state["qwen"]["target"] == {"prefill": 2, "decode": 1}


def test_reconcile_awake_background_warmup_failure_does_not_roll_back() -> None:
    """A background probe is best-effort: its failure must not fail the flip."""
    pods = [card("a", "decode"), card("b", "decode"), card("c", "decode")]
    api = FakeKubernetesApi(pods)
    rebalancer = make_rebalancer(pods, api)
    rebalancer.kv_warmup_blocking = False
    config = warm_config()
    called = threading.Event()

    def failing_warmup(config, woken) -> None:  # type: ignore[no-untyped-def]
        called.set()
        raise RuntimeError("KV path did not warm up")

    rebalancer._warmup_kv_path = failing_warmup  # type: ignore[method-assign]

    assert rebalancer.reconcile_awake(config, MODULE.Replicas(2, 1)) is True
    assert called.wait(2), "background warm-up never ran"
    assert rebalancer.last_error == ""
    assert api.state["qwen"]["target"] == {"prefill": 2, "decode": 1}


def test_warmup_budget_exhaustion_releases_traffic() -> None:
    """A probe that outlives the blocking budget must not park the proxy drain:
    the flip completes and the probe is left running in the background."""
    pods = [card("a", "decode"), card("b", "decode"), card("c", "decode")]
    api = FakeKubernetesApi(pods)
    rebalancer = make_rebalancer(pods, api)
    rebalancer.kv_warmup_blocking = True
    rebalancer.kv_warmup_blocking_budget = 0.05
    config = warm_config()
    drain_calls: list[bool] = []
    rebalancer.drain_proxy = lambda config, enabled: drain_calls.append(enabled)  # type: ignore[method-assign]
    started = threading.Event()
    release = threading.Event()

    def hanging_warmup(config, woken) -> None:  # type: ignore[no-untyped-def]
        started.set()
        release.wait(5)

    rebalancer._warmup_kv_path = hanging_warmup  # type: ignore[method-assign]

    try:
        assert rebalancer.reconcile_awake(config, MODULE.Replicas(2, 1)) is True
        assert started.wait(2), "warm-up never started"
        # Drain released despite the probe still running, and the flip committed.
        assert drain_calls == [True, False]
        assert active_roles(rebalancer, config) == {
            "a": "decode",
            "b": "prefill",
            "c": "prefill",
        }
        assert api.state["qwen"]["target"] == {"prefill": 2, "decode": 1}
    finally:
        release.set()


def test_warmup_failure_within_budget_rolls_back() -> None:
    pods = [card("a", "decode"), card("b", "decode"), card("c", "decode")]
    api = FakeKubernetesApi(pods)
    rebalancer = make_rebalancer(pods, api)
    config = warm_config()
    drain_calls: list[bool] = []
    rebalancer.drain_proxy = lambda config, enabled: drain_calls.append(enabled)  # type: ignore[method-assign]

    def fail_warmup(config, woken) -> None:  # type: ignore[no-untyped-def]
        raise RuntimeError("KV path did not warm up")

    rebalancer._warmup_kv_path = fail_warmup  # type: ignore[method-assign]
    rebalancer.kv_warmup_enabled = True

    assert rebalancer.reconcile_awake(config, MODULE.Replicas(2, 1)) is False
    # Rolled back to the previous decode-only topology (drain held through the
    # gate, released by the rollback path).
    assert drain_calls == [True, False]
    assert active_roles(rebalancer, config) == {
        "a": "decode",
        "b": "decode",
        "c": "decode",
    }
    assert "awake transition failed" in rebalancer.last_error
    assert "KV path did not warm up" in rebalancer.last_error
    assert rebalancer.pool(config) == 0
    # The per-card roles are restored to the previous decode-only set, but the
    # committed target is floored at the configured minimums (min_prefill=1,
    # min_decode=1, max_total=3). Parking the target on P0,D3 would leave the
    # model served by the decode-only proxy fallback, so the next reconcile is
    # aimed at P1,D2 instead.
    assert api.state["qwen"]["target"] == {"prefill": 1, "decode": 2}


def test_warmup_probe_uses_served_model_discovery_and_pins_pairs() -> None:
    # Post-flip state for P1,D2 -> P2,D1: a stays decode; b,c woke as prefill.
    # Each woken prefill must be warmed against the active decode a, via
    # DIRECT engine-to-engine probes.
    pods = [card("a", "decode"), card("b", "prefill"), card("c", "prefill")]
    api = FakeKubernetesApi(pods)
    rebalancer = make_rebalancer(pods, api)
    config = warm_config()
    base = rebalancer._proxy_base(config)
    chat_bodies: list[dict] = []
    urls: list[str] = []

    def fake_urlopen(request, timeout=None) -> _FakeResponse:  # type: ignore[no-untyped-def]
        url = request.full_url if isinstance(request, MODULE.Request) else request
        if url == f"{base}/v1/models":
            return _FakeResponse(json.dumps({"data": [{"id": "qwen3-8b"}]}).encode())
        if url.endswith("/v1/chat/completions"):
            urls.append(url)
            chat_bodies.append(json.loads(request.data))
            return _FakeResponse(b"{}")
        raise AssertionError(f"unexpected url {url}")

    woken = [("b", "prefill"), ("c", "prefill")]
    with mock.patch.object(MODULE, "urlopen", side_effect=fake_urlopen):
        rebalancer._warmup_kv_path(config, woken)

    # config.name is "qwen" but the engines serve "qwen3-8b"; the probe must
    # use the discovered served id or every attempt would 404.
    assert len(chat_bodies) == 4
    assert all(body["model"] == "qwen3-8b" for body in chat_bodies)
    # Two jobs (b then c); each phase-1 goes to the woken prefill port 8200
    # and phase-2 to the decode port 8201 on pod a.
    assert [url for url in urls if url.endswith(":8200/v1/chat/completions")] == [
        "http://10.0.0.1:8200/v1/chat/completions",
        "http://10.0.0.1:8200/v1/chat/completions",
    ]
    assert [url for url in urls if url.endswith(":8201/v1/chat/completions")] == [
        "http://10.0.0.1:8201/v1/chat/completions",
        "http://10.0.0.1:8201/v1/chat/completions",
    ]
    # Both legs are deliberately minimal: phase 1 forces max_tokens=1 (prefill
    # compute + publish) and phase 2 pulls the published prefix with the same
    # 1-token budget, which is all it takes to open the KV pair.
    assert [body["max_tokens"] for body in chat_bodies] == [1, 1, 1, 1]


def test_warmup_woken_decode_covers_every_active_prefill() -> None:
    pods = [card("a", "prefill"), card("b", "prefill"), card("c", "decode")]
    api = FakeKubernetesApi(pods)
    rebalancer = make_rebalancer(pods, api)
    config = warm_config()
    urls: list[str] = []

    def fake_urlopen(request, timeout=None) -> _FakeResponse:  # type: ignore[no-untyped-def]
        url = request.full_url if isinstance(request, MODULE.Request) else request
        if url.endswith("/v1/models"):
            return _FakeResponse(json.dumps({"data": [{"id": "qwen3-8b"}]}).encode())
        if url.endswith("/v1/chat/completions"):
            urls.append(url)
            return _FakeResponse(b"{}")
        raise AssertionError(f"unexpected url {url}")

    # A decode woken into P2,D1 -> P2,D2 must warm against BOTH active
    # prefills: requests can land on either, so both pairs must be hot.
    woken = [("c", "decode")]
    with mock.patch.object(MODULE, "urlopen", side_effect=fake_urlopen):
        rebalancer._warmup_kv_path(config, woken)

    assert len(urls) == 4
    # one job per active prefill: (a,c) and (b,c)
    assert urls.count("http://10.0.0.1:8200/v1/chat/completions") == 2
    assert urls.count("http://10.0.0.1:8201/v1/chat/completions") == 2


def test_begin_transition_records_previous_roles_for_stale_rollback() -> None:
    pods = [card("a", "decode"), card("b", "prefill"), card("c", "decode")]
    api = FakeKubernetesApi(pods)
    rebalancer = make_rebalancer(pods, api)
    config = warm_config()
    previous_roles = active_roles(rebalancer, config)
    rebalancer._begin_transition("qwen", MODULE.Replicas(2, 1), MODULE.Replicas(1, 2), previous_roles)
    transition = api.state["qwen"]["transition"]
    assert transition["active"] is True
    assert transition["previousRoles"] == {"a": "decode", "b": "prefill", "c": "decode"}

    # rollback restores the recorded per-pod roles even when only some flipped
    pods[0]["metadata"]["labels"].update({"pd-prefill-awake": "true", "pd-decode-awake": "false"})
    rebalancer._rollback_awake(config, MODULE.Replicas(2, 1), MODULE.Replicas(1, 2), previous_roles, "test")
    assert active_roles(rebalancer, config) == previous_roles


def test_transition_plan_rejects_warmstandby_target_beyond_cards() -> None:
    config = MODULE.ModelConfig(
        "qwen", "v", "d", "p", 1, 1, 4, mode="warmstandby", replicas=3
    )
    with pytest.raises(ValueError, match="dual-engine pod"):
        MODULE.transition_plan(MODULE.Replicas(1, 1), MODULE.Replicas(2, 2), config)
    MODULE.transition_plan(MODULE.Replicas(1, 2), MODULE.Replicas(2, 1), config)  # ok


def test_reconcile_model_dispatches_warmstandby_to_awake_executor() -> None:
    pods = [card("a", "decode")]
    rebalancer = make_rebalancer(pods)
    config = warm_config()
    called = []
    rebalancer.reconcile_awake = lambda config, target: called.append(target) or True  # type: ignore[method-assign]
    assert rebalancer.reconcile_model(config, MODULE.Replicas(1, 0)) is True
    assert called == [MODULE.Replicas(1, 0)]


def test_scale_mode_current_uses_deployment_replicas() -> None:
    config = MODULE.ModelConfig(
        "qwen", "vllm-qwen-prefill", "vllm-qwen-decode", "vllm-qwen", 1, 1, 3, mode="scale"
    )

    class ScaleApi:
        def deployment(self, name: str) -> dict:
            return {"spec": {"replicas": 2 if name == "vllm-qwen-prefill" else 1}}

    rebalancer = make_rebalancer([])
    rebalancer.api = ScaleApi()  # type: ignore[assignment]
    assert rebalancer.current(config) == MODULE.Replicas(prefill=2, decode=1)


# --- mid-flip pod replacement (2026-09-15 kill injection) --------------------


def _scripted_engines(rebalancer, fail_on: str | None = None) -> None:
    """Engine stubs that record the pod they touch.

    ``fail_on`` mimics the real thing the kill injection exercised: a card
    whose engine is not listening (freshly recreated pod) answers the HTTP call
    with ECONNREFUSED. Without ``fail_on`` the stubs answer for every card.
    """

    def engine(kind: str):
        def call(config, pod, role) -> None:
            name = (pod.get("metadata") or {}).get("name", "")
            rebalancer.engine_calls.append((kind, name, role))
            if fail_on is not None and name == fail_on:
                raise URLError(ConnectionRefusedError(111, "Connection refused"))

        return call

    rebalancer._sleep_engine = engine("sleep")  # type: ignore[method-assign]
    rebalancer._wake_engine = engine("wake")  # type: ignore[method-assign]


def _state_with_transition(api, prefill: int, decode: int) -> None:
    api.state["qwen"] = {
        "target": {"prefill": prefill, "decode": decode},
        "transition": {"active": True, "startedAt": time.time()},
    }


def test_flip_pod_refuses_a_card_whose_engines_are_not_serving() -> None:
    pod = card("a", "decode", ready=False)
    rebalancer = make_rebalancer([pod])
    _scripted_engines(rebalancer)
    with pytest.raises(MODULE.CardUnavailable, match="not Ready"):
        rebalancer._flip_pod(warm_config(), pod, "prefill")
    assert rebalancer.engine_calls == []


def test_rollback_skips_a_booting_replacement_card_and_finishes() -> None:
    """The replacement pod must not be slept: it has boot labels, no engine."""
    pods = [
        card("a", "decode"),
        card("b", "prefill"),
        card("c", "prefill"),
        card("d", "decode", ready=False),  # replacement pod, still booting
    ]
    api = FakeKubernetesApi(pods)
    rebalancer = make_rebalancer(pods, api)
    config = warm_config()
    _state_with_transition(api, 2, 2)
    previous_roles = {"a": "decode", "b": "prefill", "c": "prefill"}
    _scripted_engines(rebalancer)

    rebalancer._rollback_awake(
        config, MODULE.Replicas(2, 2), MODULE.Replicas(2, 1), previous_roles, "wake failed"
    )

    assert [call for call in rebalancer.engine_calls if call[1] == "d"] == []
    assert api.state["qwen"]["target"] == {"prefill": 2, "decode": 1}
    assert "transition" not in api.state["qwen"]
    assert rebalancer.last_error == ""


def test_rollback_survives_a_card_that_cannot_be_flipped() -> None:
    pods = [card("a", "decode"), card("b", "prefill"), card("c", "decode")]
    api = FakeKubernetesApi(pods)
    rebalancer = make_rebalancer(pods, api)
    config = warm_config()
    _state_with_transition(api, 1, 2)
    previous_roles = {"a": "decode", "b": "prefill", "c": None}
    _scripted_engines(rebalancer, fail_on="c")

    rebalancer._rollback_awake(
        config, MODULE.Replicas(1, 2), MODULE.Replicas(2, 1), previous_roles, "wake failed"
    )

    assert ("sleep", "c", "decode") in rebalancer.engine_calls
    # a failing card must not strand the rollback: target is still published and
    # the transition marker (which blocks propose/commit) is cleared
    assert api.state["qwen"]["target"] == {"prefill": 2, "decode": 1}
    assert "transition" not in api.state["qwen"]
    assert rebalancer.last_error == ""


def test_sync_needs_recreate_forgets_pods_that_no_longer_exist() -> None:
    rebalancer = make_rebalancer([card("a", "decode")])
    rebalancer.needs_recreate = {"a", "killed-pod"}
    rebalancer._sync_needs_recreate(warm_config())
    assert rebalancer.needs_recreate == {"a"}
