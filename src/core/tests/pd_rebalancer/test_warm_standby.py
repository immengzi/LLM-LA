"""Unit tests for the per-card dual-engine warm-standby executor.

Each dual-engine pod hosts a prefill and a decode engine on one card; exactly
one is awake at a time. A rebalance is a per-card sleep/wake flip that never
touches Deployment `/scale`.
"""

from __future__ import annotations

import importlib.util
import threading
import time
import types
from pathlib import Path

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


def card(name: str, role: str | None, ip: str = "10.0.0.1") -> dict:
    labels = {
        "app": "vllm-qwen-pd",
        "pd-prefill-awake": "true" if role == "prefill" else "false",
        "pd-decode-awake": "true" if role == "decode" else "false",
    }
    status = {
        "phase": "Running",
        "podIP": ip,
        "conditions": [{"type": "Ready", "status": "True"}],
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

    def read(self) -> bytes:
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
    rebalancer.heartbeats = {
        "rebalancer": time.monotonic(),
        "planner": time.monotonic(),
    }
    rebalancer.heartbeat_lock = threading.Lock()
    rebalancer.targets = lambda: api.state
    rebalancer._ensure_entry = lambda state, name: state.setdefault(name, {})
    rebalancer.drain_proxy = lambda config, enabled: None
    rebalancer.wait_drained = lambda config: None
    rebalancer.engine_calls: list[tuple[str, str]] = []
    rebalancer.fail_wake_at_call: int | None = None
    rebalancer._wake_count = 0
    rebalancer.needs_recreate: set[str] = set()

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

        def read(self) -> bytes:
            return self._payload

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
    assert rebalancer.pool(config) == 1


def test_status_reports_pool_depth() -> None:
    pods = [card("a", "decode"), card("b", "prefill"), card("c", None)]
    api = FakeKubernetesApi(pods)
    rebalancer = make_rebalancer(pods, api)
    config = warm_config()
    rebalancer.models = {"qwen": config}  # type: ignore[attr-defined]

    status = rebalancer.status("qwen")
    assert status["current"] == {"prefill": 1, "decode": 1}
    assert status["pool"] == {"cards": 1}


def test_pool_counts_only_cards_with_both_engines_asleep() -> None:
    pods = [
        card("a", "decode"),    # decode serving, prefill sleeping -> not pooled
        card("b", "prefill"),   # prefill serving, decode sleeping -> not pooled
        card("c", None),        # both engines asleep -> pooled
        card("d", "decode"),
    ]
    rebalancer = make_rebalancer(pods)
    assert rebalancer.pool(warm_config()) == 1


def test_reconcile_awake_flags_failed_wake_card_idle_and_rolls_back_others() -> None:
    pods = [card("a", "decode"), card("b", "decode"), card("c", "decode")]
    api = FakeKubernetesApi(pods)
    rebalancer = make_rebalancer(pods, api)
    config = warm_config()
    # First flip (card-b) succeeds; second flip (card-c) fails at wake. The
    # failed card must be left idle and flagged for recreation (its engine may
    # have crashed), never re-woken on the same card; card-b rolls back.
    rebalancer.fail_wake_at_call = 2

    assert rebalancer.reconcile_awake(config, MODULE.Replicas(2, 1)) is False
    assert active_roles(rebalancer, config) == {
        "a": "decode",
        "b": "decode",
        "c": None,
    }
    assert rebalancer.needs_recreate == {"c"}
    entry = api.state["qwen"]
    assert entry["target"] == {"prefill": 0, "decode": 3}
    assert "transition" not in entry
    assert "awake transition failed" in rebalancer.last_error
    assert "needs recreation" in rebalancer.last_error
    # rollback flips card-b back (sleep prefill, wake decode) but never touches c
    assert ("sleep", "prefill") in rebalancer.engine_calls
    assert ("wake", "decode") in rebalancer.engine_calls
    # c stayed idle (out of service) after the failed wake; rollback must not
    # wake its decode back on the same card.
    labels_c = next(p["metadata"]["labels"] for p in api.pods_list if p["metadata"]["name"] == "c")
    assert labels_c["pd-prefill-awake"] == "false"
    assert labels_c["pd-decode-awake"] == "false"


def test_flip_plan_excludes_cards_awaiting_recreation() -> None:
    pods = [card("a", "decode"), card("b", "prefill"), card("c", None)]
    rebalancer = make_rebalancer(pods)
    rebalancer.needs_recreate = {"c"}
    config = warm_config()

    # A target satisfiable without the flagged card plans normally.
    assert rebalancer._flip_plan(config, MODULE.Replicas(1, 1)) == []
    # A target that needs the flagged card fails loudly instead of waking it.
    with pytest.raises(RuntimeError, match="awaiting recreation"):
        rebalancer._flip_plan(config, MODULE.Replicas(2, 1))


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
