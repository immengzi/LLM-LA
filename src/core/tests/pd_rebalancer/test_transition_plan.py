import importlib.util
import json
import threading
from pathlib import Path


MODULE_PATH = Path(__file__).parents[2] / "vllm-kv-stack" / "files" / "pd_rebalancer.py"
SPEC = importlib.util.spec_from_file_location("pd_rebalancer", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(MODULE)


def config() -> object:
    return MODULE.ModelConfig("qwen", "qwen-prefill", "qwen-decode", 1, 1, 3)


def test_conversion_releases_source_before_starting_target() -> None:
    plan = MODULE.transition_plan(MODULE.Replicas(2, 1), MODULE.Replicas(1, 2), config())
    assert plan == [("prefill", 1), ("decode", 2)]


def test_reverse_conversion_releases_decode_before_prefill() -> None:
    plan = MODULE.transition_plan(MODULE.Replicas(1, 2), MODULE.Replicas(2, 1), config())
    assert plan == [("decode", 1), ("prefill", 2)]


def test_target_cannot_break_budget_or_role_floor() -> None:
    import pytest

    with pytest.raises(ValueError, match="budget"):
        MODULE.transition_plan(MODULE.Replicas(1, 2), MODULE.Replicas(2, 2), config())
    with pytest.raises(ValueError, match="minimum"):
        MODULE.transition_plan(MODULE.Replicas(1, 2), MODULE.Replicas(0, 3), config())


class FakeKubernetesApi:
    def __init__(self) -> None:
        self.replica_counts = {"qwen-prefill": 2, "qwen-decode": 1}
        self.calls: list[tuple[str, int]] = []
        self.state_data: dict[str, object] = {}

    def deployment(self, name: str) -> dict[str, object]:
        replicas = self.replica_counts[name]
        return {"spec": {"replicas": replicas}, "status": {"readyReplicas": replicas}}

    def scale(self, name: str, replicas: int) -> None:
        self.calls.append((name, replicas))
        self.replica_counts[name] = replicas

    def state(self, name: str) -> dict[str, object]:
        return {"data": {"targets.json": json.dumps(self.state_data, sort_keys=True)}}

    def write_state(self, name: str, targets: dict[str, object]) -> None:
        self.state_data = targets


def make_rebalancer(api: FakeKubernetesApi, dry_run: bool = False) -> object:
    rebalancer = object.__new__(MODULE.Rebalancer)
    rebalancer.api = api
    rebalancer.poll_seconds = 0
    rebalancer.ready_timeout = 1
    rebalancer.state_configmap = "state"
    rebalancer.dry_run = dry_run
    rebalancer.lock = threading.Lock()
    rebalancer.models = {"qwen": config()}
    rebalancer.last_error = ""
    return rebalancer


def test_reconcile_waits_for_source_scale_down_before_target_scale_up() -> None:
    rebalancer = make_rebalancer(FakeKubernetesApi())

    rebalancer.reconcile_model(config(), MODULE.Replicas(1, 2))

    assert rebalancer.api.calls == [("qwen-prefill", 1), ("qwen-decode", 2)]
    assert rebalancer.api.replica_counts == {"qwen-prefill": 1, "qwen-decode": 2}


def test_dry_run_logs_plan_without_touching_scale() -> None:
    api = FakeKubernetesApi()
    rebalancer = make_rebalancer(api, dry_run=True)

    rebalancer.reconcile_model(config(), MODULE.Replicas(1, 2))

    assert api.calls == []
    assert api.replica_counts == {"qwen-prefill": 2, "qwen-decode": 1}


def test_propose_then_commit_applies_target_in_two_phases() -> None:
    api = FakeKubernetesApi()
    rebalancer = make_rebalancer(api)

    rebalancer.propose("qwen", MODULE.Replicas(1, 2), reason="decode KV high")
    entry = api.state_data["qwen"]
    assert entry["proposed"]["prefill"] == 1
    assert entry["proposed"]["decode"] == 2
    assert entry["proposed"]["reason"] == "decode KV high"
    assert "proposedAt" in entry["proposed"]
    assert rebalancer.status("qwen")["proposed"] is not None
    assert rebalancer.status("qwen")["target"] is None

    rebalancer.commit("qwen")
    entry = api.state_data["qwen"]
    assert entry["target"] == {"prefill": 1, "decode": 2}
    assert "proposed" not in entry
    assert rebalancer.status("qwen")["target"] == {"prefill": 1, "decode": 2}


def test_commit_without_proposal_is_rejected() -> None:
    import pytest

    rebalancer = make_rebalancer(FakeKubernetesApi())
    with pytest.raises(ValueError, match="no proposed"):
        rebalancer.commit("qwen")


def test_discard_removes_proposal_and_is_idempotent() -> None:
    api = FakeKubernetesApi()
    rebalancer = make_rebalancer(api)

    rebalancer.propose("qwen", MODULE.Replicas(1, 2))
    assert "proposed" in api.state_data["qwen"]
    rebalancer.discard("qwen")
    assert "proposed" not in api.state_data["qwen"]
    rebalancer.discard("qwen")


def test_propose_rejects_unknown_model_and_budget_overflow() -> None:
    import pytest

    rebalancer = make_rebalancer(FakeKubernetesApi())
    with pytest.raises(ValueError, match="unknown"):
        rebalancer.propose("other", MODULE.Replicas(1, 1))
    with pytest.raises(ValueError, match="budget"):
        rebalancer.propose("qwen", MODULE.Replicas(2, 2))


def test_target_for_handles_legacy_and_new_state_shapes() -> None:
    assert MODULE.Rebalancer.target_for({"prefill": 2, "decode": 1}) == {"prefill": 2, "decode": 1}
    assert MODULE.Rebalancer.target_for({"target": {"prefill": 1, "decode": 2}}) == {"prefill": 1, "decode": 2}
    assert MODULE.Rebalancer.target_for({"proposed": {"prefill": 1, "decode": 2}}) is None
    assert MODULE.Rebalancer.target_for(None) is None
