import importlib.util
import json
import threading
import time
import types
from pathlib import Path


MODULE_PATH = Path(__file__).parents[2] / "vllm-kv-stack" / "files" / "pd_rebalancer.py"
SPEC = importlib.util.spec_from_file_location("pd_rebalancer", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(MODULE)


def config() -> object:
    return MODULE.ModelConfig("qwen", "qwen-prefill", "qwen-decode", "qwen-proxy", 1, 1, 3)


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
        self.fail_convergence: set[str] = set()
        self.terminating_pods: set[str] = set()
        self.observed_generation_lag: set[str] = set()
        self.nodes: list[dict[str, object]] = []
        self.pods_on_nodes: dict[str, list[dict[str, object]]] = {}

    def deployment(self, name: str) -> dict[str, object]:
        replicas = self.replica_counts[name]
        ready = getattr(self, "ready_override", {}).get(name, replicas)
        status: dict[str, object] = {
            "readyReplicas": ready,
            "updatedReplicas": ready,
            "availableReplicas": ready,
            "observedGeneration": 1,
        }
        if name in self.observed_generation_lag:
            status["observedGeneration"] = 0
        return {
            "metadata": {"generation": 1},
            "spec": {"replicas": replicas},
            "status": status,
        }

    def scale(self, name: str, replicas: int) -> None:
        self.calls.append((name, replicas))
        self.replica_counts[name] = replicas

    def pods(self, label_selector: str) -> list[dict[str, object]]:
        name = label_selector.split("=", 1)[1]
        count = self.replica_counts.get(name, 0)
        pods = []
        for _ in range(count):
            pod: dict[str, object] = {
                "metadata": {},
                "status": {
                    "phase": "Running",
                    "conditions": [{"type": "Ready", "status": "True"}],
                },
            }
            if name in self.terminating_pods:
                pod["metadata"] = {"deletionTimestamp": "2026-08-24T00:00:00Z"}
            pods.append(pod)
        return pods

    def nodes(self) -> list[dict[str, object]]:
        return self.nodes

    def pods_on_node(self, node: str) -> list[dict[str, object]]:
        return self.pods_on_nodes.get(node, [])

    def state(self, name: str) -> dict[str, object]:
        return {"data": {"targets.json": json.dumps(self.state_data, sort_keys=True)}}

    def write_state(self, name: str, targets: dict[str, object]) -> None:
        self.state_data = targets

    def patch_data(self, name: str, data: dict[str, str]) -> None:
        raise NotImplementedError


def make_rebalancer(api: FakeKubernetesApi, dry_run: bool = False) -> object:
    rebalancer = object.__new__(MODULE.Rebalancer)
    rebalancer.api = api
    rebalancer.poll_seconds = 0
    rebalancer.ready_timeout = 1
    rebalancer.drain_timeout = 5
    rebalancer.state_configmap = "state"
    rebalancer.dry_run = dry_run
    rebalancer.capacity_preflight = False
    rebalancer.capacity_resource = "accelerator.example.com/device"
    rebalancer.proxy_port = 8200
    rebalancer.lock = threading.Lock()
    rebalancer.models = {"qwen": config()}
    rebalancer.last_error = ""
    rebalancer._first_pass = False
    rebalancer.heartbeat_timeout = 60.0
    rebalancer.heartbeats = {
        "rebalancer": time.monotonic(),
        "planner": time.monotonic(),
    }
    rebalancer.heartbeat_lock = threading.Lock()
    rebalancer.drain_proxy = lambda model_config, enabled: None  # type: ignore[method-assign]
    rebalancer.wait_drained = lambda model_config: None  # type: ignore[method-assign]
    rebalancer.preflight = lambda model_config, target: None  # type: ignore[method-assign]
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


def test_wait_for_role_waits_for_observed_generation() -> None:
    import pytest

    api = FakeKubernetesApi()
    api.observed_generation_lag.add("qwen-prefill")
    rebalancer = make_rebalancer(api)

    with pytest.raises(TimeoutError, match="converge"):
        rebalancer.wait_for_role("qwen-prefill", 2)


def test_wait_for_role_waits_until_terminating_pod_is_gone() -> None:
    import pytest

    api = FakeKubernetesApi()
    api.terminating_pods.add("qwen-decode")
    rebalancer = make_rebalancer(api)

    with pytest.raises(TimeoutError, match="converge"):
        rebalancer.wait_for_role("qwen-decode", 1)


def test_wait_for_role_timeout_reports_observed_shortfall() -> None:
    import pytest

    api = FakeKubernetesApi()
    api.ready_override = {"qwen-decode": 1}  # spec moves to 2, status stays at 1
    rebalancer = make_rebalancer(api)
    rebalancer.api.scale("qwen-decode", 2)

    with pytest.raises(TimeoutError, match=r"spec=2 ready=1 .*readyPods="):
        rebalancer.wait_for_role("qwen-decode", 2)


def test_reconcile_persists_transition_marker_and_clears_it() -> None:
    api = FakeKubernetesApi()
    rebalancer = make_rebalancer(api)

    assert rebalancer.reconcile_model(config(), MODULE.Replicas(1, 2)) is True

    entry = api.state_data["qwen"]
    assert "transition" not in entry
    assert entry["target"] == {"prefill": 1, "decode": 2}


def test_reconcile_drains_proxy_before_scaling() -> None:
    calls: list[tuple[str, bool]] = []
    api = FakeKubernetesApi()
    rebalancer = make_rebalancer(api)
    rebalancer.drain_proxy = lambda cfg, enabled: calls.append((cfg.name, enabled))  # type: ignore[method-assign]

    rebalancer.reconcile_model(config(), MODULE.Replicas(1, 2))

    assert calls == [("qwen", True), ("qwen", False)]
    assert api.calls == [("qwen-prefill", 1), ("qwen-decode", 2)]


def test_reconcile_rolls_back_on_scale_timeout() -> None:
    api = FakeKubernetesApi()
    rebalancer = make_rebalancer(api)

    failed_scale_down: set[str] = set()

    def flaky(name: str, replicas: int) -> None:
        # Fail the forward scale-down once; the rollback scale-up must succeed.
        if name in ("qwen-prefill", "qwen-decode") and replicas < api.replica_counts.get(name, 0):
            failed_scale_down.add(name)
            raise TimeoutError(f"{name} never converged")
        api.scale(name, replicas)

    rebalancer.api.scale = flaky  # type: ignore[method-assign]
    assert rebalancer.reconcile_model(config(), MODULE.Replicas(1, 2)) is False

    # The failed forward target is rolled back to the previous topology and the
    # transition marker is cleared once the rollback converges.
    entry = api.state_data["qwen"]
    assert entry["target"] == {"prefill": 2, "decode": 1}
    assert "transition" not in entry
    assert rebalancer.last_error != ""
    assert failed_scale_down == {"qwen-prefill"}


def test_reconcile_up_step_timeout_warns_and_rolls_back(capsys) -> None:
    api = FakeKubernetesApi()
    # Decode never becomes ready at 2 replicas, so the scale-up step times out
    # after the source (prefill) already scaled down: exactly the capacity dip
    # the reviewer asked to make visible.
    api.ready_override = {"qwen-decode": 1}
    rebalancer = make_rebalancer(api)

    assert rebalancer.reconcile_model(config(), MODULE.Replicas(1, 2)) is False

    out = capsys.readouterr().out
    assert "WARNING" in out
    assert "decode scale-up to 2" in out
    assert "temporary capacity dip" in out
    assert "did not converge" in rebalancer.last_error
    # Rollback restores the previous fixed-budget topology P2,D1.
    assert api.replica_counts == {"qwen-prefill": 2, "qwen-decode": 1}
    entry = api.state_data["qwen"]
    assert entry["target"] == {"prefill": 2, "decode": 1}
    assert "transition" not in entry


def test_propose_and_commit_rejected_while_transition_in_progress() -> None:
    import pytest

    api = FakeKubernetesApi()
    rebalancer = make_rebalancer(api)
    rebalancer._begin_transition("qwen", MODULE.Replicas(1, 2), MODULE.Replicas(2, 1))

    with pytest.raises(RuntimeError, match="in progress"):
        rebalancer.propose("qwen", MODULE.Replicas(1, 2))
    with pytest.raises(RuntimeError, match="in progress"):
        rebalancer.commit("qwen")
    with pytest.raises(RuntimeError, match="in progress"):
        rebalancer.discard("qwen")
    assert rebalancer.status("qwen")["transition"] is not None


def test_commit_runs_capacity_preflight() -> None:
    import pytest

    api = FakeKubernetesApi()
    rebalancer = make_rebalancer(api)
    rebalancer.capacity_preflight = True
    rebalancer.preflight = lambda cfg, target: (_ for _ in ()).throw(  # type: ignore[method-assign]
        MODULE.CapacityError("not enough cards")
    )
    rebalancer.propose("qwen", MODULE.Replicas(1, 2))

    with pytest.raises(MODULE.CapacityError, match="not enough cards"):
        rebalancer.commit("qwen")


def test_capacity_preflight_math() -> None:
    api = FakeKubernetesApi()
    api.fake_nodes = [
        {
            "metadata": {"name": "node-a"},
            "spec": {},
            "status": {
                "conditions": [{"type": "Ready", "status": "True"}],
                "allocatable": {"accelerator.example.com/device": "8"},
            },
        }
    ]
    api.nodes = lambda: api.fake_nodes  # type: ignore[method-assign]
    api.pods_on_nodes["node-a"] = [
        {
            "spec": {
                "containers": [
                    {"resources": {"requests": {"accelerator.example.com/device": "2"}}}
                ]
            }
        }
    ]
    rebalancer = make_rebalancer(api)
    rebalancer.capacity_preflight = True
    rebalancer.preflight = types.MethodType(MODULE.Rebalancer.preflight, rebalancer)

    # current P2,D1 with TP1 uses 3 cards; target P1,D2 also uses 3 cards ->
    # no extra cards needed, passes.
    rebalancer.preflight(config(), MODULE.Replicas(1, 2))

    # A target needing more than the 6 free cards must be rejected.
    import pytest

    with pytest.raises(MODULE.CapacityError, match="extra"):
        rebalancer.preflight(config(), MODULE.Replicas(5, 5))


def test_run_rolls_back_stale_transition() -> None:
    api = FakeKubernetesApi()
    rebalancer = make_rebalancer(api)
    rebalancer.drain_timeout = 1
    rebalancer.ready_timeout = 1
    rebalancer._begin_transition("qwen", MODULE.Replicas(1, 2), MODULE.Replicas(2, 1))
    api.state_data["qwen"]["target"] = {"prefill": 1, "decode": 2}
    api.state_data["qwen"]["transition"]["startedAt"] = time.time() - 100000

    rebalancer.run(max_iters=1)

    entry = api.state_data["qwen"]
    assert entry["target"] == {"prefill": 2, "decode": 1}
    assert "transition" not in entry


def test_run_first_pass_rolls_back_fresh_transition_after_restart() -> None:
    api = FakeKubernetesApi()
    rebalancer = make_rebalancer(api)
    rebalancer.drain_timeout = 300
    rebalancer.ready_timeout = 900
    rebalancer._first_pass = True  # simulate a pod restart
    rebalancer._begin_transition("qwen", MODULE.Replicas(1, 2), MODULE.Replicas(2, 1))
    api.state_data["qwen"]["target"] = {"prefill": 1, "decode": 2}
    api.state_data["qwen"]["transition"]["startedAt"] = time.time() - 5  # fresh

    rebalancer.run(max_iters=1)

    entry = api.state_data["qwen"]
    assert entry["target"] == {"prefill": 2, "decode": 1}
    assert "transition" not in entry
