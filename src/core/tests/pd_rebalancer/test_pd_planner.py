import importlib.util
import sys
from pathlib import Path


FILES_DIR = Path(__file__).parents[2] / "vllm-kv-stack" / "files"
if str(FILES_DIR) not in sys.path:
    sys.path.insert(0, str(FILES_DIR))

CORE_SPEC = importlib.util.spec_from_file_location("pd_rebalancer", FILES_DIR / "pd_rebalancer.py")
CORE = importlib.util.module_from_spec(CORE_SPEC)
assert CORE_SPEC and CORE_SPEC.loader
sys.modules["pd_rebalancer"] = CORE
CORE_SPEC.loader.exec_module(CORE)

MODULE_PATH = FILES_DIR / "pd_planner.py"
SPEC = importlib.util.spec_from_file_location("pd_planner", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules["pd_planner"] = MODULE
SPEC.loader.exec_module(MODULE)


def cfg(**overrides) -> MODULE.PlannerConfig:
    values = dict(
        min_prefill=1,
        min_decode=1,
        max_total=3,
        prefill_scale_up_tokens=512.0,
        prefill_scale_down_tokens=128.0,
        decode_scale_up_kv_percent=80.0,
        decode_scale_down_kv_percent=60.0,
        min_observations=2,
        cooldown_seconds=300.0,
    )
    values.update(overrides)
    return MODULE.PlannerConfig(**values)


def metrics(prefill_backlog: float = 0.0, decode_kv: float = 0.0) -> MODULE.MetricsSnapshot:
    return MODULE.MetricsSnapshot(
        prefill_backlog_tokens=prefill_backlog,
        decode_kv_usage_percent=decode_kv,
    )


def test_no_pressure_means_no_change() -> None:
    state = MODULE.PlannerState()
    decision = MODULE.decide(
        MODULE.Replicas(2, 1),
        metrics(prefill_backlog=50.0, decode_kv=40.0),
        cfg(),
        state,
        now=1_000_000.0,
    )
    assert decision.target is None
    assert "no change" in decision.reason


def test_decode_to_prefill_after_sustained_pressure_and_cooldown() -> None:
    # Sample 1: streak starts, not enough observations yet.
    state = MODULE.PlannerState(last_change_timestamp=0.0)
    decision = MODULE.decide(
        MODULE.Replicas(1, 2),
        metrics(prefill_backlog=1_000.0, decode_kv=30.0),
        cfg(min_observations=2),
        state,
        now=1_000_000.0,
    )
    assert decision.target is None
    assert decision.state.decode_to_prefill_streak == 1

    # Sample 2: streak satisfied, cooldown elapsed -> D->P.
    decision = MODULE.decide(
        MODULE.Replicas(1, 2),
        metrics(prefill_backlog=1_000.0, decode_kv=30.0),
        cfg(min_observations=2),
        decision.state,
        now=1_000_000.0,
    )
    assert decision.target == MODULE.Replicas(2, 1)
    assert decision.state.last_change_timestamp == 1_000_000.0
    assert decision.state.decode_to_prefill_streak == 0


def test_cooldown_blocks_immediate_second_transition() -> None:
    state = MODULE.PlannerState(last_change_timestamp=1_000_000.0)
    decision = MODULE.decide(
        MODULE.Replicas(1, 2),
        metrics(prefill_backlog=1_000.0, decode_kv=30.0),
        cfg(min_observations=1, cooldown_seconds=300.0),
        state,
        now=1_000_100.0,  # only 100s later
    )
    assert decision.target is None
    assert "cooldown" in decision.reason


def test_prefill_to_decode_direction() -> None:
    state = MODULE.PlannerState()
    decision = MODULE.decide(
        MODULE.Replicas(2, 1),
        metrics(prefill_backlog=50.0, decode_kv=90.0),
        cfg(min_observations=1),
        state,
        now=1_000_000.0,
    )
    assert decision.target == MODULE.Replicas(1, 2)


def test_budget_floor_blocks_decode_to_prefill_when_decode_is_min() -> None:
    decision = MODULE.decide(
        MODULE.Replicas(2, 1),
        metrics(prefill_backlog=1_000.0, decode_kv=30.0),
        cfg(min_observations=1),
        MODULE.PlannerState(),
        now=1_000_000.0,
    )
    assert decision.target is None
    assert "no change" in decision.reason


def test_budget_ceiling_blocks_prefill_to_decode() -> None:
    decision = MODULE.decide(
        MODULE.Replicas(1, 2),
        metrics(prefill_backlog=50.0, decode_kv=90.0),
        cfg(min_observations=1),
        MODULE.PlannerState(),
        now=1_000_000.0,
    )
    assert decision.target is None


def test_max_step_replicas_moves_multiple_replicas_in_one_transition() -> None:
    # P1,D3 -> P3,D1 in a single decision (one executor transition).
    decision = MODULE.decide(
        MODULE.Replicas(1, 3),
        metrics(prefill_backlog=1_000.0, decode_kv=30.0),
        cfg(min_observations=1, max_total=4, max_step_replicas=2),
        MODULE.PlannerState(),
        now=1_000_000.0,
    )
    assert decision.target == MODULE.Replicas(3, 1)
    assert "D->P x2" in decision.reason


def test_max_step_replicas_is_clamped_by_budget_room() -> None:
    # max_total=3 with current P1,D2 leaves room for exactly one replica.
    decision = MODULE.decide(
        MODULE.Replicas(1, 2),
        metrics(prefill_backlog=1_000.0, decode_kv=30.0),
        cfg(min_observations=1, max_total=3, max_step_replicas=3),
        MODULE.PlannerState(),
        now=1_000_000.0,
    )
    assert decision.target == MODULE.Replicas(2, 1)
    assert "D->P x1" in decision.reason


def test_max_step_replicas_is_clamped_by_role_floor() -> None:
    # Two decode replicas but min_decode=1 -> at most one may move.
    decision = MODULE.decide(
        MODULE.Replicas(1, 2),
        metrics(prefill_backlog=1_000.0, decode_kv=30.0),
        cfg(min_observations=1, max_total=4, max_step_replicas=5),
        MODULE.PlannerState(),
        now=1_000_000.0,
    )
    assert decision.target == MODULE.Replicas(2, 1)


def test_max_step_replicas_applies_in_both_directions() -> None:
    decision = MODULE.decide(
        MODULE.Replicas(3, 1),
        metrics(prefill_backlog=50.0, decode_kv=95.0),
        cfg(min_observations=1, max_total=4, max_step_replicas=2),
        MODULE.PlannerState(),
        now=1_000_000.0,
    )
    assert decision.target == MODULE.Replicas(1, 3)
    assert "P->D x2" in decision.reason


def test_default_step_is_one_replica_for_backward_compatibility() -> None:
    assert MODULE.default_config().max_step_replicas == 1
    decision = MODULE.decide(
        MODULE.Replicas(1, 3),
        metrics(prefill_backlog=1_000.0, decode_kv=30.0),
        cfg(min_observations=1, max_total=4),
        MODULE.PlannerState(),
        now=1_000_000.0,
    )
    assert decision.target == MODULE.Replicas(2, 2)


def test_max_step_replicas_must_be_positive() -> None:
    import pytest

    with pytest.raises(ValueError, match="max_step_replicas"):
        cfg(max_step_replicas=0).validate()


def test_both_roles_pressured_means_no_change() -> None:
    decision = MODULE.decide(
        MODULE.Replicas(2, 1),
        metrics(prefill_backlog=1_000.0, decode_kv=90.0),
        cfg(min_observations=1),
        MODULE.PlannerState(),
        now=1_000_000.0,
    )
    assert decision.target is None
    assert "no change" in decision.reason


def test_hysteresis_gap_is_neutral() -> None:
    # 200 tokens sits between down(128) and up(512): neither pressure nor relaxed.
    decision = MODULE.decide(
        MODULE.Replicas(1, 2),
        metrics(prefill_backlog=200.0, decode_kv=70.0),
        cfg(min_observations=1),
        MODULE.PlannerState(),
        now=1_000_000.0,
    )
    assert decision.target is None


def test_invalid_metrics_are_rejected() -> None:
    import pytest

    with pytest.raises(ValueError, match="0..100"):
        MODULE.decide(
            MODULE.Replicas(1, 2),
            metrics(prefill_backlog=10.0, decode_kv=101.0),
            cfg(),
            MODULE.PlannerState(),
            now=1_000_000.0,
        )


def test_state_round_trip() -> None:
    state = MODULE.PlannerState(
        last_change_timestamp=123.0,
        decode_to_prefill_streak=3,
        prefill_to_decode_streak=0,
    )
    restored = MODULE.PlannerState.from_dict(state.to_dict())
    assert restored == state
