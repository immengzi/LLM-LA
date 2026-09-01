"""Offline decision-correctness matrix for the advisory P/D planner.

Covers the five items of layer 1 (offline, pure-function) from
``docs/validation/planner-validation-metrics.md``:

1. Threshold boundary matrix at 512/128 backlog tokens and 80%/60% KV
   utilization, with +-1 assertions around each boundary.
2. Budget invariants: prefill >= 1, decode >= 1, prefill + decode <= max_total,
   and at most one replica moved per transition.
3. Debounce: N consecutive same-direction samples are required, and an
   opposite/neutral sample resets the streak.
4. Cooldown: 300s default; transitions are blocked while it is active and
   allowed at the exact boundary.
5. State persistence round trip through the JSON state file (CLI-level).

The module under test is loaded the same way as ``test_pd_planner.py`` so the
planner can be exercised without a cluster or external dependencies.
"""

import importlib.util
import json
import sys
from pathlib import Path

import pytest


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


# Documentation thresholds, hardcoded in the oracle so the matrix checks the
# policy's boundary semantics rather than echoing the config object.
PREFILL_UP = 512.0
PREFILL_DOWN = 128.0
KV_UP = 80.0
KV_DOWN = 60.0

PREFILL_BOUNDARY_VALUES = [PREFILL_DOWN - 1, PREFILL_DOWN, PREFILL_DOWN + 1,
                           PREFILL_UP - 1, PREFILL_UP, PREFILL_UP + 1]
KV_BOUNDARY_VALUES = [KV_DOWN - 1, KV_DOWN, KV_DOWN + 1,
                      KV_UP - 1, KV_UP, KV_UP + 1]


def cfg(**overrides) -> MODULE.PlannerConfig:
    values = dict(
        min_prefill=1,
        min_decode=1,
        max_total=3,
        prefill_scale_up_tokens=PREFILL_UP,
        prefill_scale_down_tokens=PREFILL_DOWN,
        decode_scale_up_kv_percent=KV_UP,
        decode_scale_down_kv_percent=KV_DOWN,
        min_observations=5,
        cooldown_seconds=300.0,
    )
    values.update(overrides)
    return MODULE.PlannerConfig(**values)


def metrics(prefill_backlog: float = 0.0, decode_kv: float = 0.0) -> MODULE.MetricsSnapshot:
    return MODULE.MetricsSnapshot(
        prefill_backlog_tokens=prefill_backlog,
        decode_kv_usage_percent=decode_kv,
    )


def oracle_target(backlog: float, kv: float, current: MODULE.Replicas) -> MODULE.Replicas | None:
    """Independent boundary oracle: classify pressure and budget feasibility."""

    prefill_pressure = backlog >= PREFILL_UP
    prefill_relaxed = backlog <= PREFILL_DOWN
    decode_pressure = kv >= KV_UP
    decode_relaxed = kv <= KV_DOWN

    wants_d2p = (
        prefill_pressure
        and decode_relaxed
        and current.decode > 1
        and current.prefill + 1 <= 3
    )
    wants_p2d = (
        decode_pressure
        and prefill_relaxed
        and current.prefill > 1
        and current.decode + 1 <= 3
    )
    if wants_d2p and not wants_p2d:
        return MODULE.Replicas(current.prefill + 1, current.decode - 1)
    if wants_p2d and not wants_d2p:
        return MODULE.Replicas(current.prefill - 1, current.decode + 1)
    return None


def test_documented_defaults() -> None:
    """The defaults shipped by the planner must match the validation document."""

    default = MODULE.default_config()
    assert default.prefill_scale_up_tokens == 512.0
    assert default.prefill_scale_down_tokens == 128.0
    assert default.decode_scale_up_kv_percent == 80.0
    assert default.decode_scale_down_kv_percent == 60.0
    assert default.min_observations == 5
    assert default.cooldown_seconds == 300.0


@pytest.mark.parametrize("backlog", PREFILL_BOUNDARY_VALUES)
@pytest.mark.parametrize("kv", KV_BOUNDARY_VALUES)
def test_boundary_matrix_decode_to_prefill(backlog: float, kv: float) -> None:
    """D->P is decided exactly when backlog >= 512 and KV <= 60 (plus-/-1 grid)."""

    current = MODULE.Replicas(1, 2)  # decode has headroom to release
    expected = oracle_target(backlog, kv, current)
    decision = MODULE.decide(
        current,
        metrics(prefill_backlog=backlog, decode_kv=kv),
        cfg(min_observations=1, cooldown_seconds=0.0),
        MODULE.PlannerState(),
        now=1_000_000.0,
    )
    assert decision.target == expected, (backlog, kv, decision.reason)
    if expected is not None:
        assert "D->P" in decision.reason


@pytest.mark.parametrize("backlog", PREFILL_BOUNDARY_VALUES)
@pytest.mark.parametrize("kv", KV_BOUNDARY_VALUES)
def test_boundary_matrix_prefill_to_decode(backlog: float, kv: float) -> None:
    """P->D is decided exactly when KV >= 80 and backlog <= 128 (plus-/-1 grid)."""

    current = MODULE.Replicas(2, 1)  # prefill has headroom to release
    expected = oracle_target(backlog, kv, current)
    decision = MODULE.decide(
        current,
        metrics(prefill_backlog=backlog, decode_kv=kv),
        cfg(min_observations=1, cooldown_seconds=0.0),
        MODULE.PlannerState(),
        now=1_000_000.0,
    )
    assert decision.target == expected, (backlog, kv, decision.reason)
    if expected is not None:
        assert "P->D" in decision.reason


@pytest.mark.parametrize("prefill", (1, 2))
@pytest.mark.parametrize("decode", (1, 2))
def test_budget_invariants_hold_for_all_reachable_states(prefill: int, decode: int) -> None:
    """Every feasible (P,D) under max_total=3 must never be moved out of budget."""

    current = MODULE.Replicas(prefill, decode)
    if prefill + decode > 3:
        pytest.skip("state unreachable under max_total=3")

    prefill_samples = [0.0, 127.0, 128.0, 129.0, 511.0, 512.0, 513.0, 1_000_000.0]
    kv_samples = [0.0, 59.0, 60.0, 61.0, 79.0, 80.0, 81.0, 100.0]
    for backlog in prefill_samples:
        for kv in kv_samples:
            decision = MODULE.decide(
                current,
                metrics(prefill_backlog=backlog, decode_kv=kv),
                cfg(min_observations=1, cooldown_seconds=0.0),
                MODULE.PlannerState(),
                now=1_000_000.0,
            )
            target = decision.target
            if target is None:
                continue
            assert target.prefill >= 1, (backlog, kv)
            assert target.decode >= 1, (backlog, kv)
            assert target.prefill + target.decode <= 3, (backlog, kv)
            # Exactly one replica moves between roles: one up, one down, total fixed.
            assert target.prefill + target.decode == current.prefill + current.decode, (backlog, kv)
            assert abs(target.prefill - current.prefill) == 1, (backlog, kv)
            assert abs(target.decode - current.decode) == 1, (backlog, kv)


def test_budget_invariants_when_both_roles_at_floor() -> None:
    """(1,1) has no movable replica: any pressure combination stays put."""

    for backlog, kv in [
        (1_000_000.0, 100.0),  # both pressured
        (1_000_000.0, 0.0),    # prefill pressured only
        (0.0, 100.0),          # decode pressured only
        (0.0, 0.0),            # both relaxed
    ]:
        decision = MODULE.decide(
            MODULE.Replicas(1, 1),
            metrics(prefill_backlog=backlog, decode_kv=kv),
            cfg(min_observations=1, cooldown_seconds=0.0),
            MODULE.PlannerState(),
            now=1_000_000.0,
        )
        assert decision.target is None


def test_debounce_requires_five_consecutive_samples() -> None:
    """Default min_observations=5: samples 1-4 only accumulate, sample 5 acts."""

    current = MODULE.Replicas(1, 2)
    state = MODULE.PlannerState()
    for expected_streak in range(1, 5):
        decision = MODULE.decide(
            current,
            metrics(prefill_backlog=1_000.0, decode_kv=30.0),
            cfg(),  # min_observations=5, cooldown_seconds=300
            state,
            now=1_000_000.0,
        )
        assert decision.target is None
        assert decision.state.decode_to_prefill_streak == expected_streak
        assert "D->P candidate observed" in decision.reason
        state = decision.state

    decision = MODULE.decide(
        current,
        metrics(prefill_backlog=1_000.0, decode_kv=30.0),
        cfg(),
        state,
        now=1_000_000.0,
    )
    assert decision.target == MODULE.Replicas(2, 1)


def test_debounce_opposite_direction_resets_streak() -> None:
    """A mid-way opposite-direction sample clears the accumulated streak."""

    state = MODULE.PlannerState()
    state = MODULE.decide(
        MODULE.Replicas(1, 2),
        metrics(prefill_backlog=1_000.0, decode_kv=30.0),  # D->P candidate
        cfg(),
        state,
        now=1_000_000.0,
    ).state
    assert state.decode_to_prefill_streak == 1

    decision = MODULE.decide(
        MODULE.Replicas(2, 1),
        metrics(prefill_backlog=50.0, decode_kv=90.0),  # P->D candidate
        cfg(),
        state,
        now=1_000_000.0,
    )
    assert decision.target is None
    assert decision.state.decode_to_prefill_streak == 0
    assert decision.state.prefill_to_decode_streak == 1


def test_debounce_neutral_sample_resets_streak() -> None:
    """A neutral sample (hysteresis gap) also clears both streaks."""

    state = MODULE.decide(
        MODULE.Replicas(1, 2),
        metrics(prefill_backlog=1_000.0, decode_kv=30.0),
        cfg(),
        MODULE.PlannerState(),
        now=1_000_000.0,
    ).state
    assert state.decode_to_prefill_streak == 1

    decision = MODULE.decide(
        MODULE.Replicas(1, 2),
        metrics(prefill_backlog=200.0, decode_kv=70.0),  # between both threshold pairs
        cfg(),
        state,
        now=1_000_000.0,
    )
    assert decision.target is None
    assert decision.state.decode_to_prefill_streak == 0
    assert decision.state.prefill_to_decode_streak == 0


def test_cooldown_blocks_until_300s_boundary() -> None:
    """300s cooldown: 299.999s blocks, exactly 300s allows."""

    current = MODULE.Replicas(1, 2)
    state = MODULE.PlannerState(last_change_timestamp=1_000_000.0)
    block_config = cfg(min_observations=1, cooldown_seconds=300.0)

    blocked = MODULE.decide(
        current,
        metrics(prefill_backlog=1_000.0, decode_kv=30.0),
        block_config,
        state,
        now=1_000_299.0,
    )
    assert blocked.target is None
    assert "cooldown" in blocked.reason

    allowed = MODULE.decide(
        current,
        metrics(prefill_backlog=1_000.0, decode_kv=30.0),
        block_config,
        state,
        now=1_000_300.0,
    )
    assert allowed.target == MODULE.Replicas(2, 1)


def test_streak_accumulates_during_cooldown_then_fires() -> None:
    """Pressure observed during cooldown keeps counting; transition fires at expiry."""

    current = MODULE.Replicas(1, 2)
    state = MODULE.PlannerState(last_change_timestamp=1_000_000.0)
    config = cfg()  # min_observations=5, cooldown_seconds=300

    for _ in range(5):
        decision = MODULE.decide(
            current,
            metrics(prefill_backlog=1_000.0, decode_kv=30.0),
            config,
            state,
            now=1_000_100.0,
        )
        assert decision.target is None
        assert "cooldown" in decision.reason
        state = decision.state
    assert state.decode_to_prefill_streak == 5

    fired = MODULE.decide(
        current,
        metrics(prefill_backlog=1_000.0, decode_kv=30.0),
        config,
        state,
        now=1_000_300.0,
    )
    assert fired.target == MODULE.Replicas(2, 1)
    assert fired.state.last_change_timestamp == 1_000_300.0
    assert fired.state.decode_to_prefill_streak == 0


def test_state_dict_round_trip_preserves_all_fields() -> None:
    """PlannerState to_dict/from_dict is a lossless round trip."""

    state = MODULE.PlannerState(
        last_change_timestamp=123.456,
        decode_to_prefill_streak=4,
        prefill_to_decode_streak=2,
    )
    assert MODULE.PlannerState.from_dict(state.to_dict()) == state
    assert MODULE.PlannerState.from_dict({}) == MODULE.PlannerState()


def test_cli_state_file_round_trip(tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
    """CLI --state writes progress, and a second invocation resumes from it."""

    metrics_file = tmp_path / "metrics.json"
    metrics_file.write_text(json.dumps({"prefill_backlog_tokens": 1_000.0, "decode_kv_usage_percent": 30.0}))
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps({"min_observations": 2}))
    state_file = tmp_path / "state.json"
    argv = [
        "--metrics", str(metrics_file),
        "--current", "1,2",
        "--state", str(state_file),
        "--config", str(config_file),
        "--now", "1000000",
    ]

    assert MODULE.advise_cli(argv) == 0
    capsys.readouterr()  # discard first-run output
    persisted = json.loads(state_file.read_text())
    assert persisted["decode_to_prefill_streak"] == 1
    assert persisted["prefill_to_decode_streak"] == 0

    assert MODULE.advise_cli(argv) == 0
    persisted = json.loads(state_file.read_text())
    assert persisted["decode_to_prefill_streak"] == 0
    assert persisted["prefill_to_decode_streak"] == 0
    assert persisted["last_change_timestamp"] == 1_000_000.0

    second_run = json.loads(capsys.readouterr().out)
    assert second_run["target"] == {"prefill": 2, "decode": 1}


def test_cli_state_file_resets_on_neutral_sample(tmp_path: Path) -> None:
    """A neutral sample persisted through --state clears accumulated streaks."""

    metrics_file = tmp_path / "metrics.json"
    metrics_file.write_text(json.dumps({"prefill_backlog_tokens": 1_000.0, "decode_kv_usage_percent": 30.0}))
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps({"min_observations": 5}))
    state_file = tmp_path / "state.json"
    base_argv = [
        "--metrics", str(metrics_file),
        "--current", "1,2",
        "--state", str(state_file),
        "--config", str(config_file),
        "--now", "1000000",
    ]

    MODULE.advise_cli(base_argv)
    assert json.loads(state_file.read_text())["decode_to_prefill_streak"] == 1

    metrics_file.write_text(json.dumps({"prefill_backlog_tokens": 200.0, "decode_kv_usage_percent": 70.0}))
    MODULE.advise_cli(base_argv)
    persisted = json.loads(state_file.read_text())
    assert persisted["decode_to_prefill_streak"] == 0
    assert persisted["prefill_to_decode_streak"] == 0
    assert persisted["last_change_timestamp"] == 0.0
