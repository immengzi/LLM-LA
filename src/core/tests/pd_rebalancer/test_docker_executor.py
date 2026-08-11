import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


FILES_DIR = Path(__file__).parents[2] / "vllm-kv-stack" / "files"
if str(FILES_DIR) not in sys.path:
    sys.path.insert(0, str(FILES_DIR))

CORE_SPEC = importlib.util.spec_from_file_location("pd_rebalancer", FILES_DIR / "pd_rebalancer.py")
CORE = importlib.util.module_from_spec(CORE_SPEC)
assert CORE_SPEC and CORE_SPEC.loader
sys.modules["pd_rebalancer"] = CORE
CORE_SPEC.loader.exec_module(CORE)

MODULE_PATH = FILES_DIR / "pd_rebalancer_docker.py"
SPEC = importlib.util.spec_from_file_location("pd_rebalancer_docker", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules["pd_rebalancer_docker"] = MODULE
SPEC.loader.exec_module(MODULE)


def config() -> MODULE.Config:
    return MODULE.load_config()


def test_apply_plan_reuses_core_transition_plan() -> None:
    plan = MODULE.transition_plan(
        MODULE.Replicas(2, 1),
        MODULE.Replicas(1, 2),
        config().model_config(),
    )
    assert plan == [("prefill", 1), ("decode", 2)]


def test_reverse_apply_plan_order() -> None:
    plan = MODULE.transition_plan(
        MODULE.Replicas(1, 2),
        MODULE.Replicas(2, 1),
        config().model_config(),
    )
    assert plan == [("decode", 1), ("prefill", 2)]


def test_config_rejects_npu_outside_allocation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    raw = {
        "allowed_npus": [0, 1, 2],
        "slots": [
            {
                "npu": 0,
                "prefill_http": 19100,
                "prefill_kv": 19300,
                "decode_http": 19200,
                "decode_kv": 19400,
            },
            {
                "npu": 8,
                "prefill_http": 19101,
                "prefill_kv": 19301,
                "decode_http": 19201,
                "decode_kv": 19401,
            },
        ],
    }
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(raw), encoding="utf-8")
    monkeypatch.setenv("LZM_DYNPD_CONFIG", str(config_path))
    with pytest.raises(ValueError, match="outside the allowed allocation"):
        MODULE.load_config()


def _slot_for_npu(cfg: MODULE.Config, npu: int) -> MODULE.Slot:
    return next(slot for slot in cfg.slots if slot.npu == npu)


def test_default_engine_settings_match_verified_lab() -> None:
    cfg = MODULE.load_config()
    assert cfg.tensor_parallel_size == 1
    assert cfg.max_model_len == 4096
    assert cfg.max_num_batched_tokens == 4096
    assert cfg.max_num_seqs == 16
    assert cfg.gpu_memory_utilization == 0.60
    assert cfg.enforce_eager is True


def test_config_loads_engine_overrides(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    raw = {
        "allowed_npus": [0, 1, 2],
        "tensor_parallel_size": 2,
        "max_model_len": 8192,
        "max_num_batched_tokens": 2048,
        "max_num_seqs": 8,
        "gpu_memory_utilization": 0.75,
        "enforce_eager": "false",
        "served_model_name": "my-model",
    }
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(raw), encoding="utf-8")
    monkeypatch.setenv("LZM_DYNPD_CONFIG", str(config_path))
    cfg = MODULE.load_config()
    assert cfg.tensor_parallel_size == 2
    assert cfg.max_model_len == 8192
    assert cfg.max_num_batched_tokens == 2048
    assert cfg.max_num_seqs == 8
    assert cfg.gpu_memory_utilization == 0.75
    assert cfg.enforce_eager is False
    assert cfg.served_model_name == "my-model"


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        ({"tensor_parallel_size": 0}, "tensor_parallel_size"),
        ({"max_model_len": 0}, "max_model_len"),
        ({"max_num_batched_tokens": 0}, "max_num_batched_tokens"),
        ({"max_num_seqs": 0}, "max_num_seqs"),
        ({"gpu_memory_utilization": 0.0}, "gpu_memory_utilization"),
        ({"gpu_memory_utilization": 1.5}, "gpu_memory_utilization"),
    ],
)
def test_config_rejects_invalid_engine_settings(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    raw: dict,
    message: str,
) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(raw), encoding="utf-8")
    monkeypatch.setenv("LZM_DYNPD_CONFIG", str(config_path))
    with pytest.raises(ValueError, match=message):
        MODULE.load_config()


def test_kv_config_uses_configured_tensor_parallel_size(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    raw = {"allowed_npus": [0, 1, 2], "tensor_parallel_size": 2}
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(raw), encoding="utf-8")
    monkeypatch.setenv("LZM_DYNPD_CONFIG", str(config_path))
    cfg = MODULE.load_config()
    payload = json.loads(MODULE.kv_config(cfg, "prefill", cfg.slots[0]))
    assert payload["kv_connector_extra_config"]["prefill"]["tp_size"] == 2
    assert payload["kv_connector_extra_config"]["decode"]["tp_size"] == 2


def test_run_role_passes_engine_args_from_config(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    raw = {
        "allowed_npus": [0, 1, 2],
        "tensor_parallel_size": 2,
        "max_model_len": 8192,
        "max_num_batched_tokens": 2048,
        "max_num_seqs": 8,
        "gpu_memory_utilization": 0.75,
        "enforce_eager": False,
    }
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(raw), encoding="utf-8")
    monkeypatch.setenv("LZM_DYNPD_CONFIG", str(config_path))
    cfg = MODULE.load_config()

    captured: dict[str, list[str]] = {}

    def fake_docker(*args: str, check: bool = True, timeout: int = 600) -> SimpleNamespace:
        captured["args"] = list(args)
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(MODULE, "docker", fake_docker)
    monkeypatch.setattr(MODULE, "wait_http", lambda *args, **kwargs: None)

    MODULE.run_role(cfg, "prefill", cfg.slots[0])

    args = captured["args"]
    assert args[args.index("--tensor-parallel-size") + 1] == "2"
    assert args[args.index("--max-model-len") + 1] == "8192"
    assert args[args.index("--max-num-batched-tokens") + 1] == "2048"
    assert args[args.index("--max-num-seqs") + 1] == "8"
    assert args[args.index("--gpu-memory-utilization") + 1] == "0.75"
    assert "--enforce-eager" not in args


def test_removal_prefers_high_npu_for_prefill_and_low_for_decode() -> None:
    cfg = config()
    occupied = [
        ("prefill", _slot_for_npu(cfg, 0)),
        ("decode", _slot_for_npu(cfg, 1)),
        ("prefill", _slot_for_npu(cfg, 2)),
    ]
    assert MODULE.select_removals(occupied, "prefill", 1) == [_slot_for_npu(cfg, 2)]

    occupied = [
        ("prefill", _slot_for_npu(cfg, 0)),
        ("decode", _slot_for_npu(cfg, 1)),
        ("decode", _slot_for_npu(cfg, 2)),
    ]
    assert MODULE.select_removals(occupied, "decode", 1) == [_slot_for_npu(cfg, 1)]


def test_addition_prefers_low_npu_for_prefill_and_high_for_decode() -> None:
    cfg = config()
    occupied = [("prefill", _slot_for_npu(cfg, 0)), ("decode", _slot_for_npu(cfg, 2))]
    assert MODULE.select_additions(cfg, occupied, "prefill", 1) == [_slot_for_npu(cfg, 1)]
    assert MODULE.select_additions(cfg, occupied, "decode", 1) == [_slot_for_npu(cfg, 1)]


class FakeBackend:
    """In-memory mirror of owned containers for apply-order verification."""

    def __init__(self) -> None:
        self.roles: dict[int, str] = {}  # slot index -> role
        self.events: list[tuple[str, str, int]] = []

    def ensure(self, cfg: MODULE.Config) -> MODULE.Replicas:
        return self.current(cfg)

    def current(self, cfg: MODULE.Config) -> MODULE.Replicas:
        counts = {"prefill": 0, "decode": 0}
        for role in self.roles.values():
            counts[role] += 1
        return MODULE.Replicas(prefill=counts["prefill"], decode=counts["decode"])

    def occupied(self, cfg: MODULE.Config) -> list[tuple[str, MODULE.Slot]]:
        return [(role, cfg.slots[idx]) for idx, role in sorted(self.roles.items())]

    def add(self, cfg: MODULE.Config, role: str, slot: MODULE.Slot) -> None:
        idx = cfg.slots.index(slot)
        self.events.append(("add", role, slot.npu))
        self.roles[idx] = role

    def remove(self, cfg: MODULE.Config, role: str, slot: MODULE.Slot) -> None:
        idx = cfg.slots.index(slot)
        self.events.append(("remove", role, slot.npu))
        self.roles.pop(idx, None)

    def wait_topology(self, cfg: MODULE.Config, expected: MODULE.Replicas) -> None:
        return


def _patch_backend(monkeypatch: pytest.MonkeyPatch, backend: FakeBackend) -> None:
    monkeypatch.setattr(MODULE, "ensure_base", backend.ensure)
    monkeypatch.setattr(MODULE, "current_counts", backend.current)
    monkeypatch.setattr(MODULE, "occupied_slots", backend.occupied)
    monkeypatch.setattr(MODULE, "add_role", backend.add)
    monkeypatch.setattr(MODULE, "remove_role", backend.remove)
    monkeypatch.setattr(MODULE, "wait_topology", backend.wait_topology)


def test_apply_removes_source_role_before_adding_target_role(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = config()
    backend = FakeBackend()
    backend.roles = {0: "prefill", 1: "decode", 2: "prefill"}  # P2,D1
    _patch_backend(monkeypatch, backend)

    MODULE.apply_target(cfg, MODULE.Replicas(prefill=1, decode=2))

    events = backend.events
    remove_prefill = next(i for i, (op, role, _) in enumerate(events) if op == "remove" and role == "prefill")
    add_decode = next(i for i, (op, role, _) in enumerate(events) if op == "add" and role == "decode")
    assert remove_prefill < add_decode
    assert events == [("remove", "prefill", 2), ("add", "decode", 2)]
    assert backend.current(cfg) == MODULE.Replicas(prefill=1, decode=2)


def test_apply_reverse_removes_decode_before_adding_prefill(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = config()
    backend = FakeBackend()
    backend.roles = {0: "prefill", 1: "decode", 2: "decode"}  # P1,D2
    _patch_backend(monkeypatch, backend)

    MODULE.apply_target(cfg, MODULE.Replicas(prefill=2, decode=1))

    assert backend.events == [("remove", "decode", 1), ("add", "prefill", 1)]
    assert backend.current(cfg) == MODULE.Replicas(prefill=2, decode=1)


def test_apply_rejects_target_over_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = config()
    backend = FakeBackend()
    backend.roles = {0: "prefill", 1: "decode", 2: "decode"}  # P1,D2
    _patch_backend(monkeypatch, backend)

    with pytest.raises(ValueError, match="budget"):
        MODULE.apply_target(cfg, MODULE.Replicas(prefill=2, decode=2))


def test_owned_container_names_only_matches_prefix() -> None:
    def fake_docker(*args: str, check: bool = True, timeout: int = 600) -> SimpleNamespace:
        assert args[:2] == ("ps", "-a")
        return SimpleNamespace(
            stdout="lzm-dynpd-p0\nlzm-dynpd-d1\nlzm-dynpd-proxy\nother-user-container\n",
            returncode=0,
            stderr="",
        )

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(MODULE, "docker", fake_docker)
    try:
        names = MODULE.owned_container_names("lzm-dynpd")
        assert names == ["lzm-dynpd-p0", "lzm-dynpd-d1", "lzm-dynpd-proxy"]
    finally:
        monkeypatch.undo()
