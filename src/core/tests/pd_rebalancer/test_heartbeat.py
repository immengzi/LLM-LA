"""Unit tests for worker-loop heartbeats and /healthz staleness."""

import importlib.util
import json
import sys
import threading
import time
from pathlib import Path


FILES_DIR = Path(__file__).parents[2] / "vllm-kv-stack" / "files"
if str(FILES_DIR) not in sys.path:
    sys.path.insert(0, str(FILES_DIR))

SPEC = importlib.util.spec_from_file_location("pd_rebalancer", FILES_DIR / "pd_rebalancer.py")
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules["pd_rebalancer"] = MODULE
SPEC.loader.exec_module(MODULE)


class FakeRebalancer:
    def __init__(self) -> None:
        self.models = {
            "qwen": MODULE.ModelConfig(
                "qwen", "qwen-prefill", "qwen-decode", "qwen-proxy", 1, 1, 3
            )
        }
        self.heartbeat_timeout = 60.0
        self.heartbeats = {"rebalancer": 0.0, "planner": 0.0}
        self.heartbeat_lock = threading.Lock()

    def stamp_heartbeat(self, component: str) -> None:
        with self.heartbeat_lock:
            self.heartbeats[component] = time.monotonic()


def assert_fresh(rebalancer, component: str) -> None:
    assert time.monotonic() - rebalancer.heartbeats[component] < 1.0


def test_rebalancer_run_stamps_heartbeat() -> None:
    rebalancer = MODULE.Rebalancer.__new__(MODULE.Rebalancer)
    rebalancer._first_pass = False
    rebalancer.last_error = ""
    rebalancer.poll_seconds = 0
    rebalancer.heartbeat_timeout = 60.0
    rebalancer.heartbeats = {"rebalancer": 0.0, "planner": 0.0}
    rebalancer.heartbeat_lock = threading.Lock()
    rebalancer.targets = lambda: {}  # type: ignore[method-assign]

    rebalancer.run(max_iters=2)

    assert_fresh(rebalancer, "rebalancer")


def test_planner_run_once_stamps_heartbeat() -> None:
    loop = MODULE.PlannerLoop.__new__(MODULE.PlannerLoop)
    loop.rebalancer = FakeRebalancer()
    loop.advisory = True
    loop.config_overrides = {}
    loop._config = lambda: None  # type: ignore[method-assign]
    loop._endpoint_ips = lambda service: []  # type: ignore[method-assign]

    loop.run_once()

    assert_fresh(loop.rebalancer, "planner")


def test_planner_fetch_stamps_heartbeat(monkeypatch) -> None:
    loop = MODULE.PlannerLoop.__new__(MODULE.PlannerLoop)
    loop.rebalancer = FakeRebalancer()
    loop.metrics_port = 8200
    loop.proxy_metrics_port = 8200

    def unreachable(*_args, **_kwargs):
        raise TimeoutError("unreachable")

    monkeypatch.setattr(MODULE.urllib.request, "urlopen", unreachable)

    assert loop._fetch("10.0.0.1") is None
    assert_fresh(loop.rebalancer, "planner")


def make_healthy() -> object:
    rebalancer = MODULE.Rebalancer.__new__(MODULE.Rebalancer)
    rebalancer.dry_run = False
    rebalancer.last_error = ""
    rebalancer.planner_error = ""
    rebalancer.heartbeat_timeout = 60.0
    rebalancer.heartbeats = {
        "rebalancer": time.monotonic(),
        "planner": time.monotonic(),
    }
    rebalancer.heartbeat_lock = threading.Lock()
    return rebalancer


class _Wfile:
    def __init__(self) -> None:
        self.buf = b""

    def write(self, data: bytes) -> None:
        self.buf += data


def get_healthz(rebalancer) -> tuple[int, dict]:
    handler = MODULE.handler_for(rebalancer).__new__(MODULE.handler_for(rebalancer))
    handler.path = "/healthz"
    handler.headers = {}
    handler.wfile = _Wfile()
    handler.send_response = lambda code: setattr(handler, "code", code)
    handler.send_header = lambda *_: None
    handler.end_headers = lambda: None
    handler.do_GET()
    return handler.code, json.loads(handler.wfile.buf.decode())


def test_healthz_ok_when_both_loops_fresh() -> None:
    rebalancer = make_healthy()
    code, payload = get_healthz(rebalancer)
    assert code == 200
    assert payload["status"] == "ok"
    assert payload["staleLoops"] == []


def test_healthz_503_when_loop_stale_and_recovers_after_stamp() -> None:
    rebalancer = make_healthy()
    rebalancer.heartbeats["planner"] = (
        time.monotonic() - rebalancer.heartbeat_timeout - 5
    )

    code, payload = get_healthz(rebalancer)
    assert code == 503
    assert payload["status"] == "unhealthy"
    assert payload["staleLoops"] == ["planner"]
    assert payload["heartbeatTimeout"] == 60.0

    rebalancer.stamp_heartbeat("planner")
    code, payload = get_healthz(rebalancer)
    assert code == 200
    assert payload["status"] == "ok"
    assert payload["staleLoops"] == []
