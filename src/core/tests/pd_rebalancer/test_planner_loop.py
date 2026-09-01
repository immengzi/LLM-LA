"""Unit tests for the PlannerLoop (advisory vs automatic P/D switching)."""

import importlib.util
import json
import sys
import threading
import time as time_module
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

PLANNER_PATH = FILES_DIR / "pd_planner.py"
PLANNER_SPEC = importlib.util.spec_from_file_location("pd_planner", PLANNER_PATH)
PLANNER = importlib.util.module_from_spec(PLANNER_SPEC)
assert PLANNER_SPEC and PLANNER_SPEC.loader
sys.modules["pd_planner"] = PLANNER
PLANNER_SPEC.loader.exec_module(PLANNER)

MODULE = CORE


class FakeKubernetesApi:
    def __init__(self) -> None:
        self.data = {"targets.json": "{}"}
        self.calls: list[str] = []

    def state(self, name: str) -> dict[str, object]:
        return {"data": dict(self.data)}

    def patch_data(self, name: str, data: dict[str, str]) -> None:
        self.calls.append("patch_data")
        self.data.update(data)


class FakeRebalancer:
    def __init__(self, current: tuple[int, int] = (2, 1)) -> None:
        self.namespace = "dyn-pd"
        self.state_configmap = "state"
        self.models = {
            "qwen": MODULE.ModelConfig(
                "qwen",
                "vllm-qwen-prefill",
                "vllm-qwen-decode",
                "vllm-qwen",
                1,
                1,
                3,
            )
        }
        self.api = FakeKubernetesApi()
        self.proposals: list[tuple[str, MODULE.Replicas, str]] = []
        self.commits: list[str] = []
        self.planner_error = ""
        self._current = current
        self.heartbeat_timeout = 60.0
        self.heartbeats = {
            "rebalancer": time_module.monotonic(),
            "planner": time_module.monotonic(),
        }
        self.heartbeat_lock = threading.Lock()

    def stamp_heartbeat(self, component: str) -> None:
        self.heartbeats[component] = time_module.monotonic()

    def current(self, config: MODULE.ModelConfig) -> MODULE.Replicas:
        return MODULE.Replicas(*self._current)

    def propose(self, model: str, target: MODULE.Replicas, reason: str = "") -> None:
        self.proposals.append((model, target, reason))

    def commit(self, model: str) -> None:
        self.commits.append(model)

    def transition_active(self, model: str) -> bool:
        return False


def make_loop(advisory: bool, overrides: dict[str, float] | None = None) -> MODULE.PlannerLoop:
    rebalancer = FakeRebalancer()
    loop = MODULE.PlannerLoop.__new__(MODULE.PlannerLoop)
    loop.rebalancer = rebalancer
    loop.advisory = advisory
    loop.poll_seconds = 0.0
    loop.metrics_port = 8200
    loop.proxy_metrics_port = 8200
    loop.config_overrides = overrides or {
        "decode_scale_up_kv_percent": 2.0,
        "decode_scale_down_kv_percent": 1.0,
        "min_observations": 1,
        "cooldown_seconds": 0.0,
    }
    loop.last_error = ""
    loop.scrape_deadline = 5.0
    loop._endpoint_ips = lambda service: [  # noqa: E731
        "10.0.0.1" if "prefill" in service
        else "10.0.0.2" if "decode" in service
        else "10.0.0.3"
    ]
    loop._fetch = lambda ip, port=None: (  # noqa: E731
        "pd_proxy_prefill_inflight 0\n"
        "pd_proxy_prefill_requests_total 1\n"
        "pd_proxy_prefill_mean_prompt_tokens 1190.0\n"
        "vllm:kv_cache_usage_perc 0.52\n"
    )
    loop._proxy_signal = lambda ips: (0.0, 1190.0, 1.0, 1)  # inflight 0 -> backlog 0
    return loop


def test_advisory_mode_logs_recommendation_without_applying() -> None:
    loop = make_loop(advisory=True)
    loop.run_once()
    assert loop.rebalancer.proposals == []
    assert loop.rebalancer.commits == []


def test_auto_mode_proposes_and_commits() -> None:
    loop = make_loop(advisory=False)
    loop.run_once()
    assert len(loop.rebalancer.proposals) == 1
    model, target, reason = loop.rebalancer.proposals[0]
    assert model == "qwen"
    assert target == MODULE.Replicas(1, 2)
    assert "P->D" in reason
    assert loop.rebalancer.commits == ["qwen"]


def test_auto_mode_defers_while_transition_in_progress() -> None:
    loop = make_loop(advisory=False)
    loop.rebalancer.transition_active = lambda model: True  # type: ignore[method-assign]
    loop.run_once()
    assert loop.rebalancer.proposals == []
    assert loop.rebalancer.commits == []


def test_relaxed_metrics_produce_no_action() -> None:
    loop = make_loop(advisory=False)
    loop._proxy_signal = lambda ips: (0.0, 0.0, 0.0, 1)
    loop._fetch = lambda ip, port=None: (  # noqa: E731
        "pd_proxy_prefill_inflight 0\n"
        "vllm:kv_cache_usage_perc 0.005\n"
    )
    loop.run_once()
    assert loop.rebalancer.proposals == []
    assert loop.rebalancer.commits == []


def test_default_thresholds_require_higher_kv() -> None:
    loop = make_loop(advisory=False, overrides={"min_observations": 1, "cooldown_seconds": 0.0})
    # Default decode up threshold is 80%; 52% must not trigger.
    loop.run_once()
    assert loop.rebalancer.proposals == []


def test_planner_config_warns_and_skips_unknown_key(capsys) -> None:
    loop = make_loop(
        advisory=True,
        overrides={"min_preffil": 2, "min_observations": 1, "cooldown_seconds": 0.0},
    )
    config = loop._config()
    # Typo'd key must not silently disappear: it is dropped with a warning and
    # the default stays, while the valid overrides still apply.
    assert config.min_prefill == 1
    assert config.min_observations == 1
    assert "min_preffil" in capsys.readouterr().out


def test_planner_config_rejects_fractional_int_without_truncating(capsys) -> None:
    loop = make_loop(advisory=True, overrides={"max_total": 2.9})
    config = loop._config()
    # Old code truncated 2.9 -> 2 silently; now it is ignored with a warning.
    assert config.max_total == 3
    assert "max_total" in capsys.readouterr().out


def test_planner_config_accepts_integral_values_and_strings() -> None:
    loop = make_loop(
        advisory=True,
        overrides={
            "max_total": 4.0,  # integral float is fine for an int field
            "min_observations": "3",  # strings that parse cleanly are accepted
            "decode_scale_up_kv_percent": "90.5",
        },
    )
    config = loop._config()
    assert config.max_total == 4
    assert config.min_observations == 3
    assert config.decode_scale_up_kv_percent == 90.5


def test_planner_state_persisted_in_configmap() -> None:
    loop = make_loop(advisory=True)
    loop.run_once()
    raw = loop.rebalancer.api.data.get("planner_state.qwen.json")
    assert raw is not None
    state = json.loads(raw)
    assert "last_change_timestamp" in state
    assert "prefill_to_decode_streak" in state


def test_planner_state_isolated_per_model() -> None:
    loop = make_loop(advisory=True)
    qwen_state = PLANNER.PlannerState(decode_to_prefill_streak=1)
    llama_state = PLANNER.PlannerState(prefill_to_decode_streak=2)
    loop._save_state("qwen", qwen_state)
    loop._save_state("llama", llama_state)
    data = loop.rebalancer.api.data
    assert "planner_state.qwen.json" in data
    assert "planner_state.llama.json" in data
    assert json.loads(data["planner_state.qwen.json"])["decode_to_prefill_streak"] == 1
    assert json.loads(data["planner_state.llama.json"])["prefill_to_decode_streak"] == 2
    assert json.loads(data["planner_state.qwen.json"]).get("prefill_to_decode_streak", 0) == 0


def test_planner_state_not_written_when_unchanged() -> None:
    loop = make_loop(advisory=True)
    state = PLANNER.PlannerState(decode_to_prefill_streak=3)
    loop._save_state("qwen", state)
    writes_after_first = len(loop.rebalancer.api.calls)
    loop._save_state("qwen", state)
    assert len(loop.rebalancer.api.calls) == writes_after_first


def test_planner_error_surfaces_on_rebalancer() -> None:
    loop = make_loop(advisory=True)

    def boom() -> None:
        raise RuntimeError("boom")

    loop.run_once = boom  # type: ignore[method-assign]
    original_sleep = time_module.sleep
    time_module.sleep = lambda _: (_ for _ in ()).throw(SystemExit)  # type: ignore[assignment]
    try:
        with pytest.raises(SystemExit):
            loop.run()
    finally:
        time_module.sleep = original_sleep
    assert loop.rebalancer.planner_error == "boom"


def test_run_once_skips_when_proxy_metrics_unavailable() -> None:
    loop = make_loop(advisory=False)
    loop._proxy_signal = lambda ips: (0.0, 0.0, 0.0, 0)
    loop.run_once()
    assert loop.rebalancer.proposals == []
    assert loop.rebalancer.commits == []


def test_proxy_signal_parses_single_fetch() -> None:
    loop = make_loop(advisory=True)
    del loop._proxy_signal  # use the real aggregation method
    text = (
        "pd_proxy_prefill_inflight 43\n"
        "pd_proxy_prefill_requests_total 715\n"
        "pd_proxy_prefill_mean_prompt_tokens 1189.247\n"
    )
    loop._fetch = lambda ip, port=None: text  # type: ignore[method-assign]
    inflight, mean, requests, ok = loop._proxy_signal(["10.0.0.3"])
    assert (inflight, mean, requests, ok) == (43.0, 1189.247, 715.0, 1)


def test_proxy_signal_skips_failed_ips() -> None:
    loop = make_loop(advisory=True)
    del loop._proxy_signal  # use the real aggregation method
    good = (
        "pd_proxy_prefill_inflight 2\n"
        "pd_proxy_prefill_requests_total 5\n"
        "pd_proxy_prefill_mean_prompt_tokens 1000.0\n"
    )

    def fetch(ip: str, port: int | None = None) -> str | None:
        return None if ip == "10.0.0.3" else good

    loop._fetch = fetch  # type: ignore[method-assign]
    inflight, mean, requests, ok = loop._proxy_signal(["10.0.0.3", "10.0.0.4"])
    assert ok == 1
    assert (inflight, mean, requests) == (2.0, 1000.0, 5.0)


def test_proxy_signal_weights_means_by_inflight() -> None:
    """The aggregate mean must follow each pod's actual backlog share, not an
    equal-weight average of the per-pod sliding-window means."""
    loop = make_loop(advisory=True)
    del loop._proxy_signal  # use the real aggregation method
    busy = (
        "pd_proxy_prefill_inflight 9\n"
        "pd_proxy_prefill_requests_total 100\n"
        "pd_proxy_prefill_mean_prompt_tokens 1000.0\n"
    )
    idle = (
        "pd_proxy_prefill_inflight 1\n"
        "pd_proxy_prefill_requests_total 100\n"
        "pd_proxy_prefill_mean_prompt_tokens 2000.0\n"
    )

    def fetch(ip: str, port: int | None = None) -> str | None:
        return busy if ip == "10.0.0.3" else idle

    loop._fetch = fetch  # type: ignore[method-assign]
    inflight, mean, requests, ok = loop._proxy_signal(["10.0.0.3", "10.0.0.4"])
    # Equal-weight average would be 1500.0; inflight-weighted is
    # (9*1000 + 1*2000) / (9 + 1) = 1100.0.
    assert (inflight, mean, requests, ok) == (10.0, 1100.0, 200.0, 2)


def test_scrape_uses_max_across_engine_series() -> None:
    """A decode pod can expose one KV series per engine; take the busiest."""
    loop = make_loop(advisory=True)
    text = (
        "# HELP vllm:kv_cache_usage_perc Fraction of GPU KV cache used\n"
        "# TYPE vllm:kv_cache_usage_perc gauge\n"
        'vllm:kv_cache_usage_perc{engine="0",model_name="served-model"} 0.42\n'
        'vllm:kv_cache_usage_perc{engine="1",model_name="served-model"} 0.58\n'
    )
    loop._fetch = lambda ip, port=None: text  # type: ignore[method-assign]
    assert loop._scrape("10.0.0.2", "vllm:kv_cache_usage_perc") == 0.58


def test_run_once_does_not_overflow_multi_series_kv() -> None:
    """Summing engine series could push decode_kv_usage_percent past 100 and
    trip MetricsSnapshot.validate(); max keeps it within 0..100."""
    loop = make_loop(advisory=True)
    del loop._proxy_signal
    decode_text = (
        "# TYPE vllm:kv_cache_usage_perc gauge\n"
        'vllm:kv_cache_usage_perc{engine="0",model_name="served-model"} 0.7\n'
        'vllm:kv_cache_usage_perc{engine="1",model_name="served-model"} 0.7\n'
    )
    proxy_text = (
        "pd_proxy_prefill_inflight 2\n"
        "pd_proxy_prefill_requests_total 5\n"
        "pd_proxy_prefill_mean_prompt_tokens 1000.0\n"
    )

    def fetch(ip: str, port: int | None = None) -> str | None:
        return proxy_text if ip == "10.0.0.3" else decode_text

    loop._fetch = fetch  # type: ignore[method-assign]
    loop.run_once()  # must not raise MetricsSnapshot.validate()


def test_run_once_clamps_out_of_range_kv_gauge() -> None:
    """A KV gauge above 1.0 (e.g. 1.1 -> 110%) must be clamped to 100% instead
    of raising MetricsSnapshot.validate() and aborting the poll cycle."""
    loop = make_loop(advisory=False)
    loop._proxy_signal = lambda ips: (0.0, 0.0, 0.0, 1)  # prefill relaxed
    loop._fetch = lambda ip, port=None: (  # noqa: E731
        "pd_proxy_prefill_inflight 0\n"
        "vllm:kv_cache_usage_perc 1.1\n"
    )
    loop.run_once()  # must not raise
    assert len(loop.rebalancer.proposals) == 1
    model, target, reason = loop.rebalancer.proposals[0]
    assert model == "qwen"
    assert target == MODULE.Replicas(1, 2)
    assert "100.0%" in reason
    assert loop.rebalancer.commits == ["qwen"]


def test_run_once_clamps_negative_kv_gauge() -> None:
    """A negative gauge is clamped to 0% (relaxed) instead of raising."""
    loop = make_loop(advisory=False)
    loop._proxy_signal = lambda ips: (0.0, 0.0, 0.0, 1)  # prefill relaxed
    loop._fetch = lambda ip, port=None: (  # noqa: E731
        "pd_proxy_prefill_inflight 0\n"
        "vllm:kv_cache_usage_perc -0.2\n"
    )
    loop.run_once()  # must not raise
    assert loop.rebalancer.proposals == []
    assert loop.rebalancer.commits == []


def test_run_once_skips_when_decode_metrics_unavailable() -> None:
    loop = make_loop(advisory=False)
    loop._proxy_signal = lambda ips: (3.0, 2000.0, 3.0, 1)
    loop._fetch = lambda ip, port=None: None  # type: ignore[assignment]
    loop.run_once()
    assert loop.rebalancer.proposals == []


def test_backlog_derivation_uses_proxy_inflight_times_mean() -> None:
    loop = make_loop(advisory=True)
    loop._proxy_signal = lambda ips: (3.0, 2000.0, 3.0, 1)  # backlog = 3 * 2000
    loop._fetch = lambda ip, port=None: (  # noqa: E731
        "pd_proxy_prefill_inflight 0\n"
        "vllm:kv_cache_usage_perc 0.0\n"
    )
    loop.run_once()  # must not raise; backlog = 3 * 2000 = 6000


def test_proxy_backlog_triggers_d_to_p_in_auto_mode() -> None:
    loop = make_loop(advisory=False)
    loop.rebalancer._current = (1, 2)  # P1,D2: D->P is budget-feasible
    loop._proxy_signal = lambda ips: (2.0, 2000.0, 2.0, 1)  # mean 2000 -> backlog 4000
    loop._fetch = lambda ip, port=None: (  # noqa: E731
        "pd_proxy_prefill_inflight 0\n"
        "vllm:kv_cache_usage_perc 0.005\n"
    )
    loop.run_once()
    assert len(loop.rebalancer.proposals) == 1
    model, target, reason = loop.rebalancer.proposals[0]
    assert model == "qwen"
    assert target == MODULE.Replicas(2, 1)
    assert "D->P" in reason
    assert loop.rebalancer.commits == ["qwen"]


def test_mean_fallback_used_before_first_prefill_response() -> None:
    loop = make_loop(advisory=False, overrides={
        "decode_scale_up_kv_percent": 2.0,
        "decode_scale_down_kv_percent": 1.0,
        "min_observations": 1,
        "cooldown_seconds": 0.0,
        "prefill_mean_prompt_tokens_fallback": 1000.0,
    })
    loop.rebalancer._current = (1, 2)  # D->P feasible
    loop._proxy_signal = lambda ips: (2.0, 0.0, 0.0, 1)  # no measured usage yet
    loop._fetch = lambda ip, port=None: (  # noqa: E731
        "pd_proxy_prefill_inflight 0\n"
        "vllm:kv_cache_usage_perc 0.005\n"
    )
    loop.run_once()
    assert len(loop.rebalancer.proposals) == 1
    assert loop.rebalancer.proposals[0][1] == MODULE.Replicas(2, 1)


def test_cold_start_backlog_triggers_d_to_p_with_default_fallback() -> None:
    """Before any prefill response (requests_total == 0) the default fallback
    must still produce a nonzero backlog, so a cold-start queue is not
    invisible to the planner."""
    loop = make_loop(advisory=False, overrides={
        "decode_scale_up_kv_percent": 2.0,
        "decode_scale_down_kv_percent": 1.0,
        "min_observations": 1,
        "cooldown_seconds": 0.0,
    })
    loop.rebalancer._current = (1, 2)  # D->P feasible
    loop._proxy_signal = lambda ips: (5.0, 0.0, 0.0, 1)  # no measured usage yet
    loop._fetch = lambda ip, port=None: (  # noqa: E731
        "pd_proxy_prefill_inflight 0\n"
        "vllm:kv_cache_usage_perc 0.005\n"
    )
    loop.run_once()
    # default fallback 128 * 5 inflight = 640 >= scale_up 512 -> D->P
    assert len(loop.rebalancer.proposals) == 1
    assert loop.rebalancer.proposals[0][1] == MODULE.Replicas(2, 1)


def test_cold_start_inflight_backlog_blocks_premature_p_to_d() -> None:
    """With no prefill responses yet, in-flight requests must keep prefill
    non-relaxed instead of letting decode pressure drain the prefill pool."""
    loop = make_loop(advisory=False, overrides={
        "decode_scale_up_kv_percent": 2.0,
        "decode_scale_down_kv_percent": 1.0,
        "min_observations": 1,
        "cooldown_seconds": 0.0,
    })
    loop.rebalancer._current = (2, 1)  # P->D feasible
    loop._proxy_signal = lambda ips: (2.0, 0.0, 0.0, 1)  # no measured usage yet
    loop._fetch = lambda ip, port=None: (  # noqa: E731
        "pd_proxy_prefill_inflight 0\n"
        "vllm:kv_cache_usage_perc 0.9\n"  # decode pressured
    )
    loop.run_once()
    # default fallback 128 * 2 = 256 > scale_down 128 -> prefill not relaxed
    assert loop.rebalancer.proposals == []


def test_gauge_parsing_sums_label_series() -> None:
    text = (
        '# HELP vllm:num_requests_waiting Number of requests waiting to be processed.\n'
        'vllm:num_requests_waiting{engine="0",model_name="qwen3-8b"} 1.0\n'
        'vllm:num_requests_waiting{engine="1",model_name="qwen3-8b"} 2.0\n'
    )
    assert MODULE.PlannerLoop._gauge_values(text, "vllm:num_requests_waiting") == [1.0, 2.0]
    assert MODULE.PlannerLoop._gauge_values(text, "vllm:kv_cache_usage_perc") == []


def test_gauge_parsing_accepts_unlabeled_proxy_metrics() -> None:
    text = (
        '# HELP pd_proxy_prefill_inflight Requests currently waiting on the prefill engine.\n'
        '# TYPE pd_proxy_prefill_inflight gauge\n'
        'pd_proxy_prefill_inflight 43\n'
        '# HELP pd_proxy_prefill_requests_total Requests that completed the prefill phase successfully.\n'
        '# TYPE pd_proxy_prefill_requests_total counter\n'
        'pd_proxy_prefill_requests_total 715\n'
        'pd_proxy_prefill_prompt_tokens_total 850111\n'
        'pd_proxy_prefill_mean_prompt_tokens 1189.247\n'
    )
    assert MODULE.PlannerLoop._gauge_values(text, "pd_proxy_prefill_inflight") == [43.0]
    assert MODULE.PlannerLoop._gauge_values(text, "pd_proxy_prefill_requests_total") == [715.0]
    assert MODULE.PlannerLoop._gauge_values(text, "pd_proxy_prefill_prompt_tokens_total") == [850111.0]
    assert MODULE.PlannerLoop._gauge_values(text, "pd_proxy_prefill_mean_prompt_tokens") == [1189.247]


def test_fetch_many_collects_all_fast_pods() -> None:
    loop = make_loop(advisory=True)
    loop.scrape_deadline = 5.0
    text = "pd_proxy_prefill_inflight 1\n"
    loop._fetch = lambda ip, port=None: text  # type: ignore[method-assign]
    results = loop._fetch_many([("10.0.0.3", 8200), ("10.0.0.4", 8200)])
    assert set(results) == {("10.0.0.3", 8200), ("10.0.0.4", 8200)}
    assert all(value == text for value in results.values())


def test_fetch_many_treats_failed_pod_as_none_without_aborting_batch() -> None:
    loop = make_loop(advisory=True)
    loop.scrape_deadline = 5.0
    good = "pd_proxy_prefill_inflight 2\n"
    loop._fetch = lambda ip, port=None: None if ip == "10.0.0.3" else good  # type: ignore[method-assign]
    results = loop._fetch_many([("10.0.0.3", 8200), ("10.0.0.4", 8200)])
    assert results[("10.0.0.3", 8200)] is None
    assert results[("10.0.0.4", 8200)] == good


def test_fetch_many_bounds_batch_even_with_hung_pod() -> None:
    """A pod that hangs until its own request timeout must not delay the whole
    poll past the per-poll scrape deadline; it is skipped for that round."""
    loop = make_loop(advisory=True)
    loop.scrape_deadline = 0.05
    release = threading.Event()

    def hung(ip: str, port: int | None = None) -> str | None:
        release.wait(5)
        return "late"

    loop._fetch = hung  # type: ignore[method-assign]
    started = time_module.monotonic()
    results = loop._fetch_many([("10.0.0.3", 8200)])
    elapsed = time_module.monotonic() - started
    release.set()  # unblock the straggler so the interpreter can exit promptly
    assert elapsed < 1.0
    assert results == {}


def test_run_once_survives_partial_decode_failure() -> None:
    """One terminating decode pod is skipped; the remaining pod still yields a
    planner decision, so a single failing pod cannot abort the whole poll."""
    loop = make_loop(advisory=False)
    loop.rebalancer._current = (1, 2)  # P1,D2: D->P is budget-feasible
    loop._proxy_signal = lambda ips: (2.0, 2000.0, 2.0, 1)  # backlog 4000
    loop._endpoint_ips = lambda service: (  # noqa: E731
        ["10.0.0.1"] if "prefill" in service
        else ["10.0.0.2", "10.0.0.9"] if "decode" in service
        else ["10.0.0.3"]
    )

    def fetch(ip: str, port: int | None = None) -> str | None:
        return None if ip == "10.0.0.9" else "vllm:kv_cache_usage_perc 0.005\n"

    loop._fetch = fetch  # type: ignore[method-assign]
    loop.run_once()
    assert len(loop.rebalancer.proposals) == 1
    assert loop.rebalancer.proposals[0][1] == MODULE.Replicas(2, 1)
