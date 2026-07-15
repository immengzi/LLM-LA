# tests/test_slo_backpressure.py
# -*- coding: utf-8 -*-
"""Unit tests for SLO-driven dynamic pull backpressure.

Covers:
  - pure controller: violation decrease (additive + multiplicative), recovery,
    min/max clamping, cooldown hysteresis, no-data hold.
  - config parsing of SLO_* knobs.
  - TPOT sliding window (mean / p90).
  - VLLMTpotScraper delta(sum)/delta(count) logic (with a fake session).
  - end-to-end convergence: a violating TPOT series drives the cap down and a
    recovered series drives it back up to max.
  - closed-state wiring: RouterPullWorker without a cap_provider uses the static
    BATCH_SIZE + PREFETCH cap (zero-change path).
"""
import pytest

from sidecar.config import get_config
from sidecar.slo_backpressure import (
    ControllerParams,
    PullCapController,
    SloBackpressureMonitor,
    TpotWindow,
    VLLMTpotScraper,
    next_pull_cap,
)


def _params(**over) -> ControllerParams:
    base = dict(
        slo_target_s=0.040,
        min_pull=1,
        max_pull=8,
        decrease_mode="additive",
        decrease_step=1,
        decrease_factor=0.5,
        recover_step=1,
        cooldown_s=10.0,
    )
    base.update(over)
    return ControllerParams(**base)


# ------------------------------------------------------------------
# Pure controller
# ------------------------------------------------------------------

def test_violation_additive_decrease():
    p = _params()
    new, reason = next_pull_cap(0.060, current_cap=8, now=100.0, last_adjust_ts=0.0, p=p)
    assert new == 7
    assert reason == "decrease"


def test_violation_multiplicative_decrease():
    p = _params(decrease_mode="multiplicative", decrease_factor=0.5)
    new, reason = next_pull_cap(0.060, current_cap=8, now=100.0, last_adjust_ts=0.0, p=p)
    assert new == 4
    assert reason == "decrease"


def test_recover_additive_increase():
    p = _params()
    new, reason = next_pull_cap(0.030, current_cap=4, now=100.0, last_adjust_ts=0.0, p=p)
    assert new == 5
    assert reason == "recover"


def test_decrease_clamped_at_min_pull():
    p = _params(min_pull=1)
    new, reason = next_pull_cap(0.060, current_cap=1, now=100.0, last_adjust_ts=0.0, p=p)
    assert new == 1
    assert reason == "hold_at_min"


def test_multiplicative_decrease_respects_min():
    p = _params(decrease_mode="multiplicative", decrease_factor=0.5, min_pull=3)
    # floor(4 * 0.5) = 2, but min_pull = 3 -> clamp up to 3.
    new, reason = next_pull_cap(0.060, current_cap=4, now=100.0, last_adjust_ts=0.0, p=p)
    assert new == 3
    assert reason == "decrease"


def test_recover_clamped_at_max_pull():
    p = _params(max_pull=8)
    new, reason = next_pull_cap(0.030, current_cap=8, now=100.0, last_adjust_ts=0.0, p=p)
    assert new == 8
    assert reason == "hold_at_max"


def test_at_slo_boundary_is_not_violation():
    p = _params(slo_target_s=0.040)
    # observed == target is NOT > target, so treated as in-SLO -> recover.
    new, reason = next_pull_cap(0.040, current_cap=4, now=100.0, last_adjust_ts=0.0, p=p)
    assert new == 5
    assert reason == "recover"


def test_cooldown_holds_cap():
    p = _params(cooldown_s=10.0)
    # 5s since last adjust (< cooldown) -> hold regardless of violation.
    new, reason = next_pull_cap(0.060, current_cap=6, now=105.0, last_adjust_ts=100.0, p=p)
    assert new == 6
    assert reason == "hold_cooldown"


def test_no_data_holds_cap():
    p = _params()
    new, reason = next_pull_cap(None, current_cap=6, now=1000.0, last_adjust_ts=0.0, p=p)
    assert new == 6
    assert reason == "no_data"


def test_incoming_cap_reclamped_to_window():
    p = _params(min_pull=2, max_pull=6)
    # A stale/oversized cap gets clamped to max even while holding on no_data.
    new, reason = next_pull_cap(None, current_cap=99, now=1.0, last_adjust_ts=0.0, p=p)
    assert new == 6
    assert reason == "no_data"


# ------------------------------------------------------------------
# Stateful controller: cooldown + last_adjust bookkeeping
# ------------------------------------------------------------------

def test_controller_advances_last_adjust_only_on_change():
    p = _params(cooldown_s=10.0)
    c = PullCapController(p, initial_cap=8)

    old, new, reason = c.update(0.060, now=0.0)
    assert (old, new, reason) == (8, 7, "decrease")

    # Within cooldown -> no change, last_adjust stays at t=0.
    old, new, reason = c.update(0.060, now=5.0)
    assert (old, new, reason) == (7, 7, "hold_cooldown")

    # After cooldown -> can decrease again.
    old, new, reason = c.update(0.060, now=11.0)
    assert (old, new, reason) == (7, 6, "decrease")


def test_controller_hold_at_min_does_not_reset_cooldown():
    p = _params(cooldown_s=10.0, min_pull=5)
    c = PullCapController(p, initial_cap=5)
    # Already at min; violation cannot decrease. last_adjust must not advance,
    # otherwise recovery would be delayed forever.
    old, new, reason = c.update(0.060, now=100.0)
    assert (old, new, reason) == (5, 5, "hold_at_min")
    # Immediately able to recover once TPOT is healthy (no spurious cooldown).
    old, new, reason = c.update(0.030, now=100.1)
    assert (old, new, reason) == (5, 6, "recover")


# ------------------------------------------------------------------
# Sliding window
# ------------------------------------------------------------------

def test_window_mean():
    w = TpotWindow(max_samples=3, agg="mean")
    assert w.value() is None
    w.add(0.02)
    w.add(0.04)
    assert w.value() == pytest.approx(0.03)
    w.add(0.06)
    w.add(0.12)  # evicts 0.02 -> {0.04, 0.06, 0.12}
    assert w.value() == pytest.approx((0.04 + 0.06 + 0.12) / 3)


def test_window_ignores_none():
    w = TpotWindow(max_samples=3)
    w.add(None)
    assert w.value() is None
    w.add(0.05)
    assert w.value() == pytest.approx(0.05)


def test_window_p90():
    w = TpotWindow(max_samples=10, agg="p90")
    for v in [0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.10]:
        w.add(v)
    # p90 index = 0.9 * 9 = 8.1 -> between 0.09 and 0.10.
    assert w.value() == pytest.approx(0.09 + 0.1 * (0.10 - 0.09))


# ------------------------------------------------------------------
# Scraper: delta(sum)/delta(count)
# ------------------------------------------------------------------

class _FakeResp:
    def __init__(self, text):
        self.text = text

    def raise_for_status(self):
        pass


class _FakeSession:
    def __init__(self, pages):
        self._pages = list(pages)
        self._i = 0

    def get(self, url, timeout=None):
        page = self._pages[min(self._i, len(self._pages) - 1)]
        self._i += 1
        if isinstance(page, Exception):
            raise page
        return _FakeResp(page)


def _metrics_text(total_sum, total_count):
    m = "vllm:time_per_output_token_seconds"
    return (
        f"# HELP {m} TPOT\n"
        f"# TYPE {m} histogram\n"
        f'{m}_bucket{{le="0.01"}} 0\n'
        f'{m}_bucket{{le="+Inf"}} {total_count}\n'
        f"{m}_sum {total_sum}\n"
        f"{m}_count {total_count}\n"
    )


def test_scraper_needs_baseline_then_returns_interval_avg():
    pages = [
        _metrics_text(1.0, 100),   # baseline
        _metrics_text(1.6, 110),   # +0.6s over +10 tokens -> 0.06
    ]
    sc = VLLMTpotScraper("http://x", "vllm:time_per_output_token_seconds", 1.0,
                         session=_FakeSession(pages))
    assert sc.sample() is None            # baseline tick
    assert sc.sample() == pytest.approx(0.06)


def test_scraper_returns_none_when_no_new_tokens():
    pages = [_metrics_text(1.0, 100), _metrics_text(1.0, 100)]
    sc = VLLMTpotScraper("http://x", "vllm:time_per_output_token_seconds", 1.0,
                         session=_FakeSession(pages))
    assert sc.sample() is None
    assert sc.sample() is None            # delta_count == 0


def test_scraper_handles_counter_reset():
    pages = [_metrics_text(5.0, 500), _metrics_text(0.2, 10)]  # vLLM restarted
    sc = VLLMTpotScraper("http://x", "vllm:time_per_output_token_seconds", 1.0,
                         session=_FakeSession(pages))
    assert sc.sample() is None
    assert sc.sample() is None            # negative delta -> None


def test_scraper_handles_scrape_error():
    pages = [RuntimeError("boom"), _metrics_text(1.0, 100)]
    sc = VLLMTpotScraper("http://x", "vllm:time_per_output_token_seconds", 1.0,
                         session=_FakeSession(pages))
    assert sc.sample() is None            # error tick
    assert sc.sample() is None            # first good tick is only a baseline


# ------------------------------------------------------------------
# End-to-end convergence via injected TPOT series
# ------------------------------------------------------------------

class _ScriptedScraper:
    """Returns a predetermined TPOT sequence (already interval-averaged)."""

    def __init__(self, series):
        self._series = list(series)
        self._i = 0

    def sample(self):
        if self._i >= len(self._series):
            v = self._series[-1]
        else:
            v = self._series[self._i]
        self._i += 1
        return v


def _monitor_with_series(series, **cfg_over):
    cfg = get_config()
    cfg.SLO_DYNAMIC_PULL_ENABLED = True
    cfg.SLO_TPOT_SLO_S = 0.040
    cfg.SLO_WINDOW_SAMPLES = 1        # no smoothing -> deterministic steps
    cfg.SLO_WINDOW_AGG = "mean"
    cfg.SLO_MIN_PULL = 1
    cfg.SLO_MAX_PULL = 8
    cfg.SLO_DECREASE_MODE = "additive"
    cfg.SLO_DECREASE_STEP = 1
    cfg.SLO_RECOVER_STEP = 1
    cfg.SLO_COOLDOWN_S = 0.0          # no cooldown -> react every tick
    for k, v in cfg_over.items():
        setattr(cfg, k, v)
    return SloBackpressureMonitor(
        cfg,
        default_cap=8,
        endpoint_id="pod-a",
        scraper=_ScriptedScraper(series),
    )


def test_convergence_violation_drives_cap_down_then_recovers():
    # 4 violating samples, then healthy samples.
    series = [0.080, 0.080, 0.080, 0.080] + [0.020] * 12
    mon = _monitor_with_series(series)
    caps = []
    t = 0.0
    for _ in range(len(series)):
        _, _, new, _ = mon.tick(now=t)
        caps.append(new)
        t += 1.0
    # Starts at max (8), decreases by 1 per violating tick.
    assert caps[0] == 7
    assert caps[3] == 4
    # Then recovers by 1 per healthy tick, back up to max (8) and holds.
    assert caps[-1] == 8
    assert max(caps) == 8
    assert min(caps) == 4


def test_convergence_multiplicative_fast_backoff():
    series = [0.090, 0.090]
    mon = _monitor_with_series(
        series,
        SLO_DECREASE_MODE="multiplicative",
        SLO_DECREASE_FACTOR=0.5,
    )
    _, _, c1, _ = mon.tick(now=0.0)
    _, _, c2, _ = mon.tick(now=1.0)
    assert c1 == 4      # 8 * 0.5
    assert c2 == 2      # 4 * 0.5


def test_convergence_never_below_min_pull():
    series = [0.090] * 50
    mon = _monitor_with_series(series, SLO_MIN_PULL=2)
    last = 8
    for i in range(50):
        _, _, last, _ = mon.tick(now=float(i))
    assert last == 2


def test_cooldown_prevents_rapid_adjust_in_monitor():
    series = [0.090] * 10
    mon = _monitor_with_series(series, SLO_COOLDOWN_S=10.0)
    # t=0 decrease to 7; t=1..9 within cooldown -> hold; t=10 decrease to 6.
    _, _, c_t0, r0 = mon.tick(now=0.0)
    assert (c_t0, r0) == (7, "decrease")
    for t in range(1, 10):
        _, _, cap, reason = mon.tick(now=float(t))
        assert cap == 7
        assert reason == "hold_cooldown"
    _, _, c_t10, r10 = mon.tick(now=10.0)
    assert (c_t10, r10) == (6, "decrease")


def test_monitor_get_cap_starts_at_max():
    mon = _monitor_with_series([0.02])
    assert mon.get_cap() == 8


# ------------------------------------------------------------------
# Config parsing
# ------------------------------------------------------------------

def test_slo_config_defaults_disabled():
    cfg = get_config()
    assert cfg.SLO_DYNAMIC_PULL_ENABLED is False
    assert cfg.SLO_MIN_PULL == 1
    assert cfg.SLO_MAX_PULL == 0          # 0 => use BATCH_SIZE + PREFETCH
    assert cfg.SLO_DECREASE_MODE == "additive"


def test_slo_config_env_overrides(monkeypatch):
    monkeypatch.setenv("SLO_DYNAMIC_PULL_ENABLED", "true")
    monkeypatch.setenv("SLO_TPOT_SLO_S", "0.03")
    monkeypatch.setenv("SLO_EVAL_INTERVAL_S", "2.5")
    monkeypatch.setenv("SLO_WINDOW_SAMPLES", "10")
    monkeypatch.setenv("SLO_MIN_PULL", "2")
    monkeypatch.setenv("SLO_MAX_PULL", "16")
    monkeypatch.setenv("SLO_DECREASE_MODE", "MULTIPLICATIVE")
    monkeypatch.setenv("SLO_DECREASE_FACTOR", "0.25")
    monkeypatch.setenv("SLO_RECOVER_STEP", "2")
    monkeypatch.setenv("SLO_COOLDOWN_S", "7.5")
    cfg = get_config()
    assert cfg.SLO_DYNAMIC_PULL_ENABLED is True
    assert cfg.SLO_TPOT_SLO_S == 0.03
    assert cfg.SLO_EVAL_INTERVAL_S == 2.5
    assert cfg.SLO_WINDOW_SAMPLES == 10
    assert cfg.SLO_MIN_PULL == 2
    assert cfg.SLO_MAX_PULL == 16
    assert cfg.SLO_DECREASE_MODE == "multiplicative"
    assert cfg.SLO_DECREASE_FACTOR == 0.25
    assert cfg.SLO_RECOVER_STEP == 2
    assert cfg.SLO_COOLDOWN_S == 7.5


def test_params_from_config_default_max_pull_uses_static_cap():
    cfg = get_config()
    cfg.SLO_MAX_PULL = 0
    p = ControllerParams.from_config(cfg, default_max_pull=12)
    assert p.max_pull == 12


def test_params_from_config_fixes_inverted_window():
    cfg = get_config()
    cfg.SLO_MIN_PULL = 10
    cfg.SLO_MAX_PULL = 4
    p = ControllerParams.from_config(cfg, default_max_pull=4)
    assert p.min_pull == 10
    assert p.max_pull == 10          # widened up so min <= max holds


# ------------------------------------------------------------------
# Closed-state wiring: no cap_provider -> static cap
# ------------------------------------------------------------------

def test_router_pull_worker_static_cap_when_disabled(monkeypatch):
    monkeypatch.setenv("BATCH_SIZE", "8")
    monkeypatch.setenv("PREFETCH", "2")
    # router_client caches _cfg at import; patch its module-level cfg.
    import sidecar.router_client as rc
    from sidecar.config import get_config as _gc
    monkeypatch.setattr(rc, "_cfg", _gc())
    from sidecar.local_queue import LocalQueue

    w = rc.RouterPullWorker(LocalQueue("pod-a"), "pod-a")  # no cap_provider
    assert w._current_pull_cap() == 10


def test_router_pull_worker_uses_cap_provider_when_present():
    import sidecar.router_client as rc
    from sidecar.local_queue import LocalQueue

    dynamic = {"cap": 3}
    w = rc.RouterPullWorker(
        LocalQueue("pod-a"), "pod-a", cap_provider=lambda: dynamic["cap"],
    )
    assert w._current_pull_cap() == 3
    dynamic["cap"] = 5
    assert w._current_pull_cap() == 5


# ------------------------------------------------------------------
# Closed-state /metrics cleanliness: lazily-registered SLO gauges
# ------------------------------------------------------------------

def test_disabled_state_emits_no_slo_metric_headers():
    """In a fresh process that never constructs a monitor, /metrics output must
    not contain the SLO gauges — not even HELP/TYPE headers."""
    import os
    import subprocess
    import sys

    service_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    code = (
        "from prometheus_client import generate_latest;"
        "import sidecar.metrics as m;"
        "out = generate_latest().decode();"
        "assert 'sidecar_slo_dynamic_pull_cap' not in out, out;"
        "assert 'sidecar_slo_observed_tpot_seconds' not in out, out;"
        "assert 'sidecar_slo_violation' not in out, out;"
        # set_* before init must be a safe no-op that registers nothing.
        "m.set_slo_backpressure_state('pod', 3, 0.05, 0.04);"
        "out2 = generate_latest().decode();"
        "assert 'sidecar_slo_dynamic_pull_cap' not in out2, out2;"
        "print('OK')"
    )
    env = dict(os.environ)
    env["PYTHONPATH"] = service_dir + os.pathsep + env.get("PYTHONPATH", "")
    r = subprocess.run(
        [sys.executable, "-c", code],
        cwd=service_dir, env=env, capture_output=True, text=True,
    )
    assert r.returncode == 0, f"stdout={r.stdout!r} stderr={r.stderr!r}"
    assert "OK" in r.stdout


def test_init_slo_metrics_registers_and_is_idempotent():
    import sidecar.metrics as m

    m.init_slo_metrics()
    m.init_slo_metrics()  # second call must not raise (no duplicate registration)
    assert m._SLO_METRICS_INITED is True
    assert m.SIDECAR_SLO_DYNAMIC_PULL_CAP is not None

    from prometheus_client import generate_latest
    m.set_slo_backpressure_state("pod-x", 4, observed_tpot=0.06, slo_target=0.04)
    out = generate_latest().decode()
    assert "sidecar_slo_dynamic_pull_cap" in out
    assert 'sidecar_slo_violation{endpoint="pod-x"} 1.0' in out


def test_monitor_construction_registers_metrics_when_hook_present():
    import sidecar.metrics as m
    from sidecar.slo_backpressure import SloBackpressureMonitor

    cfg = get_config()
    cfg.SLO_MAX_PULL = 8
    SloBackpressureMonitor(
        cfg, default_cap=8, endpoint_id="pod-y",
        scraper=_ScriptedScraper([0.02]),
        metrics_hook=m.set_slo_backpressure_state,
    )
    assert m._SLO_METRICS_INITED is True


def test_router_pull_worker_falls_back_on_provider_error(monkeypatch):
    monkeypatch.setenv("BATCH_SIZE", "8")
    monkeypatch.setenv("PREFETCH", "0")
    import sidecar.router_client as rc
    from sidecar.config import get_config as _gc
    monkeypatch.setattr(rc, "_cfg", _gc())
    from sidecar.local_queue import LocalQueue

    def _boom():
        raise RuntimeError("provider down")

    w = rc.RouterPullWorker(LocalQueue("pod-a"), "pod-a", cap_provider=_boom)
    assert w._current_pull_cap() == 8    # static fallback
