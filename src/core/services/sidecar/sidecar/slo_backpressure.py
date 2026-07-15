# sidecar/slo_backpressure.py
# -*- coding: utf-8 -*-
"""SLO-driven dynamic pull backpressure for the sidecar (default OFF).

The sidecar pulls up to ``pull_cap = BATCH_SIZE + PREFETCH`` requests per pod
(see router_client.RouterPullWorker). When enabled, this module watches vLLM's
TPOT (time-per-output-token) and shrinks that cap when the TPOT SLO is violated,
then slowly restores it once TPOT is back inside SLO (AIMD-style control).

Design (three decoupled pieces so each is independently testable):

  1. ``next_pull_cap`` — a *pure* function: (observed_tpot, current_cap, now,
     last_adjust_ts, params) -> (new_cap, reason). No I/O, no clock, no threads.
  2. ``PullCapController`` — thin stateful wrapper that owns current_cap /
     last_adjust_ts and calls the pure function.
  3. ``VLLMTpotScraper`` + ``SloBackpressureMonitor`` — scrape vLLM /metrics,
     maintain a sliding TPOT window, and drive the controller on a timer.

When SLO_DYNAMIC_PULL_ENABLED is false, main.py never constructs a monitor and
router_client keeps computing the static cap, so the code path is unchanged and
carries zero extra cost.
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Callable, Deque, Optional, Tuple
from collections import deque

import requests

try:
    # prometheus_client is already a sidecar runtime dependency.
    from prometheus_client.parser import text_string_to_metric_families
except Exception:  # pragma: no cover - defensive, package is a hard dep
    text_string_to_metric_families = None  # type: ignore


# ------------------------------------------------------------------
# Controller parameters (decoupled from SidecarConfig for testability)
# ------------------------------------------------------------------

@dataclass(frozen=True)
class ControllerParams:
    slo_target_s: float
    min_pull: int
    max_pull: int
    decrease_mode: str        # "additive" | "multiplicative"
    decrease_step: int
    decrease_factor: float
    recover_step: int
    cooldown_s: float

    @staticmethod
    def from_config(cfg, default_max_pull: int) -> "ControllerParams":
        max_pull = int(cfg.SLO_MAX_PULL)
        if max_pull <= 0:
            max_pull = int(default_max_pull)
        min_pull = max(1, int(cfg.SLO_MIN_PULL))
        # Keep the window [min_pull, max_pull] well-formed.
        if max_pull < min_pull:
            max_pull = min_pull
        mode = str(cfg.SLO_DECREASE_MODE).lower()
        if mode not in ("additive", "multiplicative"):
            mode = "additive"
        return ControllerParams(
            slo_target_s=float(cfg.SLO_TPOT_SLO_S),
            min_pull=min_pull,
            max_pull=max_pull,
            decrease_mode=mode,
            decrease_step=max(1, int(cfg.SLO_DECREASE_STEP)),
            decrease_factor=float(cfg.SLO_DECREASE_FACTOR),
            recover_step=max(1, int(cfg.SLO_RECOVER_STEP)),
            cooldown_s=max(0.0, float(cfg.SLO_COOLDOWN_S)),
        )


# ------------------------------------------------------------------
# Pure controller
# ------------------------------------------------------------------

def next_pull_cap(
    observed_tpot: Optional[float],
    current_cap: int,
    now: float,
    last_adjust_ts: float,
    p: ControllerParams,
) -> Tuple[int, str]:
    """Decide the pull cap for this tick. Pure: no side effects.

    Priority order (first match wins):
      no_data -> cooldown -> violation(decrease) -> in-SLO(recover)

    Returns (new_cap, reason). new_cap is always clamped to [min_pull, max_pull].
    The caller advances last_adjust_ts only when new_cap != current_cap.
    """
    # Always keep the incoming cap inside the configured window first, so a
    # config change (e.g. lowered max_pull) is respected even while holding.
    current_cap = _clamp(current_cap, p.min_pull, p.max_pull)

    if observed_tpot is None:
        return current_cap, "no_data"

    if (now - last_adjust_ts) < p.cooldown_s:
        return current_cap, "hold_cooldown"

    if observed_tpot > p.slo_target_s:
        # Violation -> decrease (AIMD "MD" when multiplicative).
        if p.decrease_mode == "multiplicative":
            new = int(current_cap * p.decrease_factor)
        else:
            new = current_cap - p.decrease_step
        new = max(p.min_pull, new)
        reason = "decrease" if new != current_cap else "hold_at_min"
        return new, reason

    # Inside SLO -> recover (AIMD "AI": additive slow rise).
    new = min(p.max_pull, current_cap + p.recover_step)
    reason = "recover" if new != current_cap else "hold_at_max"
    return new, reason


def _clamp(v: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, v))


# ------------------------------------------------------------------
# Stateful controller wrapper
# ------------------------------------------------------------------

class PullCapController:
    """Owns the mutable cap state and delegates decisions to next_pull_cap."""

    def __init__(self, params: ControllerParams, initial_cap: int):
        self._p = params
        self._cap = _clamp(int(initial_cap), params.min_pull, params.max_pull)
        # -inf so the very first evaluation is never gated by cooldown,
        # regardless of the absolute value of the clock passed in.
        self._last_adjust_ts = float("-inf")
        self._lock = threading.Lock()

    @property
    def cap(self) -> int:
        with self._lock:
            return self._cap

    def update(self, observed_tpot: Optional[float], now: Optional[float] = None) -> Tuple[int, int, str]:
        """Advance one control step. Returns (old_cap, new_cap, reason)."""
        if now is None:
            now = time.monotonic()
        with self._lock:
            old = self._cap
            new, reason = next_pull_cap(observed_tpot, old, now, self._last_adjust_ts, self._p)
            if new != old:
                self._cap = new
                self._last_adjust_ts = now
            return old, new, reason


# ------------------------------------------------------------------
# vLLM TPOT scraper (interval average via delta(sum)/delta(count))
# ------------------------------------------------------------------

class VLLMTpotScraper:
    """Scrapes vLLM /metrics and returns the interval-average TPOT (seconds).

    vLLM exposes ``vllm:time_per_output_token_seconds`` as a Prometheus
    histogram. The average TPOT of tokens produced *since the last scrape* is
    ``delta(_sum) / delta(_count)``, which is robust to warm-up bias and to
    single-token outliers. Returns None on the first scrape, on scrape failure,
    or when no new tokens were generated in the interval.
    """

    def __init__(self, vllm_url: str, metric_name: str, timeout_s: float,
                 session: Optional[requests.Session] = None):
        self._url = vllm_url.rstrip("/") + "/metrics"
        self._metric = metric_name
        self._timeout = float(timeout_s)
        self._session = session or requests.Session()
        self._prev_sum: Optional[float] = None
        self._prev_count: Optional[float] = None

    def _fetch_text(self) -> str:
        r = self._session.get(self._url, timeout=self._timeout)
        r.raise_for_status()
        return r.text

    def _parse_sum_count(self, text: str) -> Optional[Tuple[float, float]]:
        if text_string_to_metric_families is None:
            return None
        total_sum = 0.0
        total_count = 0.0
        seen = False
        for fam in text_string_to_metric_families(text):
            if fam.name != self._metric:
                continue
            for s in fam.samples:
                # Aggregate across all label sets (e.g. per model_name).
                if s.name.endswith("_sum"):
                    total_sum += float(s.value)
                    seen = True
                elif s.name.endswith("_count"):
                    total_count += float(s.value)
                    seen = True
        if not seen:
            return None
        return total_sum, total_count

    def sample(self) -> Optional[float]:
        """Return interval-average TPOT (s), or None if unavailable this tick."""
        try:
            text = self._fetch_text()
        except Exception:
            return None
        parsed = self._parse_sum_count(text)
        if parsed is None:
            return None
        cur_sum, cur_count = parsed

        prev_sum, prev_count = self._prev_sum, self._prev_count
        self._prev_sum, self._prev_count = cur_sum, cur_count

        if prev_sum is None or prev_count is None:
            return None  # need a baseline

        d_sum = cur_sum - prev_sum
        d_count = cur_count - prev_count
        if d_count <= 0 or d_sum < 0:
            # No new tokens (idle) or a counter reset (vLLM restart).
            return None
        return d_sum / d_count


# ------------------------------------------------------------------
# Sliding window
# ------------------------------------------------------------------

class TpotWindow:
    """Fixed-size sliding window over interval-average TPOT samples."""

    def __init__(self, max_samples: int, agg: str = "mean"):
        self._buf: Deque[float] = deque(maxlen=max(1, int(max_samples)))
        self._agg = agg if agg in ("mean", "p90") else "mean"

    def add(self, value: Optional[float]) -> None:
        if value is not None:
            self._buf.append(float(value))

    def value(self) -> Optional[float]:
        if not self._buf:
            return None
        vals = list(self._buf)
        if self._agg == "p90":
            return _percentile(vals, 0.90)
        return sum(vals) / len(vals)

    def __len__(self) -> int:
        return len(self._buf)


def _percentile(values, q: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    if len(s) == 1:
        return s[0]
    idx = q * (len(s) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(s) - 1)
    frac = idx - lo
    return s[lo] + (s[hi] - s[lo]) * frac


# ------------------------------------------------------------------
# Background monitor
# ------------------------------------------------------------------

class SloBackpressureMonitor:
    """Background thread: scrape TPOT, update the window, drive the controller.

    The current cap is published via ``get_cap()`` and consumed by the pull path
    (RouterPullWorker). Also emits observability metrics and logs each change.
    """

    def __init__(
        self,
        cfg,
        default_cap: int,
        endpoint_id: str = "",
        scraper: Optional[VLLMTpotScraper] = None,
        metrics_hook: Optional[Callable[..., None]] = None,
        log: Optional[Callable[[str], None]] = None,
    ):
        self._cfg = cfg
        self._endpoint_id = endpoint_id
        self._params = ControllerParams.from_config(cfg, default_max_pull=default_cap)
        # Start optimistic at max_pull so the enabled path matches the static
        # cap until the first violation is observed.
        self._controller = PullCapController(self._params, initial_cap=self._params.max_pull)
        self._window = TpotWindow(cfg.SLO_WINDOW_SAMPLES, cfg.SLO_WINDOW_AGG)
        self._scraper = scraper or VLLMTpotScraper(
            vllm_url=cfg.VLLM_URL,
            metric_name=cfg.SLO_TPOT_METRIC,
            timeout_s=cfg.SLO_SCRAPE_TIMEOUT_S,
        )
        self._metrics_hook = metrics_hook
        self._log = log or (lambda m: print(m))
        self._stop_evt = threading.Event()
        self._thread: Optional[threading.Thread] = None

        # Register the SLO gauges lazily, exactly when the feature is in use.
        # Keeps the disabled path's /metrics output byte-for-byte unchanged.
        if metrics_hook is not None:
            try:
                from .metrics import init_slo_metrics
                init_slo_metrics()
            except Exception:
                pass

    # ---------------- public API ----------------

    def get_cap(self) -> int:
        return self._controller.cap

    @property
    def params(self) -> ControllerParams:
        return self._params

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop_evt.clear()
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="slo-backpressure",
        )
        self._thread.start()
        self._log(
            "[sidecar][slo] dynamic pull backpressure ENABLED "
            f"(slo_tpot={self._params.slo_target_s}s, "
            f"min_pull={self._params.min_pull}, max_pull={self._params.max_pull}, "
            f"eval_interval={self._cfg.SLO_EVAL_INTERVAL_S}s, "
            f"window={self._cfg.SLO_WINDOW_SAMPLES}x{self._cfg.SLO_WINDOW_AGG}, "
            f"decrease={self._params.decrease_mode}, cooldown={self._params.cooldown_s}s)"
        )

    def stop(self) -> None:
        self._stop_evt.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    # ---------------- internals ----------------

    def tick(self, now: Optional[float] = None) -> Tuple[Optional[float], int, int, str]:
        """One evaluation step (also used by tests). Returns
        (windowed_tpot, old_cap, new_cap, reason)."""
        sample = self._scraper.sample()
        self._window.add(sample)
        windowed = self._window.value()
        old, new, reason = self._controller.update(windowed, now=now)
        self._emit(windowed, old, new, reason)
        return windowed, old, new, reason

    def _emit(self, windowed: Optional[float], old: int, new: int, reason: str) -> None:
        if self._metrics_hook is not None:
            try:
                self._metrics_hook(
                    endpoint=self._endpoint_id,
                    cap=new,
                    observed_tpot=windowed,
                    slo_target=self._params.slo_target_s,
                )
            except Exception:
                pass
        if new != old:
            wt = f"{windowed:.4f}s" if windowed is not None else "n/a"
            self._log(
                f"[sidecar][slo] adjust endpoint={self._endpoint_id} "
                f"tpot={wt} slo={self._params.slo_target_s}s "
                f"old_cap={old} new_cap={new} reason={reason}"
            )

    def _loop(self) -> None:
        interval = max(0.1, float(self._cfg.SLO_EVAL_INTERVAL_S))
        while not self._stop_evt.is_set():
            try:
                self.tick()
            except Exception as e:  # pragma: no cover - defensive
                self._log(f"[sidecar][slo] monitor error: {e}")
            self._stop_evt.wait(timeout=interval)
