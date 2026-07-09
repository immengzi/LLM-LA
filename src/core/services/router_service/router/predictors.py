# router/predictors.py
# -*- coding: utf-8 -*-
"""
Output-length predictors behind a common interface.

Predictor lifecycle:
  - predict() is called at enqueue time for every request.
  - update() is called from /result with actual output token count.

Implementations:
  - SimpleLengthPredictor: char-len heuristic (legacy default).
  - HintOnlyPredictor: returns output_len_hint or falls back to simple.
  - TaskTypeDistributionPredictor: per-task-type running statistics.
  - InputLengthRegressionPredictor: per-type linear fit from completions.
"""

from __future__ import annotations

import math
from collections import deque
from threading import RLock
from typing import Deque, Dict, Optional, Tuple


# -------------------------------------------------------
# Abstract interface
# -------------------------------------------------------

class OutputLengthPredictor:
    """Base interface for all output length predictors."""
    name: str = "base"

    def predict(
        self,
        prompt: str,
        input_tokens: int,
        task_type: str = "",
        req_id: str = "",
    ) -> int:
        raise NotImplementedError

    def update(self, req_id: str, actual_output_tokens: int, task_type: str = "", input_tokens: int = 0) -> None:
        pass


# -------------------------------------------------------
# SimpleLengthPredictor (legacy)
# -------------------------------------------------------

class SimpleLengthPredictor(OutputLengthPredictor):
    name = "char-len"

    def predict(self, prompt: str, input_tokens: int = 0, task_type: str = "", req_id: str = "") -> int:
        return max(1, len(prompt) // 2)

    def predict_out_tokens(self, prompt: str, req_id: str = "") -> Optional[int]:
        """Legacy method for backward compatibility with len_select.py."""
        return self.predict(prompt, 0)


# -------------------------------------------------------
# HintOnlyPredictor
# -------------------------------------------------------

class HintOnlyPredictor(OutputLengthPredictor):
    """Returns client hint if present, else falls back to char-len."""
    name = "hint_only"

    def __init__(self, default_tokens: int = 256):
        self._default = max(1, default_tokens)

    def predict(self, prompt: str, input_tokens: int = 0, task_type: str = "", req_id: str = "") -> int:
        return self._default

    def predict_with_hint(self, hint: Optional[int], prompt: str) -> int:
        if hint is not None and hint > 0:
            return hint
        return max(1, len(prompt) // 2)


# -------------------------------------------------------
# TaskTypeDistributionPredictor
# -------------------------------------------------------

class _RunningStats:
    """Maintains running mean/variance over a capped window of observations."""

    def __init__(self, max_window: int = 500):
        self._max_window = max(10, max_window)
        self._samples: Deque[int] = deque(maxlen=self._max_window)
        self._sum: float = 0.0
        self._sum_sq: float = 0.0

    def add(self, val: int) -> None:
        if len(self._samples) == self._max_window:
            old = self._samples[0]
            self._sum -= old
            self._sum_sq -= old * old
        self._samples.append(val)
        self._sum += val
        self._sum_sq += val * val

    @property
    def count(self) -> int:
        return len(self._samples)

    @property
    def mean(self) -> float:
        if not self._samples:
            return 0.0
        return self._sum / len(self._samples)

    @property
    def median(self) -> float:
        if not self._samples:
            return 0.0
        s = sorted(self._samples)
        n = len(s)
        if n % 2 == 1:
            return float(s[n // 2])
        return (s[n // 2 - 1] + s[n // 2]) / 2.0

    @property
    def variance(self) -> float:
        n = len(self._samples)
        if n < 2:
            return 0.0
        mean = self._sum / n
        return max(0.0, self._sum_sq / n - mean * mean)

    @property
    def stddev(self) -> float:
        return math.sqrt(self.variance)


class TaskTypeDistributionPredictor(OutputLengthPredictor):
    """
    Per-task-type running statistics predictor.

    Uses median of observed output lengths per task_type.
    Falls back to global distribution when per-type samples are sparse.
    """
    name = "distribution"

    def __init__(self, min_samples: int = 5, max_window: int = 500, default_tokens: int = 256):
        self._min_samples = max(1, min_samples)
        self._max_window = max_window
        self._default = max(1, default_tokens)
        self._lock = RLock()
        self._per_type: Dict[str, _RunningStats] = {}
        self._global = _RunningStats(max_window=max_window)

    def predict(self, prompt: str, input_tokens: int = 0, task_type: str = "", req_id: str = "") -> int:
        with self._lock:
            if task_type:
                stats = self._per_type.get(task_type)
                if stats and stats.count >= self._min_samples:
                    return max(1, int(stats.median))

            if self._global.count >= self._min_samples:
                return max(1, int(self._global.median))

        return self._default

    def predict_out_tokens(self, prompt: str, req_id: str = "") -> Optional[int]:
        """Legacy compatibility."""
        return self.predict(prompt, 0)

    def update(self, req_id: str, actual_output_tokens: int, task_type: str = "", input_tokens: int = 0) -> None:
        if actual_output_tokens <= 0:
            return
        with self._lock:
            self._global.add(actual_output_tokens)
            if task_type:
                if task_type not in self._per_type:
                    self._per_type[task_type] = _RunningStats(max_window=self._max_window)
                self._per_type[task_type].add(actual_output_tokens)


# -------------------------------------------------------
# InputLengthRegressionPredictor (Step 10 upgrade)
# -------------------------------------------------------

class InputLengthRegressionPredictor(OutputLengthPredictor):
    """
    Per-type linear regression: output_tokens = a * input_tokens + b.

    Uses online least-squares update. Falls back to distribution predictor
    when insufficient samples.
    """
    name = "regression"

    def __init__(self, min_samples: int = 20, max_window: int = 500, default_tokens: int = 256):
        self._min_samples = max(2, min_samples)
        self._default = max(1, default_tokens)
        self._lock = RLock()
        # Per-type: list of (input_tokens, output_tokens)
        self._per_type: Dict[str, Deque[Tuple[int, int]]] = {}
        self._global: Deque[Tuple[int, int]] = deque(maxlen=max_window)
        self._max_window = max_window

    def _fit(self, samples: Deque[Tuple[int, int]]) -> Optional[Tuple[float, float]]:
        """Simple OLS fit: y = a*x + b."""
        n = len(samples)
        if n < self._min_samples:
            return None
        sx = sy = sxx = sxy = 0.0
        for x, y in samples:
            sx += x
            sy += y
            sxx += x * x
            sxy += x * y
        denom = n * sxx - sx * sx
        if abs(denom) < 1e-12:
            return None
        a = (n * sxy - sx * sy) / denom
        b = (sy - a * sx) / n
        return (a, b)

    def predict(self, prompt: str, input_tokens: int = 0, task_type: str = "", req_id: str = "") -> int:
        with self._lock:
            if task_type:
                samples = self._per_type.get(task_type)
                if samples:
                    fit = self._fit(samples)
                    if fit:
                        a, b = fit
                        return max(1, int(a * input_tokens + b))

            fit = self._fit(self._global)
            if fit:
                a, b = fit
                return max(1, int(a * input_tokens + b))

        return self._default

    def predict_out_tokens(self, prompt: str, req_id: str = "") -> Optional[int]:
        return self.predict(prompt, max(1, len(prompt) // 4))

    def update(self, req_id: str, actual_output_tokens: int, task_type: str = "", input_tokens: int = 0) -> None:
        if actual_output_tokens <= 0 or input_tokens <= 0:
            return
        with self._lock:
            self._global.append((input_tokens, actual_output_tokens))
            if task_type:
                if task_type not in self._per_type:
                    self._per_type[task_type] = deque(maxlen=self._max_window)
                self._per_type[task_type].append((input_tokens, actual_output_tokens))


# -------------------------------------------------------
# Factory
# -------------------------------------------------------

_predictor_instance: Optional[OutputLengthPredictor] = None
_predictor_lock = RLock()


def get_output_length_predictor() -> OutputLengthPredictor:
    """Return the singleton predictor instance based on config."""
    global _predictor_instance
    with _predictor_lock:
        if _predictor_instance is not None:
            return _predictor_instance

        from .config import get_config
        cfg = get_config()
        kind = str(getattr(cfg, "OUTPUT_LEN_PREDICTOR", "simple")).lower()

        if kind == "distribution":
            _predictor_instance = TaskTypeDistributionPredictor(
                default_tokens=cfg.DEFAULT_MAX_TOKENS,
            )
        elif kind == "regression":
            _predictor_instance = InputLengthRegressionPredictor(
                default_tokens=cfg.DEFAULT_MAX_TOKENS,
            )
        elif kind == "hint_only":
            _predictor_instance = HintOnlyPredictor(
                default_tokens=cfg.DEFAULT_MAX_TOKENS,
            )
        else:
            _predictor_instance = SimpleLengthPredictor()

        return _predictor_instance


def get_length_predictor() -> SimpleLengthPredictor:
    """Legacy accessor — returns something compatible with len_select.py."""
    pred = get_output_length_predictor()
    if isinstance(pred, SimpleLengthPredictor):
        return pred
    # Wrap non-Simple predictors so len_select.py's predict_out_tokens() works
    if hasattr(pred, "predict_out_tokens"):
        return pred  # type: ignore
    return SimpleLengthPredictor()
