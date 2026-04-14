# router/latency_predictor.py
# -*- coding: utf-8 -*-
"""
Latency predictor interface + implementations.

All predictors expose:
  predict_ttft(input_tokens, cached_tokens, batch_size) -> seconds
  predict_tpot(batch_size, accumulated_len)              -> seconds
  predict_e2e(input_tokens, cached_tokens, output_tokens, batch_size) -> seconds
  update(observation)                                     -> None

Implementations:
  - LinearLatencyPredictor: offline-profiled analytical model (section A.6 of spec).
  - PiecewiseLinearPredictor: separate coefficients per concurrency range.
  - BayesianLatencyPredictor: online RLS from live observations.
  - HybridLatencyPredictor: offline prior + online update.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Optional


@dataclass
class LatencyObservation:
    """Ground-truth observation from a completed request."""
    input_tokens: int = 0
    cached_tokens: int = 0
    output_tokens: int = 0
    batch_size: int = 1
    actual_ttft_s: float = 0.0
    actual_tpot_s: float = 0.0
    actual_e2e_s: float = 0.0


# -------------------------------------------------------
# Abstract interface
# -------------------------------------------------------

class LatencyPredictor:
    """Base interface for latency predictors."""
    name: str = "base"

    def predict_ttft(self, input_tokens: int, cached_tokens: int, batch_size: int) -> float:
        raise NotImplementedError

    def predict_tpot(self, batch_size: int, accumulated_len: int) -> float:
        raise NotImplementedError

    def predict_e2e(
        self,
        input_tokens: int,
        cached_tokens: int,
        output_tokens: int,
        batch_size: int,
    ) -> float:
        ttft = self.predict_ttft(input_tokens, cached_tokens, batch_size)
        avg_accum = input_tokens + output_tokens // 2
        tpot = self.predict_tpot(batch_size, avg_accum)
        return ttft + output_tokens * tpot

    def update(self, observation: LatencyObservation) -> None:
        pass


# -------------------------------------------------------
# Coefficient sets
# -------------------------------------------------------

@dataclass
class ComputeCoeffs:
    """t_compute(b, l_cold) = alpha*b*l_cold + beta*b + gamma*l_cold + delta"""
    alpha: float = 0.0
    beta: float = 0.0
    gamma: float = 0.0
    delta: float = 0.0


@dataclass
class LoadCoeffs:
    """t_load(b, l_cached) = alpha*b*l_cached + beta*l_cached + delta"""
    alpha: float = 0.0
    beta: float = 0.0
    delta: float = 0.0


@dataclass
class DecodeCoeffs:
    """tau_decode(b, l_a) = alpha*b*l_a + beta*b + gamma*l_a + delta"""
    alpha: float = 0.0
    beta: float = 0.0
    gamma: float = 0.0
    delta: float = 0.0


@dataclass
class LatencyProfile:
    """Full latency profile loaded from JSON."""
    compute: ComputeCoeffs
    load: LoadCoeffs
    decode: DecodeCoeffs
    block_size_tokens: int = 16

    @classmethod
    def from_dict(cls, d: dict) -> "LatencyProfile":
        def _coeff(section: dict, keys: list) -> dict:
            return {k: float(section.get(k, 0.0)) for k in keys}

        c = d.get("compute", {})
        l = d.get("load", {})
        dc = d.get("decode", {})

        return cls(
            compute=ComputeCoeffs(**_coeff(c, ["alpha", "beta", "gamma", "delta"])),
            load=LoadCoeffs(**_coeff(l, ["alpha", "beta", "delta"])),
            decode=DecodeCoeffs(**_coeff(dc, ["alpha", "beta", "gamma", "delta"])),
            block_size_tokens=int(d.get("block_size_tokens", 16)),
        )

    @classmethod
    def from_json(cls, path: str) -> "LatencyProfile":
        with open(path, "r") as f:
            return cls.from_dict(json.load(f))

    @classmethod
    def default(cls) -> "LatencyProfile":
        """
        Conservative default coefficients when no profile is available.
        These produce order-of-magnitude estimates, not accurate predictions.
        """
        return cls(
            compute=ComputeCoeffs(alpha=1e-7, beta=1e-4, gamma=5e-6, delta=0.01),
            load=LoadCoeffs(alpha=1e-8, beta=1e-6, delta=0.001),
            decode=DecodeCoeffs(alpha=1e-8, beta=5e-4, gamma=1e-7, delta=0.005),
            block_size_tokens=16,
        )


# -------------------------------------------------------
# LinearLatencyPredictor
# -------------------------------------------------------

class LinearLatencyPredictor(LatencyPredictor):
    """
    Analytical linear model from section A.6.

    t_prefill = t_compute(b, l_cold) + t_load(b, l_cached)
    tau_decode(b, l_a) = alpha*b*l_a + beta*b + gamma*l_a + delta

    Supports optional chunked prefill correction.
    """
    name = "linear"

    def __init__(
        self,
        profile: LatencyProfile,
        *,
        chunked_prefill: bool = False,
        max_num_batched_tokens: int = 0,
    ):
        self._p = profile
        self._chunked = chunked_prefill and max_num_batched_tokens > 0
        self._max_batched = max(1, max_num_batched_tokens) if self._chunked else 0

    def _t_compute(self, b: int, l_cold: int) -> float:
        c = self._p.compute
        return max(0.0, c.alpha * b * l_cold + c.beta * b + c.gamma * l_cold + c.delta)

    def _t_load(self, b: int, l_cached: int) -> float:
        if l_cached <= 0:
            return 0.0
        lo = self._p.load
        return max(0.0, lo.alpha * b * l_cached + lo.beta * l_cached + lo.delta)

    def _tau_decode(self, b: int, l_a: int) -> float:
        d = self._p.decode
        return max(0.0, d.alpha * b * l_a + d.beta * b + d.gamma * l_a + d.delta)

    def predict_ttft(self, input_tokens: int, cached_tokens: int, batch_size: int) -> float:
        b = max(1, batch_size)
        l_cached = max(0, cached_tokens)
        l_cold = max(0, input_tokens - l_cached)

        t_prefill = self._t_compute(b, l_cold) + self._t_load(b, l_cached)

        if self._chunked and l_cold > self._max_batched:
            n_chunks = math.ceil(l_cold / self._max_batched)
            avg_accum = input_tokens + 50
            t_interleave = (n_chunks - 1) * self._tau_decode(b, avg_accum)
            t_prefill += t_interleave

        return t_prefill

    def predict_tpot(self, batch_size: int, accumulated_len: int) -> float:
        return self._tau_decode(max(1, batch_size), max(1, accumulated_len))

    def predict_e2e(
        self,
        input_tokens: int,
        cached_tokens: int,
        output_tokens: int,
        batch_size: int,
    ) -> float:
        ttft = self.predict_ttft(input_tokens, cached_tokens, batch_size)
        avg_accum = input_tokens + output_tokens // 2
        tpot = self.predict_tpot(batch_size, avg_accum)
        return ttft + max(1, output_tokens) * tpot


# -------------------------------------------------------
# PiecewiseLinearPredictor (Step 10 upgrade)
# -------------------------------------------------------

class PiecewiseLinearPredictor(LatencyPredictor):
    """
    Multiple LinearLatencyPredictor instances, one per concurrency range.
    Falls back to the closest range if batch_size doesn't match exactly.
    """
    name = "piecewise"

    def __init__(self, profiles_by_range: dict):
        """
        profiles_by_range: { max_batch_size: LatencyProfile }
        e.g. { 4: profile_low, 16: profile_mid, 64: profile_high }
        """
        self._ranges = sorted(profiles_by_range.keys())
        self._predictors = {
            k: LinearLatencyPredictor(v)
            for k, v in profiles_by_range.items()
        }

    def _select(self, batch_size: int) -> LinearLatencyPredictor:
        for r in self._ranges:
            if batch_size <= r:
                return self._predictors[r]
        return self._predictors[self._ranges[-1]]

    def predict_ttft(self, input_tokens: int, cached_tokens: int, batch_size: int) -> float:
        return self._select(batch_size).predict_ttft(input_tokens, cached_tokens, batch_size)

    def predict_tpot(self, batch_size: int, accumulated_len: int) -> float:
        return self._select(batch_size).predict_tpot(batch_size, accumulated_len)


# -------------------------------------------------------
# BayesianLatencyPredictor (Step 10 upgrade)
# -------------------------------------------------------

class BayesianLatencyPredictor(LatencyPredictor):
    """
    Online recursive least squares (RLS) latency predictor.

    Starts from an offline prior (LinearLatencyPredictor) and refines
    coefficients online as observations arrive.

    Currently a simplified version: maintains running correction factors
    on top of the linear model rather than full RLS on all coefficients.
    """
    name = "bayesian"

    def __init__(self, base: LinearLatencyPredictor, forgetting_factor: float = 0.995):
        self._base = base
        self._lock = RLock()
        self._ff = max(0.9, min(1.0, forgetting_factor))
        self._ttft_scale = 1.0
        self._tpot_scale = 1.0
        self._n_obs = 0

    def predict_ttft(self, input_tokens: int, cached_tokens: int, batch_size: int) -> float:
        with self._lock:
            return self._base.predict_ttft(input_tokens, cached_tokens, batch_size) * self._ttft_scale

    def predict_tpot(self, batch_size: int, accumulated_len: int) -> float:
        with self._lock:
            return self._base.predict_tpot(batch_size, accumulated_len) * self._tpot_scale

    def update(self, observation: LatencyObservation) -> None:
        with self._lock:
            self._n_obs += 1
            if observation.actual_ttft_s > 0:
                predicted = self._base.predict_ttft(
                    observation.input_tokens,
                    observation.cached_tokens,
                    observation.batch_size,
                )
                if predicted > 0:
                    ratio = observation.actual_ttft_s / predicted
                    self._ttft_scale = self._ff * self._ttft_scale + (1 - self._ff) * ratio

            if observation.actual_tpot_s > 0:
                accum = observation.input_tokens + observation.output_tokens // 2
                predicted = self._base.predict_tpot(observation.batch_size, accum)
                if predicted > 0:
                    ratio = observation.actual_tpot_s / predicted
                    self._tpot_scale = self._ff * self._tpot_scale + (1 - self._ff) * ratio


# -------------------------------------------------------
# Factory
# -------------------------------------------------------

_latency_predictor_instance: Optional[LatencyPredictor] = None
_latency_predictor_lock = RLock()


def get_latency_predictor() -> Optional[LatencyPredictor]:
    """
    Return the singleton latency predictor, or None if not configured.
    Requires LATENCY_PROFILE_PATH to be set for meaningful predictions.
    """
    global _latency_predictor_instance
    with _latency_predictor_lock:
        if _latency_predictor_instance is not None:
            return _latency_predictor_instance

        from .config import get_config
        cfg = get_config()

        profile_path = str(getattr(cfg, "LATENCY_PROFILE_PATH", "")).strip()
        if profile_path and Path(profile_path).is_file():
            try:
                profile = LatencyProfile.from_json(profile_path)
            except Exception as e:
                import sys
                print(f"[latency_predictor] WARNING: failed to load profile from {profile_path}: {e}")
                sys.stdout.flush()
                profile = LatencyProfile.default()
        else:
            profile = LatencyProfile.default()

        kind = str(getattr(cfg, "LATENCY_PREDICTOR", "linear")).lower()
        chunked = bool(getattr(cfg, "CHUNKED_PREFILL_AWARE", False))
        max_batched = int(getattr(cfg, "MAX_NUM_BATCHED_TOKENS", 0))

        base = LinearLatencyPredictor(
            profile,
            chunked_prefill=chunked,
            max_num_batched_tokens=max_batched,
        )

        if kind == "bayesian":
            _latency_predictor_instance = BayesianLatencyPredictor(base)
        elif kind == "hybrid":
            _latency_predictor_instance = BayesianLatencyPredictor(base)
        else:
            _latency_predictor_instance = base

        return _latency_predictor_instance
