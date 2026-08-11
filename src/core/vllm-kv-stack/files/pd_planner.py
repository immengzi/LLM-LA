#!/usr/bin/env python3
"""Advisory Prefill/Decode ratio planner (decision function only).

This module computes *recommended* target replica counts from a metrics
snapshot. It performs no execution: it never starts/removes containers and
never patches Deployments. The executor (Docker or Kubernetes backend) is the
only component allowed to apply a transition.

Design mirrors the load-based mode of the Dynamo Planner and the role-level
metrics exposed by MindIE Motor, while keeping LLM-LA's fixed-budget invariant:

    min_prefill <= prefill
    min_decode  <= decode
    prefill + decode <= max_total

The policy is deliberately simple and explainable:

    prefill pressure  (backlog tokens)  high  + decode relaxed -> D -> P
    decode pressure   (KV utilization)  high  + prefill relaxed -> P -> D

Hysteresis (separate scale-up / scale-down thresholds), a sustained-observation
streak, and a post-change cooldown prevent flapping between adjacent targets.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from typing import Optional

from pd_rebalancer import Replicas


@dataclass(frozen=True)
class MetricsSnapshot:
    """One sample of the role-level pressure signals."""

    prefill_backlog_tokens: float
    decode_kv_usage_percent: float

    def validate(self) -> None:
        if self.prefill_backlog_tokens < 0:
            raise ValueError("prefill_backlog_tokens must be >= 0")
        if not 0.0 <= self.decode_kv_usage_percent <= 100.0:
            raise ValueError("decode_kv_usage_percent must be within 0..100")


@dataclass(frozen=True)
class PlannerConfig:
    min_prefill: int = 1
    min_decode: int = 1
    max_total: int = 3
    # Prefill pressure thresholds (backlog tokens).
    prefill_scale_up_tokens: float = 512.0
    prefill_scale_down_tokens: float = 128.0
    # Decode pressure thresholds (KV cache utilization percent).
    decode_scale_up_kv_percent: float = 80.0
    decode_scale_down_kv_percent: float = 60.0
    # Require the same direction for N consecutive samples before acting.
    min_observations: int = 5
    # Minimum seconds between two executed transitions.
    cooldown_seconds: float = 300.0

    def validate(self) -> None:
        if self.min_prefill < 1 or self.min_decode < 1:
            raise ValueError("min_prefill and min_decode must be >= 1")
        if self.max_total < self.min_prefill + self.min_decode:
            raise ValueError("max_total must be >= min_prefill + min_decode")
        if self.prefill_scale_up_tokens <= self.prefill_scale_down_tokens:
            raise ValueError("prefill scale-up threshold must exceed scale-down threshold")
        if self.decode_scale_up_kv_percent <= self.decode_scale_down_kv_percent:
            raise ValueError("decode scale-up threshold must exceed scale-down threshold")
        if self.min_observations < 1:
            raise ValueError("min_observations must be >= 1")
        if self.cooldown_seconds < 0:
            raise ValueError("cooldown_seconds must be >= 0")


@dataclass(frozen=True)
class PlannerState:
    """Persistent state needed to keep the policy deterministic across calls."""

    last_change_timestamp: float = 0.0
    decode_to_prefill_streak: int = 0
    prefill_to_decode_streak: int = 0

    def to_dict(self) -> dict[str, float]:
        return {
            "last_change_timestamp": self.last_change_timestamp,
            "decode_to_prefill_streak": self.decode_to_prefill_streak,
            "prefill_to_decode_streak": self.prefill_to_decode_streak,
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "PlannerState":
        return cls(
            last_change_timestamp=float(raw.get("last_change_timestamp", 0.0)),
            decode_to_prefill_streak=int(raw.get("decode_to_prefill_streak", 0)),
            prefill_to_decode_streak=int(raw.get("prefill_to_decode_streak", 0)),
        )


@dataclass(frozen=True)
class PlannerDecision:
    """Advisory result. target is None when the policy recommends no change."""

    target: Optional[Replicas]
    reason: str
    state: PlannerState

    def to_dict(self) -> dict:
        return {
            "target": None if self.target is None else {
                "prefill": self.target.prefill,
                "decode": self.target.decode,
            },
            "reason": self.reason,
            "state": self.state.to_dict(),
        }


def decide(
    current: Replicas,
    metrics: MetricsSnapshot,
    config: PlannerConfig,
    state: PlannerState,
    now: float,
) -> PlannerDecision:
    """Return an advisory target for one metrics sample.

    The function is pure: all mutable behavior is carried in ``state`` and the
    wall clock is injected via ``now``.
    """

    config.validate()
    metrics.validate()

    prefill_pressure = metrics.prefill_backlog_tokens >= config.prefill_scale_up_tokens
    prefill_relaxed = metrics.prefill_backlog_tokens <= config.prefill_scale_down_tokens
    decode_pressure = metrics.decode_kv_usage_percent >= config.decode_scale_up_kv_percent
    decode_relaxed = metrics.decode_kv_usage_percent <= config.decode_scale_down_kv_percent

    # Candidate directions. Only one replica moves per transition, and only if
    # the fixed budget and role floors still hold after the move.
    wants_decode_to_prefill = (
        prefill_pressure
        and decode_relaxed
        and current.decode > config.min_decode
        and current.prefill + 1 <= config.max_total
    )
    wants_prefill_to_decode = (
        decode_pressure
        and prefill_relaxed
        and current.prefill > config.min_prefill
        and current.decode + 1 <= config.max_total
    )

    cooldown_elapsed = now - state.last_change_timestamp >= config.cooldown_seconds

    if wants_decode_to_prefill and not wants_prefill_to_decode:
        streak = state.decode_to_prefill_streak + 1
        next_state = PlannerState(
            last_change_timestamp=state.last_change_timestamp,
            decode_to_prefill_streak=streak,
            prefill_to_decode_streak=0,
        )
        if streak >= config.min_observations and cooldown_elapsed:
            target = Replicas(prefill=current.prefill + 1, decode=current.decode - 1)
            next_state = PlannerState(
                last_change_timestamp=now,
                decode_to_prefill_streak=0,
                prefill_to_decode_streak=0,
            )
            return PlannerDecision(
                target=target,
                reason=(
                    f"prefill backlog {metrics.prefill_backlog_tokens:.0f} >= "
                    f"{config.prefill_scale_up_tokens:.0f} and decode KV "
                    f"{metrics.decode_kv_usage_percent:.1f}% <= "
                    f"{config.decode_scale_down_kv_percent:.1f}%; D->P"
                ),
                state=next_state,
            )
        return PlannerDecision(
            target=None,
            reason=(
                f"D->P candidate observed {streak}/{config.min_observations} times"
                + ("" if cooldown_elapsed else " (cooldown active)")
            ),
            state=next_state,
        )

    if wants_prefill_to_decode and not wants_decode_to_prefill:
        streak = state.prefill_to_decode_streak + 1
        next_state = PlannerState(
            last_change_timestamp=state.last_change_timestamp,
            decode_to_prefill_streak=0,
            prefill_to_decode_streak=streak,
        )
        if streak >= config.min_observations and cooldown_elapsed:
            target = Replicas(prefill=current.prefill - 1, decode=current.decode + 1)
            next_state = PlannerState(
                last_change_timestamp=now,
                decode_to_prefill_streak=0,
                prefill_to_decode_streak=0,
            )
            return PlannerDecision(
                target=target,
                reason=(
                    f"decode KV {metrics.decode_kv_usage_percent:.1f}% >= "
                    f"{config.decode_scale_up_kv_percent:.1f}% and prefill backlog "
                    f"{metrics.prefill_backlog_tokens:.0f} <= "
                    f"{config.prefill_scale_down_tokens:.0f}; P->D"
                ),
                state=next_state,
            )
        return PlannerDecision(
            target=None,
            reason=(
                f"P->D candidate observed {streak}/{config.min_observations} times"
                + ("" if cooldown_elapsed else " (cooldown active)")
            ),
            state=next_state,
        )

    # No clear direction (both relaxed, both pressured, or budget/floor blocked).
    reset_state = PlannerState(
        last_change_timestamp=state.last_change_timestamp,
        decode_to_prefill_streak=0,
        prefill_to_decode_streak=0,
    )
    return PlannerDecision(
        target=None,
        reason=(
            "no change: prefill_pressure=%s decode_pressure=%s "
            "prefill_relaxed=%s decode_relaxed=%s"
            % (prefill_pressure, decode_pressure, prefill_relaxed, decode_relaxed)
        ),
        state=reset_state,
    )


def default_config() -> PlannerConfig:
    return PlannerConfig()


def _load_json(path: str) -> dict:
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def _write_json(path: str, payload: dict) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")


def advise_cli(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Advisory P/D ratio planner (no execution)")
    parser.add_argument("--metrics", required=True, help="JSON metrics snapshot file")
    parser.add_argument("--current", required=True, help="current replicas as P,D (e.g. 2,1)")
    parser.add_argument("--state", help="optional state file to read and update")
    parser.add_argument("--config", help="optional JSON planner config file")
    parser.add_argument("--now", type=float, help="override wall clock (epoch seconds)")
    args = parser.parse_args(argv)

    metrics_raw = _load_json(args.metrics)
    metrics = MetricsSnapshot(
        prefill_backlog_tokens=float(metrics_raw["prefill_backlog_tokens"]),
        decode_kv_usage_percent=float(metrics_raw["decode_kv_usage_percent"]),
    )
    prefill, decode = (int(part) for part in args.current.split(","))
    current = Replicas(prefill=prefill, decode=decode)

    config = default_config()
    if args.config:
        raw = _load_json(args.config)
        config = PlannerConfig(
            min_prefill=int(raw.get("min_prefill", config.min_prefill)),
            min_decode=int(raw.get("min_decode", config.min_decode)),
            max_total=int(raw.get("max_total", config.max_total)),
            prefill_scale_up_tokens=float(raw.get("prefill_scale_up_tokens", config.prefill_scale_up_tokens)),
            prefill_scale_down_tokens=float(raw.get("prefill_scale_down_tokens", config.prefill_scale_down_tokens)),
            decode_scale_up_kv_percent=float(raw.get("decode_scale_up_kv_percent", config.decode_scale_up_kv_percent)),
            decode_scale_down_kv_percent=float(raw.get("decode_scale_down_kv_percent", config.decode_scale_down_kv_percent)),
            min_observations=int(raw.get("min_observations", config.min_observations)),
            cooldown_seconds=float(raw.get("cooldown_seconds", config.cooldown_seconds)),
        )

    state = PlannerState()
    if args.state and os.path.exists(args.state):
        state = PlannerState.from_dict(_load_json(args.state))
    now = args.now if args.now is not None else time.time()

    decision = decide(current, metrics, config, state, now)
    payload = decision.to_dict()
    payload["current"] = {"prefill": current.prefill, "decode": current.decode}
    payload["config"] = {
        "min_prefill": config.min_prefill,
        "min_decode": config.min_decode,
        "max_total": config.max_total,
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    if args.state:
        _write_json(args.state, decision.state.to_dict())
    return 0


if __name__ == "__main__":
    sys.exit(advise_cli())
