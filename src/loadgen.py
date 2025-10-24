# -*- coding: utf-8 -*-
#  simple, config-driven open-loop load generator with optional logging

import time, random
from typing import Callable, Deque, Tuple, Optional, Iterable, Iterator
from collections import deque


def _sleep_until(t_deadline: float) -> None:
    now = time.time()
    if t_deadline > now:
        time.sleep(t_deadline - now)


def _next_ia_seconds(rate_rps: float, kind: str) -> float:
    if rate_rps <= 0:
        return 0.5
    if kind == "poisson":
        return random.expovariate(rate_rps)
    if kind == "det":
        return 1.0 / rate_rps
    return 0.0


def _parse_steps(spec: str) -> list[Tuple[float, float]]:
    out = []
    if not spec:
        return out
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        t_str, rps_str = part.split(":")
        out.append((float(t_str), float(rps_str)))
    out.sort(key=lambda x: x[0])
    return out


def drive_load(
    *,
    pattern: str,
    prompts: Iterable[str] | Iterator[str] | Deque[str],
    enqueue_one: Callable[[str, float], None],  # (prompt, t_enq_client)
    rate_rps: float = 5.0,
    warmup_s: float = 5.0,
    duration_s: float = 60.0,
    burst_on_s: float = 2.0,
    burst_off_s: float = 2.0,
    burst_rps_on: float = 10.0,
    burst_rps_off: float = 0.0,
    step_schedule: str = "",
    verbose: bool = True,
) -> None:
    print(f"rate_rps - here: {rate_rps}")
    """
    Open-loop arrival driver. Consumes from 'prompts' lazily and calls 'enqueue_one(p, t_enq)'.
    Supports:
      - deque (legacy)
      - iterator/generator (streaming)
    Patterns:
      - "dump": enqueue all immediately
      - "poisson": exponential inter-arrivals
      - "det": fixed inter-arrivals
      - "bursty": alternating on/off load windows
      - "steps": piecewise-constant RPS from schedule
    """

    # normalize to iterator or deque
    is_deque = hasattr(prompts, "popleft")
    if not is_deque:
        # If it's an iterable but not an iterator, make it one
        if hasattr(prompts, "__iter__") and not hasattr(prompts, "__next__"):
            prompts = iter(prompts)

    def _pop_next() -> Optional[str]:
        """Get next prompt from deque or iterator."""
        if is_deque:
            if len(prompts) == 0:
                return None
            return prompts.popleft()
        else:
            try:
                return next(prompts)
            except StopIteration:
                return None

    t0 = time.time()
    sent = 0

    if verbose:
        print(
            f"[LOAD] start pattern={pattern}, rate={rate_rps}, warmup={warmup_s}s, duration={duration_s}s"
        )

    # --- Warmup phase ---
    if warmup_s > 0 and pattern != "dump":
        if verbose:
            print(f"[LOAD] warmup for {warmup_s}s")
        while (time.time() - t0) < warmup_s:
            p = _pop_next()
            if p is None:
                break
            enqueue_one(p, time.time())
            sent += 1
            if verbose and sent % 10 == 0:
                print(f"[LOAD] warmup sent={sent}")
            time.sleep(0.002)

    # --- Dump mode ---
    if pattern == "dump":
        now = time.time()
        while True:
            p = _pop_next()
            if p is None:
                break
            enqueue_one(p, now)
            sent += 1
        if verbose:
            print(f"[LOAD] dump mode: sent {sent} prompts")
        return

    end_at = t0 + warmup_s + max(0.0, duration_s)

    # --- Poisson or deterministic arrivals ---
    if pattern in ("poisson", "det"):
        next_at = time.time()
        while time.time() < end_at:
            next_at += _next_ia_seconds(rate_rps, pattern)
            _sleep_until(next_at)
            p = _pop_next()
            if p is None:
                break
            enqueue_one(p, time.time())
            sent += 1
            if verbose and sent % 50 == 0:
                print(f"[LOAD] steady sent={sent} so far at {time.time()-t0:.1f}s")

    # --- Bursty pattern ---
    elif pattern == "bursty":
        cur_on = True
        window_end = time.time() + burst_on_s
        next_at = time.time()
        while time.time() < end_at:
            r = burst_rps_on if cur_on else burst_rps_off
            if r <= 0:
                if verbose:
                    print(f"[LOAD] off-window idle for {burst_off_s}s")
                _sleep_until(window_end)
            else:
                next_at += _next_ia_seconds(r, "poisson")
                _sleep_until(next_at)
                p = _pop_next()
                if p is None:
                    break
                enqueue_one(p, time.time())
                sent += 1
                if verbose and sent % 50 == 0:
                    print(f"[LOAD] burst sent={sent} so far")
            if time.time() >= window_end:
                cur_on = not cur_on
                window_end = time.time() + (burst_on_s if cur_on else burst_off_s)
                next_at = time.time()

    # --- Steps pattern ---
    elif pattern == "steps":
        steps = _parse_steps(step_schedule) or [(0.0, rate_rps)]
        base = time.time()
        step_idx = 0
        next_at = time.time()
        while time.time() < end_at:
            if step_idx + 1 < len(steps):
                t_next, _ = steps[step_idx + 1]
                if (time.time() - base) >= t_next:
                    step_idx += 1
                    next_at = time.time()
                    if verbose:
                        print(
                            f"[LOAD] step {step_idx} => rate={steps[step_idx][1]} rps"
                        )
            _, cur_rps = steps[step_idx]
            if cur_rps <= 0:
                time.sleep(0.05)
                continue
            next_at += _next_ia_seconds(cur_rps, "poisson")
            _sleep_until(next_at)
            p = _pop_next()
            if p is None:
                break
            enqueue_one(p, time.time())
            sent += 1
            if verbose and sent % 50 == 0:
                print(f"[LOAD] step sent={sent} so far")

    if verbose:
        print(f"[LOAD] done. sent total={sent} prompts in {time.time()-t0:.1f}s")
