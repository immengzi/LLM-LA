# -*- coding: utf-8 -*-
#  simple, config-driven open-loop load generator with optional logging

import time, random, os
from typing import Callable, Deque, Tuple, Optional, Iterable, Iterator
from collections import deque

# Structured loggers
from utils import log_load, log_load_trace, get_run_dir
from config import get_config

_cfg = get_config()


def _sleep_until(t_deadline: float) -> float:
    """Sleep until deadline and return wake time (for drift measurement)."""
    now = time.time()
    if t_deadline > now:
        time.sleep(t_deadline - now)
    return time.time()


def _next_ia_seconds(rate_rps: float, kind: str, rnd: random.Random) -> float:
    """Return next interarrival interval (seconds)."""
    if rate_rps <= 0:
        return 0.5
    if kind == "poisson":
        return rnd.expovariate(rate_rps)
    if kind == "det":
        return 1.0 / rate_rps
    return 0.0


def _parse_steps(spec: str) -> list[Tuple[float, float]]:
    """Parse step schedule spec: '0:3,30:5,60:1' → [(0.0,3.0), (30.0,5.0), (60.0,1.0)]"""
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
    # --- Random-range pattern controls ---
    rand_rps_min: Optional[float] = None,
    rand_rps_max: Optional[float] = None,
    rand_epoch_s: float = 5.0,
    rand_kind: str = "poisson",
    # --- Logging controls ---
    router_mode: Optional[str] = None,
    log_every: int = 1,
    verbose: bool = True,
) -> None:
    """
    Open-loop arrival driver with deterministic RNG and detailed trace logging.
    Logs:
      - load.jsonl (summary events)
      - load_trace.jsonl (per-arrival timing and drift)
    """

    seed = int(getattr(_cfg, "LOADGEN_SEED", 12345))
    rnd = random.Random(seed)
    eff_warmup = float(warmup_s or 0.0)

    # --- Clean old trace file ---
    if router_mode:
        run_dir = get_run_dir(router_mode)
        trace_path = os.path.join(run_dir, "load_trace.jsonl")
        try:
            if os.path.exists(trace_path):
                os.remove(trace_path)
                if verbose:
                    print(f"[LOADGEN] Removed old trace file: {trace_path}")
        except Exception as e:
            print(f"[WARN] Could not remove old trace file: {e}")

    if verbose:
        print(f"[LOADGEN] pattern={pattern}, seed={seed}, rate_rps={rate_rps}")

    # Normalize to iterator or deque
    is_deque = hasattr(prompts, "popleft")
    if not is_deque:
        if hasattr(prompts, "__iter__") and not hasattr(prompts, "__next__"):
            prompts = iter(prompts)

    def _pop_next() -> Optional[str]:
        if is_deque:
            if len(prompts) == 0:
                return None
            return prompts.popleft()
        else:
            try:
                return next(prompts)
            except StopIteration:
                return None

    # --- UID + counters ---
    sent = 0
    second_counters: dict[int, int] = {}
    uid_base_ts: float | None = None
    start_emitted: bool = False
    trace_idx: int = 0

    def _emit_start_if_needed(now: float, *, pattern: str, rps: float | None):
        nonlocal start_emitted, uid_base_ts
        if start_emitted:
            return
        uid_base_ts = now
        if router_mode:
            log_load(
                router_mode=router_mode,
                event="start",
                pattern=pattern,
                rps=rps,
                extra={
                    "seed": seed,
                    "warmup_s": eff_warmup,
                    "effective_start_ts": now,
                },
            )
        start_emitted = True

    def _next_uid(now: float) -> tuple[str, int]:
        base = uid_base_ts if uid_base_ts is not None else now
        sec = int(now - base)
        cnt = second_counters.get(sec, 0) + 1
        second_counters[sec] = cnt
        return f"{sec}-{cnt}", sec

    if verbose:
        print(f"[LOAD] start pattern={pattern}, rate={rate_rps}, warmup={eff_warmup}s, duration={duration_s}s")

    t0 = time.time()
    end_at = t0 + eff_warmup + max(0.0, duration_s)

    # ---- Warmup ----
    if eff_warmup > 0 and pattern != "dump":
        if verbose:
            print(f"[LOAD] warmup for {eff_warmup:.3f}s")
        while (time.time() - t0) < eff_warmup:
            p = _pop_next()
            if p is None:
                break
            planned_at = time.time()
            woke_at = planned_at
            enq_at = time.time()
            enqueue_one(p, enq_at)
            sent += 1
            if router_mode and (sent % max(1, log_every) == 0):
                trace_idx += 1
                log_load_trace(
                    router_mode=router_mode,
                    idx=trace_idx,
                    phase="warmup",
                    pattern=pattern,
                    uid=None,
                    second=None,
                    rps_effective=rate_rps,
                    planned_at=planned_at,
                    woke_at=woke_at,
                    enq_at=enq_at,
                    note="warmup",
                )
                log_load(router_mode=router_mode, event="arrival", pattern=pattern, extra={"phase": "warmup"})
            time.sleep(0.002)

    # ---- Dump ----
    if pattern == "dump":
        first_ts: float | None = None
        while True:
            p = _pop_next()
            if p is None:
                break
            if first_ts is None:
                first_ts = time.time()
                _emit_start_if_needed(first_ts, pattern="dump", rps=None)
            planned_at = first_ts
            woke_at = time.time()
            enq_at = first_ts
            enqueue_one(p, enq_at)
            sent += 1
            if router_mode and (sent % max(1, log_every) == 0):
                uid, sec = _next_uid(enq_at)
                trace_idx += 1
                log_load_trace(
                    router_mode=router_mode,
                    idx=trace_idx,
                    phase="dump",
                    pattern="dump",
                    uid=uid,
                    second=sec,
                    rps_effective=None,
                    planned_at=planned_at,
                    woke_at=woke_at,
                    enq_at=enq_at,
                )
                log_load(router_mode=router_mode, event="arrival", pattern="dump", extra={"uid": uid, "second": sec})
        if router_mode:
            log_load(router_mode=router_mode, event="done", pattern="dump", extra={"sent": sent})
        if verbose:
            print(f"[LOAD] done. sent total={sent} prompts (dump).")
        return

    # ---- Main Patterns ----
    next_at = time.time()

    def _record(now, planned_at, woke_at, cur_rps, pattern_name):
        nonlocal trace_idx
        uid, sec = _next_uid(now)
        trace_idx += 1
        log_load_trace(
            router_mode=router_mode,
            idx=trace_idx,
            phase="main",
            pattern=pattern_name,
            uid=uid,
            second=sec,
            rps_effective=cur_rps,
            planned_at=planned_at,
            woke_at=woke_at,
            enq_at=now,
        )
        log_load(router_mode=router_mode, event="arrival", pattern=pattern_name, rps=cur_rps, extra={"uid": uid, "second": sec})

    if pattern in ("poisson", "det"):
        while time.time() < end_at:
            planned_at = next_at + _next_ia_seconds(rate_rps, pattern, rnd)
            woke_at = _sleep_until(planned_at)
            p = _pop_next()
            if p is None:
                break
            now = time.time()
            enqueue_one(p, now)
            sent += 1
            _emit_start_if_needed(now, pattern=pattern, rps=rate_rps)
            if router_mode and (sent % max(1, log_every) == 0):
                _record(now, planned_at, woke_at, rate_rps, pattern)
            next_at = planned_at

    elif pattern == "bursty":
        cur_on = True
        window_end = time.time() + burst_on_s
        while time.time() < end_at:
            r = burst_rps_on if cur_on else burst_rps_off
            if r <= 0:
                _sleep_until(window_end)
            else:
                planned_at = next_at + _next_ia_seconds(r, "poisson", rnd)
                woke_at = _sleep_until(planned_at)
                p = _pop_next()
                if p is None:
                    break
                now = time.time()
                enqueue_one(p, now)
                sent += 1
                _emit_start_if_needed(now, pattern="bursty", rps=r)
                if router_mode and (sent % max(1, log_every) == 0):
                    _record(now, planned_at, woke_at, r, "bursty")
            if time.time() >= window_end:
                cur_on = not cur_on
                window_end = time.time() + (burst_on_s if cur_on else burst_off_s)
                next_at = time.time()

    elif pattern == "steps":
        steps = _parse_steps(step_schedule) or [(0.0, rate_rps)]
        base = time.time()
        step_idx = 0
        while time.time() < end_at:
            if step_idx + 1 < len(steps):
                t_next, _ = steps[step_idx + 1]
                if (time.time() - base) >= t_next:
                    step_idx += 1
                    next_at = time.time()
            _, cur_rps = steps[step_idx]
            if cur_rps <= 0:
                time.sleep(0.05)
                continue
            planned_at = next_at + _next_ia_seconds(cur_rps, "poisson", rnd)
            woke_at = _sleep_until(planned_at)
            p = _pop_next()
            if p is None:
                break
            now = time.time()
            enqueue_one(p, now)
            sent += 1
            _emit_start_if_needed(now, pattern="steps", rps=cur_rps)
            if router_mode and (sent % max(1, log_every) == 0):
                _record(now, planned_at, woke_at, cur_rps, "steps")
            next_at = planned_at

    elif pattern == "rand":
        lo = rand_rps_min if rand_rps_min is not None else max(0.0, min(rate_rps, burst_rps_off))
        hi = rand_rps_max if rand_rps_max is not None else max(rate_rps, burst_rps_on, lo)
        next_epoch_end = time.time() + max(0.1, rand_epoch_s)
        cur_rps = rnd.uniform(lo, hi)
        while time.time() < end_at:
            now = time.time()
            if now >= next_epoch_end:
                cur_rps = rnd.uniform(lo, hi)
                next_epoch_end = now + max(0.1, rand_epoch_s)
                next_at = now
            if cur_rps <= 0:
                time.sleep(0.05)
                continue
            k = "poisson" if rand_kind not in ("poisson", "det") else rand_kind
            planned_at = next_at + _next_ia_seconds(cur_rps, k, rnd)
            woke_at = _sleep_until(planned_at)
            p = _pop_next()
            if p is None:
                break
            now = time.time()
            enqueue_one(p, now)
            sent += 1
            _emit_start_if_needed(now, pattern="rand", rps=cur_rps)
            if router_mode and (sent % max(1, log_every) == 0):
                _record(now, planned_at, woke_at, cur_rps, "rand")
            next_at = planned_at

    else:
        raise ValueError(f"Unknown pattern '{pattern}'")

    if router_mode:
        log_load(router_mode=router_mode, event="done", pattern=pattern, extra={"sent": sent})
    if verbose:
        print(f"[LOAD] done. sent total={sent} prompts in {time.time()-t0:.1f}s")
