# -*- coding: utf-8 -*-
#  simple, config-driven open-loop load generator with unified logging
#  Precompute full schedule (times + RPS) for all patterns, then execute.
#  IMPORTANT: per-second fixed-count, evenly-spaced arrivals are used for all patterns.

import time, random, os
from typing import Callable, Deque, Tuple, Optional, Iterable, Iterator, List

# Structured loggers
from utils import log_load, get_run_dir
from config import get_config

_cfg = get_config()


# --------- time helpers (monotonic for sleep, wall for logs) ---------

_BASE_MONO = time.monotonic()
_BASE_WALL = time.time()

def _to_wall(ts_mono: float) -> float:
    """Convert a monotonic timestamp to wall-clock seconds."""
    return _BASE_WALL + (ts_mono - _BASE_MONO)

def _sleep_until_mono(t_deadline_mono: float) -> float:
    """Sleep until monotonic deadline and return wake time (monotonic)."""
    now = time.monotonic()
    if t_deadline_mono > now:
        time.sleep(t_deadline_mono - now)
    return time.monotonic()


# ---------------- basic helpers ----------------

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


# --------------- schedule builders (precompute) ----------------

def _materialize_prompts(prompts: Iterable[str] | Iterator[str] | Deque[str]) -> List[str]:
    """Turn any iterable/iterator/deque of prompts into a list (materialize)."""
    if hasattr(prompts, "popleft"):
        dq = prompts  # type: ignore
        return [dq.popleft() for _ in range(len(dq))]
    if hasattr(prompts, "__iter__") and not hasattr(prompts, "__next__"):
        return list(prompts)  # type: ignore
    # iterator
    out: List[str] = []
    for p in prompts:  # type: ignore
        out.append(p)
    return out

def _build_schedule_dump(*, t0_mono: float, items: int) -> Tuple[List[float], List[Optional[int]]]:
    """All at t0 (same timestamp)."""
    return [t0_mono] * items, [None] * items

def _emit_even_points_in_window(sec_start: float, sec_len: float, n: int) -> List[float]:
    """Return n evenly spaced points inside [sec_start, sec_start+sec_len]."""
    if n <= 0:
        return []
    if sec_len <= 0:
        return []
    # center each slice: (k + 0.5)/n
    step = sec_len / n
    return [sec_start + (k + 0.5) * step for k in range(n)]

def _build_schedule_per_second_even(*, start_mono: float, duration_s: float,
                                    rps_int: int, max_items: int) -> Tuple[List[float], List[int]]:
    """
    Exactly rps_int arrivals per one-second window, evenly spaced within the window.
    For partial leading/trailing windows, scale by window length: n = round(rps_int * L).
    Returns (times, rps_effective_per_event).
    """
    times: List[float] = []
    rps_list: List[int] = []
    if duration_s <= 0 or max_items <= 0 or rps_int <= 0:
        return times, rps_list

    end_mono = start_mono + duration_s
    sec_start = start_mono
    while sec_start < end_mono and len(times) < max_items:
        sec_len = min(1.0, end_mono - sec_start)
        n = int(round(rps_int * sec_len))
        chunk = _emit_even_points_in_window(sec_start, sec_len, n)
        for ts in chunk:
            times.append(ts)
            rps_list.append(rps_int)
            if len(times) >= max_items:
                break
        sec_start += sec_len
    return times, rps_list

def _build_schedule_steps_even(*, start_mono: float, duration_s: float,
                               steps: list[Tuple[float, float]],
                               max_items: int) -> Tuple[List[float], List[int]]:
    """
    Steps is list of (offset_sec, rps). Within each second, evenly place n requests where
    n = round(rps * second_length_in_segment).
    Returns (times, rps_effective_per_event).
    """
    times: List[float] = []
    rps_list: List[int] = []
    if duration_s <= 0 or max_items <= 0 or not steps:
        return times, rps_list

    end_mono = start_mono + duration_s

    # Build segments: [(seg_start, seg_end, rps_int)]
    segs: List[Tuple[float, float, int]] = []
    for i, (off, rps) in enumerate(steps):
        seg_start = start_mono + off
        seg_end = start_mono + (steps[i + 1][0] if i + 1 < len(steps) else duration_s)
        seg_end = min(seg_end, end_mono)
        if seg_end <= seg_start:
            continue
        segs.append((seg_start, seg_end, int(round(max(0.0, rps)))))

    # Walk second-by-second and use the segment that covers the midpoint
    t = start_mono
    while t < end_mono and len(times) < max_items:
        sec_end = min(t + 1.0, end_mono)
        mid = (t + sec_end) * 0.5
        rps_int = 0
        for s0, s1, rps_i in segs:
            if s0 <= mid < s1:
                rps_int = rps_i
                break
        n = int(round(rps_int * (sec_end - t)))
        chunk = _emit_even_points_in_window(t, sec_end - t, n)
        for ts in chunk:
            times.append(ts)
            rps_list.append(rps_int)
            if len(times) >= max_items:
                break
        t = sec_end
    return times, rps_list

def _build_schedule_bursty_even(*, start_mono: float, duration_s: float,
                                on_s: float, off_s: float,
                                rps_on: float, rps_off: float,
                                max_items: int) -> Tuple[List[float], List[int]]:
    """
    Alternating on/off windows. Within each second, place an even, fixed count:
    n = round(rps_on * L) for on-windows (scaled by overlap length L),
    n = round(rps_off * L) for off-windows.
    Returns (times, rps_effective_per_event).
    """
    times: List[float] = []
    rps_list: List[int] = []
    if duration_s <= 0 or max_items <= 0:
        return times, rps_list

    end_mono = start_mono + duration_s
    t = start_mono
    cur_on = True
    window_end = t + (on_s if cur_on else off_s)
    rps_on_i = int(round(max(0.0, rps_on)))
    rps_off_i = int(round(max(0.0, rps_off)))

    while t < end_mono and len(times) < max_items:
        sec_end = min(t + 1.0, end_mono)
        # Emit possibly in two parts if the window flips inside this second
        remain = sec_end - t
        while remain > 0 and len(times) < max_items:
            sub_end = min(window_end, sec_end)
            sub_len = sub_end - t
            rps_int = rps_on_i if cur_on else rps_off_i
            n = int(round(rps_int * sub_len))
            chunk = _emit_even_points_in_window(t, sub_len, n)
            for ts in chunk:
                times.append(ts)
                rps_list.append(rps_int)
                if len(times) >= max_items:
                    break
            t = sub_end
            remain = sec_end - t
            if t >= window_end:
                cur_on = not cur_on
                window_end = t + (on_s if cur_on else off_s)
    return times, rps_list

def _build_schedule_rand_even(*, start_mono: float, duration_s: float,
                              lo: int, hi: int, epoch_s: float,
                              rnd: random.Random, max_items: int) -> Tuple[List[float], List[int]]:
    """
    Random epochs: every epoch_s sample integer RPS in [lo, hi]. Within each second,
    emit an even fixed count for that epoch's RPS.
    Returns (times, rps_effective_per_event).
    """
    times: List[float] = []
    rps_list: List[int] = []
    if duration_s <= 0 or max_items <= 0 or lo < 0 or hi < 0 or hi < lo:
        return times, rps_list

    end_mono = start_mono + duration_s
    t_epoch = start_mono

    while t_epoch < end_mono and len(times) < max_items:
        cur_rps = rnd.randint(int(lo), int(hi))
        epoch_end = min(t_epoch + epoch_s, end_mono)
        t = t_epoch
        while t < epoch_end and len(times) < max_items:
            sec_end = min(t + 1.0, epoch_end)
            n = int(round(cur_rps * (sec_end - t)))
            chunk = _emit_even_points_in_window(t, sec_end - t, n)
            for ts in chunk:
                times.append(ts)
                rps_list.append(cur_rps)
                if len(times) >= max_items:
                    break
            t = sec_end
        t_epoch += epoch_s

    return times, rps_list


# ------------------------------ main driver ------------------------------

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
    rand_kind: str = "poisson",  # IGNORED for timing; kept only for metadata in logs
    # --- Logging controls ---
    router_mode: Optional[str] = None,
    log_every: int = 1,
    verbose: bool = True,
) -> None:
    """
    Open-loop arrival driver with deterministic RNG and unified JSONL logging.
    Precomputes the full schedule of enqueue times before sending anything.

    NOTE: This version ALWAYS uses per-second fixed-count, evenly-spaced arrivals.
    """

    seed = int(getattr(_cfg, "LOADGEN_SEED", 12345))
    rnd = random.Random(seed)
    eff_warmup = float(warmup_s or 0.0)
    eff_duration = float(max(0.0, duration_s))

    # --- Clean old unified log file ---
    if router_mode:
        run_dir = get_run_dir(router_mode)
        unified_path = os.path.join(run_dir, "load.jsonl")
        try:
            if os.path.exists(unified_path):
                os.remove(unified_path)
                if verbose:
                    print(f"[LOADGEN] Removed old log file: {unified_path}")
        except Exception as e:
            print(f"[WARN] Could not remove old log file: {e}")

    if verbose:
        print(f"[LOADGEN] pattern={pattern}, seed={seed}, rate_rps={rate_rps}")

    # Materialize prompts (we need the total count to plan precisely)
    items = _materialize_prompts(prompts)
    total_available = len(items)
    if total_available == 0:
        if verbose:
            print("[LOADGEN] No prompts to send.")
        if router_mode:
            log_load(router_mode=router_mode, event="start", pattern=pattern, rps=None,
                     extra={"seed": seed, "warmup_s": eff_warmup, "effective_start_ts": _to_wall(time.monotonic())})
            log_load(router_mode=router_mode, event="done", pattern=pattern, extra={"sent": 0})
        return

    # Compute all planned times (monotonic) up front
    plan_times_mono: List[float] = []
    per_event_rps: List[Optional[int]] = []  # effective RPS per event (int) or None

    t0_mono = time.monotonic()
    start_main_mono = t0_mono + (0.0 if pattern == "dump" else eff_warmup)

    if pattern == "dump":
        plan_times_mono, per_event_rps = _build_schedule_dump(t0_mono=t0_mono, items=total_available)

    elif pattern in ("det", "poisson"):
        # Both map to the same behavior here: per-second even with fixed count = round(rate_rps)
        rps_i = int(round(max(0.0, rate_rps)))
        main_times, main_rps = _build_schedule_per_second_even(
            start_mono=start_main_mono,
            duration_s=eff_duration,
            rps_int=rps_i,
            max_items=total_available,
        )
        # If fewer events than prompts, push some into warmup evenly (fast ramp)
        warm_missing = max(0, total_available - len(main_times))
        warm_times: List[float] = []
        warm_rps: List[int] = []
        if warm_missing > 0 and eff_warmup > 0:
            warm_times, warm_rps = _build_schedule_per_second_even(
                start_mono=t0_mono,
                duration_s=eff_warmup,
                rps_int=rps_i,
                max_items=warm_missing,
            )
        plan_times_mono = (warm_times + main_times)[:total_available]
        per_event_rps = (list(map(int, warm_rps)) + list(map(int, main_rps)))[:total_available]

    elif pattern == "bursty":
        main_times, main_rps = _build_schedule_bursty_even(
            start_mono=start_main_mono,
            duration_s=eff_duration,
            on_s=burst_on_s,
            off_s=burst_off_s,
            rps_on=burst_rps_on,
            rps_off=burst_rps_off,
            max_items=total_available,
        )
        warm_missing = max(0, total_available - len(main_times))
        warm_times: List[float] = []
        warm_rps: List[int] = []
        if warm_missing > 0 and eff_warmup > 0:
            rps_i = int(round(max(0.0, burst_rps_on)))
            warm_times, warm_rps = _build_schedule_per_second_even(
                start_mono=t0_mono,
                duration_s=eff_warmup,
                rps_int=rps_i,
                max_items=warm_missing,
            )
        plan_times_mono = (warm_times + main_times)[:total_available]
        per_event_rps = (list(map(int, warm_rps)) + list(map(int, main_rps)))[:total_available]

    elif pattern == "steps":
        steps = _parse_steps(step_schedule) or [(0.0, rate_rps)]
        main_times, main_rps = _build_schedule_steps_even(
            start_mono=start_main_mono,
            duration_s=eff_duration,
            steps=steps,
            max_items=total_available,
        )
        warm_missing = max(0, total_available - len(main_times))
        warm_times: List[float] = []
        warm_rps: List[int] = []
        if warm_missing > 0 and eff_warmup > 0:
            rps_i = int(round(max(0.0, steps[0][1])))
            warm_times, warm_rps = _build_schedule_per_second_even(
                start_mono=t0_mono,
                duration_s=eff_warmup,
                rps_int=rps_i,
                max_items=warm_missing,
            )
        plan_times_mono = (warm_times + main_times)[:total_available]
        per_event_rps = (list(map(int, warm_rps)) + list(map(int, main_rps)))[:total_available]

    elif pattern == "rand":
        lo = int(rand_rps_min if rand_rps_min is not None else max(0.0, min(rate_rps, burst_rps_off)))
        hi = int(rand_rps_max if rand_rps_max is not None else max(rate_rps, burst_rps_on, lo))
        epoch = max(0.1, float(rand_epoch_s))
        main_times, main_rps = _build_schedule_rand_even(
            start_mono=start_main_mono,
            duration_s=eff_duration,
            lo=lo,
            hi=hi,
            epoch_s=epoch,
            rnd=rnd,
            max_items=total_available,
        )
        warm_missing = max(0, total_available - len(main_times))
        warm_times: List[float] = []
        warm_rps: List[int] = []
        if warm_missing > 0 and eff_warmup > 0:
            # Pick a plausible warmup RPS (middle of range) for even spacing
            rps_i = int(round((lo + hi) / 2.0))
            warm_times, warm_rps = _build_schedule_per_second_even(
                start_mono=t0_mono,
                duration_s=eff_warmup,
                rps_int=max(0, rps_i),
                max_items=warm_missing,
            )
        plan_times_mono = (warm_times + main_times)[:total_available]
        per_event_rps = (list(map(int, warm_rps)) + list(map(int, main_rps)))[:total_available]

    else:
        raise ValueError(f"Unknown pattern '{pattern}'")

    # If schedule is shorter than prompts, trim prompts to match
    if len(plan_times_mono) < total_available:
        items = items[: len(plan_times_mono)]
        per_event_rps = per_event_rps[: len(plan_times_mono)]
        total_available = len(items)

    # Logging book-keeping
    if verbose:
        print(f"[LOAD] start pattern={pattern}, computed_events={len(plan_times_mono)}, warmup={eff_warmup}s, duration={eff_duration}s")

    # Emit 'start'
    if router_mode:
        log_load(
            router_mode=router_mode,
            event="start",
            pattern=pattern,
            rps=float(rate_rps) if pattern in ("det", "poisson") else None,
            extra={
                "seed": seed,
                "warmup_s": eff_warmup,
                "effective_start_ts": _to_wall(t0_mono),
            },
        )

    # UID tracking
    second_counters: dict[int, int] = {}
    uid_base_wall = _to_wall(plan_times_mono[0]) if plan_times_mono else _to_wall(t0_mono)
    trace_idx = 0
    sent = 0

    def _uid_for_wall(ts_wall: float) -> tuple[str, int]:
        sec = int(max(0.0, ts_wall - uid_base_wall))
        cnt = second_counters.get(sec, 0) + 1
        second_counters[sec] = cnt
        return f"{sec}-{cnt}", sec

    # Execute the schedule
    # Phase tagging: anything < start_main_mono is 'warmup', else 'main'
    for ts_mono, prompt, rps_eff in zip(plan_times_mono, items, per_event_rps):
        planned_at_mono = ts_mono
        woke_mono = _sleep_until_mono(planned_at_mono)
        enq_mono = time.monotonic()

        planned_at_wall = _to_wall(planned_at_mono)
        woke_at_wall = _to_wall(woke_mono)
        enq_at_wall = _to_wall(enq_mono)

        # Enqueue
        enqueue_one(prompt, enq_at_wall)
        sent += 1

        # Log per-arrival (respect log_every)
        if router_mode and (sent % max(1, log_every) == 0):
            trace_idx += 1
            uid, sec = _uid_for_wall(enq_at_wall)
            log_load(
                router_mode=router_mode,
                event="arrival",
                pattern=pattern,
                phase=("dump" if pattern == "dump" else ("warmup" if planned_at_mono < start_main_mono else "main")),
                idx=trace_idx,
                uid=uid,
                second=sec,
                rps_effective=(int(rps_eff) if rps_eff is not None else None),
                planned_at=planned_at_wall,
                woke_at=woke_at_wall,
                enq_at=enq_at_wall,
            )

    # Done
    if router_mode:
        log_load(router_mode=router_mode, event="done", pattern=pattern, extra={"sent": sent})
    if verbose:
        total_elapsed = _to_wall(time.monotonic()) - _to_wall(t0_mono)
        print(f"[LOAD] done. sent total={sent} prompts in {total_elapsed:.1f}s")
