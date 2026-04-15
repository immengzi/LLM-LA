# sim/schedule.py
from __future__ import annotations
from typing import List, Optional, Tuple
import math
import random


def _emit_even_points_in_window(sec_start: float, sec_len: float, n: int) -> List[float]:
    if n <= 0 or sec_len <= 0:
        return []
    step = sec_len / n
    return [sec_start + (k + 0.5) * step for k in range(n)]


def _build_schedule_dump(t0: float, items: int) -> List[float]:
    return [t0] * items


def _build_schedule_per_second_even(start: float, duration_s: float, rps_int: int, max_items: int) -> List[float]:
    times: List[float] = []
    if duration_s <= 0 or max_items <= 0 or rps_int <= 0:
        return times

    end = start + duration_s
    sec_start = start
    while sec_start < end and len(times) < max_items:
        sec_len = min(1.0, end - sec_start)
        n = int(round(rps_int * sec_len))
        chunk = _emit_even_points_in_window(sec_start, sec_len, n)
        for ts in chunk:
            times.append(ts)
            if len(times) >= max_items:
                break
        sec_start += sec_len
    return times


def _build_schedule_poisson(start: float, duration_s: float, rate_rps: float, max_items: int, rnd: random.Random) -> List[float]:
    times: List[float] = []
    if duration_s <= 0 or max_items <= 0 or rate_rps <= 0:
        return times

    end = start + duration_s
    t = start
    while len(times) < max_items:
        u = rnd.random()
        if u <= 0.0:
            continue
        delta = -math.log(u) / rate_rps
        t += delta
        if t >= end:
            break
        times.append(t)
    return times


def _parse_steps(spec: str) -> List[Tuple[float, float]]:
    out: List[Tuple[float, float]] = []
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


def _build_schedule_steps_even(start: float, duration_s: float, steps: List[Tuple[float, float]], max_items: int) -> List[float]:
    times: List[float] = []
    if duration_s <= 0 or max_items <= 0 or not steps:
        return times

    end = start + duration_s
    segs: List[Tuple[float, float, int]] = []
    for i, (off, rps) in enumerate(steps):
        seg_start = start + off
        seg_end = start + (steps[i + 1][0] if i + 1 < len(steps) else duration_s)
        seg_end = min(seg_end, end)
        if seg_end <= seg_start:
            continue
        segs.append((seg_start, seg_end, int(round(max(0.0, rps)))))

    t = start
    while t < end and len(times) < max_items:
        sec_end = min(t + 1.0, end)
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
            if len(times) >= max_items:
                break
        t = sec_end
    return times


def _build_schedule_bursty_even(start: float, duration_s: float, on_s: float, off_s: float,
                               rps_on: float, rps_off: float, max_items: int) -> List[float]:
    times: List[float] = []
    if duration_s <= 0 or max_items <= 0:
        return times

    end = start + duration_s
    t = start
    cur_on = True
    window_end = t + (on_s if cur_on else off_s)
    rps_on_i = int(round(max(0.0, rps_on)))
    rps_off_i = int(round(max(0.0, rps_off)))

    while t < end and len(times) < max_items:
        sec_end = min(t + 1.0, end)
        remain = sec_end - t
        while remain > 0 and len(times) < max_items:
            sub_end = min(window_end, sec_end)
            sub_len = sub_end - t
            rps_int = rps_on_i if cur_on else rps_off_i
            n = int(round(rps_int * sub_len))
            chunk = _emit_even_points_in_window(t, sub_len, n)
            for ts in chunk:
                times.append(ts)
                if len(times) >= max_items:
                    break
            t = sub_end
            remain = sec_end - t
            if t >= window_end:
                cur_on = not cur_on
                window_end = t + (on_s if cur_on else off_s)
    return times


def _build_schedule_rand_even(start: float, duration_s: float, lo: int, hi: int, epoch_s: float,
                             rnd: random.Random, max_items: int) -> List[float]:
    times: List[float] = []
    if duration_s <= 0 or max_items <= 0 or lo < 0 or hi < 0 or hi < lo:
        return times

    end = start + duration_s
    t_epoch = start

    while t_epoch < end and len(times) < max_items:
        cur_rps = rnd.randint(int(lo), int(hi))
        epoch_end = min(t_epoch + epoch_s, end)
        t = t_epoch
        while t < epoch_end and len(times) < max_items:
            sec_end = min(t + 1.0, epoch_end)
            n = int(round(cur_rps * (sec_end - t)))
            chunk = _emit_even_points_in_window(t, sec_end - t, n)
            for ts in chunk:
                times.append(ts)
                if len(times) >= max_items:
                    break
            t = sec_end
        t_epoch += epoch_s

    return times


def build_schedule_virtual(
    *,
    pattern: str,
    total_items: int,
    rate_rps: float,
    duration_s: float,
    burst_on_s: float,
    burst_off_s: float,
    burst_rps_on: float,
    burst_rps_off: float,
    step_schedule: str,
    rand_rps_min: Optional[float],
    rand_rps_max: Optional[float],
    rand_epoch_s: float,
    seed: int,
) -> List[float]:
    if total_items <= 0:
        return []
    rnd = random.Random(seed)
    t0 = 0.0
    start = 0.0

    if pattern == "dump":
        plan = _build_schedule_dump(t0, total_items)
    elif pattern == "det":
        rps_i = int(round(max(0.0, rate_rps)))
        plan = _build_schedule_per_second_even(start, max(0.0, duration_s), rps_i, total_items)
    elif pattern == "poisson":
        plan = _build_schedule_poisson(start, max(0.0, duration_s), float(rate_rps), total_items, rnd)
    elif pattern == "bursty":
        plan = _build_schedule_bursty_even(start, max(0.0, duration_s),
                                           burst_on_s, burst_off_s,
                                           burst_rps_on, burst_rps_off,
                                           total_items)
    elif pattern == "steps":
        steps = _parse_steps(step_schedule) or [(0.0, rate_rps)]
        plan = _build_schedule_steps_even(start, max(0.0, duration_s), steps, total_items)
    elif pattern == "rand":
        lo = int(rand_rps_min if rand_rps_min is not None else max(0.0, min(rate_rps, burst_rps_off)))
        hi = int(rand_rps_max if rand_rps_max is not None else max(rate_rps, burst_rps_on, lo))
        epoch = max(0.1, float(rand_epoch_s))
        plan = _build_schedule_rand_even(start, max(0.0, duration_s), lo, hi, epoch, rnd, total_items)
    else:
        raise ValueError(f"Unknown pattern '{pattern}'")

    return plan[:total_items]
