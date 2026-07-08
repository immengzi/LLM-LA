#!/usr/bin/env python3
"""
claude_load_test.py
~~~~~~~~~~~~~~~~~~~~~
Standalone load generator that drives the real ``claude`` CLI (Claude Code) to
put load on whatever backend Claude Code is pointed at (your router via
ANTHROPIC_BASE_URL). It simulates multiple concurrent *users*, each holding a
*multi-turn* conversation, so the growing prompt prefix exercises the router's
conversation affinity and KV-prefix reuse -- exactly the traffic shape you see
from real Claude Code sessions.

How it maps to the router
-------------------------
* Each simulated user = one Claude Code session with a *unique first user
  message* -> a distinct affinity key -> pinned to one vLLM pod (hard affinity).
* Each user runs N sequential turns via ``--resume <session_id>``; every turn
  replays the growing conversation, so turn 2+ should hit the prefix cache.
* Users run concurrently (that's the "load"); --users and --rounds/--think-time
  shape the offered load.

Watch it land with, in another terminal:
    python debug/verify_redis_blocks.py --router-url http://<host>:30080 -n vllm

Safety
------
Tools are DISABLED by default (``--tools ""``) so the model only produces text
-- no file edits, no shell, no permission prompts. Pass ``--enable-tools`` to
run realistic tool-calling traffic (arbitrary commands may execute; use only in
a sandbox).

Requires: the ``claude`` CLI on PATH, already authenticated and pointed at your
backend (same env your interactive Claude Code uses). Override the endpoint with
--base-url / --model if needed.

Examples (run from the repo root, e.g. python debug/claude_load_test.py ...)
--------
    # 4 users, 3 turns each, one round:
    python debug/claude_load_test.py --users 4 --turns 3

    # Heavier sustained load: 16 users, 5 turns, 3 rounds, 1s ramp between users:
    python debug/claude_load_test.py --users 16 --turns 5 --rounds 3 --ramp 1.0

    # Point at a specific router/model and save raw per-turn results:
    python debug/claude_load_test.py --users 8 --base-url http://192.168.0.79:30080 \
        --model served-model-minmax --output /tmp/load_results.jsonl
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

DEFAULT_PROMPTS = [
    "In one short paragraph, explain what a KV cache is in LLM inference.",
    "Now give a concrete numeric example of the memory it saves.",
    "What are two common eviction policies for it?",
    "Summarize everything you said so far in three bullet points.",
    "What is one common pitfall when tuning it?",
    "Rewrite your last answer for a beginner audience.",
]


@dataclass
class TurnResult:
    user: int
    round_idx: int
    turn: int
    ok: bool
    wall_s: float
    duration_ms: Optional[float] = None
    api_ms: Optional[float] = None
    input_tokens: Optional[int] = None
    output_tokens: Optional[int] = None
    cache_read_tokens: Optional[int] = None
    cache_creation_tokens: Optional[int] = None
    session_id: Optional[str] = None
    error: Optional[str] = None


@dataclass
class Stats:
    results: List[TurnResult] = field(default_factory=list)


def build_claude_cmd(args, prompt: str, session_id: Optional[str],
                     assign_session: Optional[str]) -> List[str]:
    cmd = [args.claude_bin, "-p", prompt, "--output-format", "json"]
    if session_id:
        cmd += ["--resume", session_id]
    elif assign_session:
        cmd += ["--session-id", assign_session]
    if args.model:
        cmd += ["--model", args.model]
    if args.bare:
        cmd += ["--bare"]
    if not args.enable_tools:
        cmd += ["--tools", ""]
    else:
        cmd += ["--dangerously-skip-permissions"]
    if args.extra_args:
        cmd += args.extra_args
    return cmd


async def run_turn(args, env, user: int, round_idx: int, turn: int,
                   prompt: str, session_id: Optional[str],
                   assign_session: Optional[str]) -> TurnResult:
    cmd = build_claude_cmd(args, prompt, session_id, assign_session)
    t0 = time.time()
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            env=env, cwd=args.workdir,
        )
        try:
            out, err = await asyncio.wait_for(proc.communicate(), timeout=args.timeout)
        except asyncio.TimeoutError:
            proc.kill()
            return TurnResult(user, round_idx, turn, False, time.time() - t0,
                              error=f"timeout>{args.timeout}s")
    except FileNotFoundError:
        return TurnResult(user, round_idx, turn, False, time.time() - t0,
                          error=f"claude binary not found: {args.claude_bin}")

    wall = time.time() - t0
    if proc.returncode != 0:
        msg = (err.decode("utf-8", "replace").strip() or f"exit {proc.returncode}")[:200]
        return TurnResult(user, round_idx, turn, False, wall, error=msg)

    try:
        data = json.loads(out.decode("utf-8", "replace"))
    except json.JSONDecodeError:
        return TurnResult(user, round_idx, turn, False, wall,
                          error="non-JSON output")

    if isinstance(data, list):  # stream-json fallback: take last object
        data = data[-1] if data else {}
    usage = data.get("usage") or {}
    r = TurnResult(
        user=user, round_idx=round_idx, turn=turn,
        ok=not data.get("is_error", False),
        wall_s=wall,
        duration_ms=data.get("duration_ms"),
        api_ms=data.get("duration_api_ms"),
        input_tokens=usage.get("input_tokens"),
        output_tokens=usage.get("output_tokens"),
        cache_read_tokens=usage.get("cache_read_input_tokens"),
        cache_creation_tokens=usage.get("cache_creation_input_tokens"),
        session_id=data.get("session_id"),
    )
    if not r.ok:
        r.error = str(data.get("result") or data.get("subtype") or "is_error")[:200]
    return r


async def run_user(args, env, user: int, prompts: List[str], stats: Stats,
                   out_fh, print_lock: asyncio.Lock):
    if args.ramp > 0:
        await asyncio.sleep(user * args.ramp)
    for round_idx in range(args.rounds):
        session_id: Optional[str] = None
        # Unique opening per (user, round) => distinct affinity key / pod pin.
        tag = f"[loadtest u{user} r{round_idx}] "
        for turn in range(args.turns):
            base = prompts[turn % len(prompts)]
            prompt = (tag + base) if turn == 0 else base
            res = await run_turn(args, env, user, round_idx, turn, prompt,
                                 session_id, assign_session=None)
            if res.session_id:
                session_id = res.session_id  # chain subsequent turns
            stats.results.append(res)
            async with print_lock:
                _print_turn(res)
                if out_fh:
                    out_fh.write(json.dumps(res.__dict__) + "\n")
                    out_fh.flush()
            if not res.ok and args.stop_on_error:
                return
            if args.think_time > 0 and turn < args.turns - 1:
                await asyncio.sleep(args.think_time)


def _print_turn(r: TurnResult):
    if r.ok:
        cr = r.cache_read_tokens
        cache = f" cache_read={cr}" if cr is not None else ""
        api = f" api={r.api_ms:.0f}ms" if r.api_ms else ""
        print(f"[u{r.user} r{r.round_idx} t{r.turn}] OK wall={r.wall_s:.2f}s{api} "
              f"in={r.input_tokens} out={r.output_tokens}{cache}", flush=True)
    else:
        print(f"[u{r.user} r{r.round_idx} t{r.turn}] FAIL wall={r.wall_s:.2f}s "
              f"err={r.error}", flush=True)


def _pct(vals: List[float], p: float) -> float:
    if not vals:
        return 0.0
    vals = sorted(vals)
    k = max(0, min(len(vals) - 1, int(round((p / 100.0) * (len(vals) - 1)))))
    return vals[k]


def print_summary(stats: Stats, wall_total: float, args):
    res = stats.results
    ok = [r for r in res if r.ok]
    fail = [r for r in res if not r.ok]
    walls = [r.wall_s for r in ok]
    print("\n" + "=" * 60)
    print(f"[summary] users={args.users} turns={args.turns} rounds={args.rounds} "
          f"concurrency~={args.users}")
    print(f"[summary] requests={len(res)} ok={len(ok)} fail={len(fail)} "
          f"wall_total={wall_total:.1f}s")
    if walls:
        print(f"[summary] latency wall s: mean={statistics.mean(walls):.2f} "
              f"p50={_pct(walls,50):.2f} p90={_pct(walls,90):.2f} "
              f"p99={_pct(walls,99):.2f} max={max(walls):.2f}")
        print(f"[summary] throughput: {len(ok)/wall_total:.2f} req/s")
    in_tok = sum(r.input_tokens or 0 for r in ok)
    cr_tok = sum(r.cache_read_tokens or 0 for r in ok)
    if in_tok:
        print(f"[summary] tokens: input={in_tok} output={sum(r.output_tokens or 0 for r in ok)} "
              f"cache_read={cr_tok} ({100*cr_tok//max(in_tok,1)}% of input from cache)")
    if fail:
        errs: Dict[str, int] = {}
        for r in fail:
            errs[r.error or "?"] = errs.get(r.error or "?", 0) + 1
        print("[summary] errors:")
        for e, c in sorted(errs.items(), key=lambda x: -x[1])[:5]:
            print(f"           {c}x  {e}")
    print("=" * 60)


async def amain(args):
    env = dict(os.environ)
    if args.base_url:
        env["ANTHROPIC_BASE_URL"] = args.base_url

    prompts = DEFAULT_PROMPTS
    if args.prompt_file:
        with open(args.prompt_file, "r", encoding="utf-8") as f:
            fp = [ln.strip() for ln in f if ln.strip()]
        if fp:
            prompts = fp
    elif args.prompt:
        prompts = [args.prompt]

    stats = Stats()
    out_fh = open(args.output, "w", encoding="utf-8") if args.output else None
    print_lock = asyncio.Lock()

    print(f"[load] starting {args.users} users x {args.turns} turns x "
          f"{args.rounds} rounds (tools={'on' if args.enable_tools else 'off'}, "
          f"model={args.model or 'inherit'}, base_url={args.base_url or 'inherit'})")
    t0 = time.time()
    tasks = [
        asyncio.create_task(run_user(args, env, u, prompts, stats, out_fh, print_lock))
        for u in range(args.users)
    ]
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        for t in tasks:
            t.cancel()
    wall_total = time.time() - t0
    if out_fh:
        out_fh.close()
    print_summary(stats, wall_total, args)
    return 1 if any(not r.ok for r in stats.results) else 0


def main():
    ap = argparse.ArgumentParser(
        description="Load-test a Claude Code backend by driving the real claude CLI "
                    "with many concurrent multi-turn users.")
    load = ap.add_argument_group("load shape")
    load.add_argument("--users", type=int, default=4, help="Concurrent users (default: 4).")
    load.add_argument("--turns", type=int, default=3,
                      help="Sequential turns per user conversation (default: 3).")
    load.add_argument("--rounds", type=int, default=1,
                      help="Repeat the whole user set this many times, fresh sessions "
                           "each round (default: 1).")
    load.add_argument("--ramp", type=float, default=0.0,
                      help="Stagger each user's start by user_index * ramp seconds.")
    load.add_argument("--think-time", type=float, default=0.0,
                      help="Delay between turns within a user (seconds).")

    prm = ap.add_argument_group("prompts")
    prm.add_argument("--prompt", help="Single prompt used for every turn.")
    prm.add_argument("--prompt-file", help="File with one prompt per line (cycled per turn).")

    tgt = ap.add_argument_group("target / cli")
    tgt.add_argument("--claude-bin", default="claude", help="claude binary (default: claude).")
    tgt.add_argument("--model", default=None, help="Model alias/name passed to --model.")
    tgt.add_argument("--base-url", default=None,
                     help="Sets ANTHROPIC_BASE_URL for child processes (else inherit env).")
    tgt.add_argument("--bare", action="store_true",
                     help="Pass --bare (skip hooks/auto-memory/CLAUDE.md for reproducibility).")
    tgt.add_argument("--enable-tools", action="store_true",
                     help="Allow tool use (adds --dangerously-skip-permissions). "
                          "May execute commands -- sandbox only.")
    tgt.add_argument("--extra-args", nargs=argparse.REMAINDER,
                     help="Everything after this is passed through to claude verbatim.")
    tgt.add_argument("--workdir", default=None, help="cwd for claude processes.")

    beh = ap.add_argument_group("behavior")
    beh.add_argument("--timeout", type=float, default=300.0,
                     help="Per-turn timeout seconds (default: 300).")
    beh.add_argument("--stop-on-error", action="store_true",
                     help="Stop a user's conversation on first failed turn.")
    beh.add_argument("--output", help="Write per-turn results as NDJSON to this path.")
    args = ap.parse_args()

    try:
        rc = asyncio.run(amain(args))
    except KeyboardInterrupt:
        print("\n[load] interrupted.")
        rc = 130
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
