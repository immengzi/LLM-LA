#!/usr/bin/env python3
"""Concurrent /chat/completions microbench (LiteLLM scripts/benchmark_mock.py style).

Use against an already-running gateway (e.g. after SKIP_LOCUST=1 ./scripts/run_path.sh ...).

  python scripts/benchmark_mock.py --host http://127.0.0.1:14000 --requests 2000 --max-concurrent 200 --runs 3
"""
from __future__ import annotations

import argparse
import asyncio
import statistics
import time
import uuid
from typing import List, Optional, Tuple

import aiohttp


async def one(
    session: aiohttp.ClientSession,
    url: str,
    model: str,
    api_key: str,
    max_tokens: int,
) -> Tuple[float, Optional[float], int]:
    payload = {
        "model": model,
        "messages": [
            {
                "role": "user",
                "content": f"{uuid.uuid4()} ping " * 20,
            }
        ],
        "max_tokens": max_tokens,
        "temperature": 0,
    }
    headers = {"Authorization": f"Bearer {api_key}"}
    t0 = time.perf_counter()
    async with session.post(url, json=payload, headers=headers) as resp:
        await resp.read()
        status = resp.status
        overhead = None
        for key in (
            "x-litellm-overhead-duration-ms",
            "x-gateway-overhead-duration-ms",
        ):
            if key in resp.headers:
                try:
                    overhead = float(resp.headers[key])
                except ValueError:
                    pass
                break
        if overhead is None and "x-mock-backend-duration-ms" in resp.headers:
            try:
                backend = float(resp.headers["x-mock-backend-duration-ms"])
                overhead = max(0.0, (time.perf_counter() - t0) * 1000.0 - backend)
            except ValueError:
                pass
        if status >= 400:
            raise RuntimeError(f"HTTP {status}")
        return (time.perf_counter() - t0) * 1000.0, overhead, status


async def run_batch(
    host: str,
    model: str,
    api_key: str,
    n: int,
    concurrency: int,
    max_tokens: int,
) -> Tuple[List[float], List[float]]:
    url = host.rstrip("/") + "/v1/chat/completions"
    sem = asyncio.Semaphore(concurrency)
    latencies: List[float] = []
    overheads: List[float] = []

    async with aiohttp.ClientSession() as session:

        async def _wrapped() -> None:
            async with sem:
                ms, ov, _ = await one(session, url, model, api_key, max_tokens)
                latencies.append(ms)
                if ov is not None:
                    overheads.append(ov)

        await asyncio.gather(*[_wrapped() for _ in range(n)])
    return latencies, overheads


def pct(xs: List[float], p: float) -> float:
    if not xs:
        return float("nan")
    ys = sorted(xs)
    idx = min(len(ys) - 1, max(0, int(round((p / 100.0) * (len(ys) - 1)))))
    return ys[idx]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="http://127.0.0.1:14000")
    ap.add_argument("--model", default="served-model")
    ap.add_argument("--api-key", default="sk-1234")
    ap.add_argument("--requests", type=int, default=2000)
    ap.add_argument("--max-concurrent", type=int, default=200)
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--max-tokens", type=int, default=16)
    args = ap.parse_args()

    for run in range(1, args.runs + 1):
        t0 = time.perf_counter()
        lats, ovs = asyncio.run(
            run_batch(
                args.host,
                args.model,
                args.api_key,
                args.requests,
                args.max_concurrent,
                args.max_tokens,
            )
        )
        wall = time.perf_counter() - t0
        rps = len(lats) / wall if wall > 0 else 0.0
        print(f"=== run {run}/{args.runs} ===")
        print(
            f"/chat/completions  n={len(lats)}  "
            f"median={pct(lats,50):.2f}  p95={pct(lats,95):.2f}  "
            f"p99={pct(lats,99):.2f}  avg={statistics.mean(lats):.2f}  "
            f"rps={rps:.1f}"
        )
        if ovs:
            print(
                f"overhead (ms)      n={len(ovs)}  "
                f"median={pct(ovs,50):.2f}  p95={pct(ovs,95):.2f}  "
                f"p99={pct(ovs,99):.2f}  avg={statistics.mean(ovs):.2f}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
