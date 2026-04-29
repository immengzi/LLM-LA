#!/usr/bin/env python3
"""
Quick routing stress-test through BooM gateway.
Fires concurrent requests and prints per-request latency + routing info.

Usage:
    python route_test.py                        # 10 requests, 4 concurrent
    python route_test.py --num 50 --workers 8   # 50 requests, 8 concurrent
    python route_test.py --base-url http://10.0.0.1:30401
"""

import argparse
import time
import threading
import json
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from collections import Counter

PROMPTS = [
    "Explain how transformers work in deep learning.",
    "What are the trade-offs between TCP and UDP?",
    "Describe the CAP theorem with real-world examples.",
    "How does garbage collection work in Go vs Java?",
    "Write a Python function to find the longest palindrome in a string.",
    "Compare microservice and monolith architectures.",
    "Explain the difference between RDMA and standard TCP networking.",
    "What is expert parallelism in mixture-of-experts models?",
    "How does consistent hashing work in distributed systems?",
    "Describe the Raft consensus algorithm step by step.",
]

results = []
lock = threading.Lock()


def send_request(idx: int, base_url: str, model: str, api_key: str, max_tokens: int):
    prompt = PROMPTS[idx % len(PROMPTS)]
    payload = json.dumps({
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": max_tokens,
        "temperature": 0.7,
    }).encode()

    req = Request(
        f"{base_url}/v1/chat/completions",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
    )

    t0 = time.perf_counter()
    try:
        with urlopen(req, timeout=120) as resp:
            body = json.loads(resp.read())
        elapsed = time.perf_counter() - t0
        tokens = body.get("usage", {}).get("completion_tokens", "?")
        resp_model = body.get("model", "?")
        status = "OK"
    except (HTTPError, URLError, Exception) as e:
        elapsed = time.perf_counter() - t0
        tokens, resp_model = "—", "—"
        status = f"ERR: {e}"

    with lock:
        results.append({"idx": idx, "status": status, "elapsed": elapsed, "tokens": tokens, "model": resp_model})
        tag = f"\033[92mOK\033[0m" if status == "OK" else f"\033[91m{status}\033[0m"
        print(f"  [{idx:3d}] {tag}  {elapsed:6.2f}s  tokens={tokens}  model={resp_model}")


def main():
    parser = argparse.ArgumentParser(description="BooM gateway routing test")
    parser.add_argument("--base-url", default="http://7.216.57.215:30401")
    parser.add_argument("--model", default="served-model")
    parser.add_argument("--api-key", default="sk-boom-master")
    parser.add_argument("--num", type=int, default=10, help="total requests")
    parser.add_argument("--workers", type=int, default=4, help="concurrent workers")
    parser.add_argument("--max-tokens", type=int, default=128)
    args = parser.parse_args()

    print(f"Target:     {args.base_url}")
    print(f"Model:      {args.model}")
    print(f"Requests:   {args.num}")
    print(f"Workers:    {args.workers}")
    print(f"Max tokens: {args.max_tokens}")
    print()

    sem = threading.Semaphore(args.workers)
    threads = []
    t_start = time.perf_counter()

    for i in range(args.num):
        sem.acquire()

        def run(idx=i):
            try:
                send_request(idx, args.base_url, args.model, args.api_key, args.max_tokens)
            finally:
                sem.release()

        t = threading.Thread(target=run)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    wall = time.perf_counter() - t_start

    # Summary
    ok = [r for r in results if r["status"] == "OK"]
    errs = [r for r in results if r["status"] != "OK"]
    latencies = sorted(r["elapsed"] for r in ok)

    print()
    print("=" * 50)
    print(f"  Total:    {len(results)} requests in {wall:.1f}s")
    print(f"  Success:  {len(ok)}   Errors: {len(errs)}")
    if latencies:
        print(f"  Latency:  min={latencies[0]:.2f}s  median={latencies[len(latencies)//2]:.2f}s  max={latencies[-1]:.2f}s")
        total_tokens = sum(r["tokens"] for r in ok if isinstance(r["tokens"], int))
        if total_tokens:
            print(f"  Tokens:   {total_tokens} total  ({total_tokens/wall:.1f} tok/s throughput)")
    if errs:
        err_counts = Counter(r["status"] for r in errs)
        print(f"  Errors:   {dict(err_counts)}")
    print("=" * 50)


if __name__ == "__main__":
    main()
