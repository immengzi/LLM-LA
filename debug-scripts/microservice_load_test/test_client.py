#!/usr/bin/env python3
"""
Multi-threaded test client for kv-router-service WITH FULL RESPONSE LOGGING.

- Sends the exact same payload as your single-threaded client.
- Uses the same /enqueue endpoint.
- Logs every request and response verbosely.
- Allows N threads sending M total requests.

Prompt handling:
- Prompts are loaded from a JSON file (no hard-coded strings).
- The JSON file must contain keys: "short", "medium", "long".
- You select which one to use via --prompt-variant.

Example JSON file (prompts.json):

{
  "short": "Hello, please generate a short response.",
  "medium": "Explain how KV-cache affects routing decisions in a moderate amount of detail.",
  "long": "Provide a very detailed explanation of how KV-cache prefix matching works inside the router, including how block hashes are mapped to endpoints, how prefix continuity is determined, and how this influences warm-start routing behavior in a distributed vLLM deployment."
}
"""

import argparse
import time
import threading
import queue
import json
from typing import Dict, Any

import requests
from requests.exceptions import RequestException


# ------------------------------------------------------------
# Prompt loading
# ------------------------------------------------------------
def load_prompts_from_file(path: str) -> Dict[str, str]:
    with open(path, "r") as f:
        data = json.load(f)

    for key in ("short", "medium", "long"):
        if key not in data:
            raise ValueError(f"JSON file '{path}' is missing required key: '{key}'")

    # Return exactly what is in the file; no formatting placeholders
    return {
        "short": str(data["short"]),
        "medium": str(data["medium"]),
        "long": str(data["long"]),
    }


# ------------------------------------------------------------
# send_one() — same behavior as your single-thread client
# ------------------------------------------------------------
def send_one(
    session: requests.Session,
    router_url: str,
    prompt: str,
    meta: Dict[str, Any] | None = None,
):
    t_enq = time.time()
    payload: Dict[str, Any] = {
        "prompt": prompt,
        "t_enq_client": t_enq,
        "meta": meta or {},
    }

    url = f"{router_url}/enqueue"
    print(f"\n[client] → POST {url}")
    print(f"[client]   payload = {payload}")

    try:
        resp = session.post(url, json=payload, timeout=100.0)
    except RequestException as e:
        print(f"[client] ✗ HTTP error talking to router: {e}")
        raise

    print(f"[client] ← status = {resp.status_code}")
    try:
        print(f"[client] ← body   = {resp.text}")
    except Exception:
        print("[client] ← body   = <decode error>")

    if not resp.ok:
        raise RuntimeError(f"/enqueue failed: {resp.status_code} {resp.text}")

    data = resp.json()
    rid = int(data["req_id"])
    return rid


# ------------------------------------------------------------
# Worker thread
# ------------------------------------------------------------
def worker(
    tid: int,
    router_url: str,
    jobs: queue.Queue,
    max_tokens: int,
    temperature: float,
):
    session = requests.Session()

    while True:
        try:
            i, prompt_template = jobs.get_nowait()
        except queue.Empty:
            break

        # Fixed prompt: no formatting, no {i}
        prompt = prompt_template
        meta = {
            "max_tokens": max_tokens,
            "temperature": temperature,
        }

        print(f"\n[client][T{tid}] sending request {i}")
        try:
            rid = send_one(session, router_url, prompt, meta)
            print(f"[client][T{tid}] ✔ enqueued req_id={rid}")
        except Exception as e:
            print(f"[client][T{tid}] ✗ ERROR req {i}: {e}")

        jobs.task_done()

    session.close()


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--router-url", type=str, default="http://127.0.0.1:30080")
    parser.add_argument("--n", type=int, default=50, help="Total number of requests")
    parser.add_argument("--threads", type=int, default=4, help="Worker threads")

    # JSON-based prompt selection
    parser.add_argument(
        "--prompt-file",
        type=str,
        default="prompts.json",
        # required=True,
        help="Path to JSON file with keys: short, medium, long",
    )
    parser.add_argument(
        "--prompt-variant",
        type=str,
        default="medium",
        choices=["short", "medium", "long"],
        help="Which prompt from the JSON file to use",
    )

    parser.add_argument("--max-tokens", type=int, default=256)
    parser.add_argument("--temperature", type=float, default=0.0)

    args = parser.parse_args()

    # Load prompts from JSON file
    prompts = load_prompts_from_file(args.prompt_file)
    prompt_template = prompts[args.prompt_variant]

    print(
        f"[client] router-url={args.router_url}, "
        f"n={args.n}, threads={args.threads}, "
        f"prompt-file={args.prompt_file}, variant={args.prompt_variant}"
    )

    # Fill job queue
    jobs: queue.Queue = queue.Queue()
    for i in range(args.n):
        jobs.put((i, prompt_template))

    # Launch threads
    threads = []
    t0 = time.time()
    for tid in range(args.threads):
        t = threading.Thread(
            target=worker,
            args=(tid, args.router_url, jobs, args.max_tokens, args.temperature),
            daemon=True,
        )
        t.start()
        threads.append(t)

    # Wait for completion
    jobs.join()
    dt = time.time() - t0
    print(f"\n[client] done. Sent {args.n} requests in {dt:.3f}s")


if __name__ == "__main__":
    main()
