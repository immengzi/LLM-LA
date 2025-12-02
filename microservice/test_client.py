#!/usr/bin/env python3
"""
Simple test client for kv-router-service WITH RESPONSE LOGGING.

This version prints:

  - HTTP status code
  - Full response body
  - Any connection errors
"""

import argparse
import time
from typing import Dict, Any

import requests
from requests.exceptions import RequestException


def send_one(
    session: requests.Session,
    router_url: str,
    prompt: str,
    meta: Dict[str, Any] | None = None,
) -> int:
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

    # Log status + body
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--router-url",
        type=str,
        default="http://127.0.0.1:30080",
        help="Base URL of router-service (default: http://127.0.0.1:30080)",
    )
    parser.add_argument(
        "--n",
        type=int,
        default=3,
        help="Number of requests to send",
    )
    parser.add_argument(
        "--prompt",
        type=str,
        default="hello from client #{i}",
        help="Prompt template, {i} will be replaced by index",
    )
    parser.add_argument(
        "--max-tokens",
        type=int,
        default=10,
        help="max_tokens to pass in meta",
    )
    parser.add_argument(
        "--temperature",
        type=float,
        default=0.0,
        help="temperature to pass in meta",
    )

    args = parser.parse_args()

    print(f"[client] router-url={args.router_url}, n={args.n}")
    session = requests.Session()

    try:
        for i in range(args.n):
            prompt = args.prompt.format(i=i)
            meta = {
                "max_tokens": args.max_tokens,
                "temperature": args.temperature,
            }
            rid = send_one(session, args.router_url, prompt, meta=meta)
            print(f"[client] ✔ enqueued req_id={rid}")
    finally:
        session.close()

    print("\n[client] done.")


if __name__ == "__main__":
    main()
