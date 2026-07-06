#!/usr/bin/env python3
# verify_prefix_routing.py
#
# Validation for the targeted KV prefix-routing fix (bz cluster).
#
# It reproduces the "section 7" scenario deterministically: several requests
# that share a LONG leading prefix but differ in the TAIL of the first user
# message. Because the affinity key is derived from the whole first user
# message, each request gets a DIFFERENT affinity key -- so affinity alone
# cannot group them. With the fix (ROUTER_STRATEGY=both + KV_OWNER_SOURCE=lookup)
# the router resolves the shared leading blocks from Redis and should:
#   - report matched_tokens > 0 / kv_hit=true for the shared prefix, and
#   - land the requests on the same endpoint as the warm-up.
#
# How it decides: after a warm-up request populates the engine cache (and thus
# Redis ownership), it sends N test requests and reads the router's
# /latency_log ring, correlating by time window + large prompt size.
#
# Usage:
#   python3 verify_prefix_routing.py \
#       --chat-url  http://<node>:<gw_port>/v1/chat/completions \
#       --router-url http://<node>:<router_port> \
#       --model served-model-minmax \
#       --api-key  "<key>" \
#       --prefix-tokens 20000 --n 6
#
# Exit code 0 = PASS (prefix reuse observed), 1 = FAIL, 2 = error.

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.request
import urllib.error
from typing import Any, Dict, List, Optional


def _http_json(url: str, *, payload: Optional[dict] = None, api_key: str = "",
               timeout: float = 300.0) -> Any:
    data = None
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers,
                                 method="POST" if data else "GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", errors="replace")
    return json.loads(body) if body.strip() else {}


def _shared_prefix(approx_tokens: int) -> str:
    # ~4 chars/token heuristic; deterministic so every request tokenizes to the
    # exact same leading blocks (identical block hashes).
    sentence = ("The quick brown fox jumps over the lazy dog while the "
                "engineers review the distributed KV cache routing design. ")
    target_chars = max(1, approx_tokens) * 4
    reps = (target_chars // len(sentence)) + 1
    return (sentence * reps)[:target_chars]


def _send(chat_url: str, model: str, api_key: str, prefix: str, tail: str) -> float:
    """Send one chat request; return client-side wall time it was sent."""
    payload = {
        "model": model,
        # Big shared context first, unique short question last -> shared leading
        # blocks, but a different affinity key per request.
        "messages": [
            {"role": "system", "content": "You are a concise assistant."},
            {"role": "user", "content": prefix + "\n\nQuestion: " + tail},
        ],
        "max_tokens": 8,
        "temperature": 0.0,
        "stream": False,
    }
    t = time.time()
    try:
        _http_json(chat_url, payload=payload, api_key=api_key)
    except urllib.error.HTTPError as e:
        print(f"  [warn] HTTP {e.code} on tail={tail!r}: {e.read()[:200]!r}")
    except Exception as e:
        print(f"  [warn] request failed tail={tail!r}: {e}")
    return t


def main() -> int:
    ap = argparse.ArgumentParser(description="Validate targeted KV prefix routing.")
    ap.add_argument("--chat-url", required=True,
                    help="OpenAI-compatible chat completions URL (gateway/router).")
    ap.add_argument("--router-url", required=True,
                    help="Router base URL exposing /latency_log (e.g. http://node:8080).")
    ap.add_argument("--model", required=True)
    ap.add_argument("--api-key", default="")
    ap.add_argument("--prefix-tokens", type=int, default=20000,
                    help="Approx shared leading-prefix size in tokens (default 20000).")
    ap.add_argument("--n", type=int, default=6, help="Number of test requests.")
    ap.add_argument("--warm-wait", type=float, default=3.0,
                    help="Seconds to wait after warm-up for KV events -> Redis.")
    ap.add_argument("--min-prompt-tokens", type=int, default=0,
                    help="Filter latency_log to entries at least this large "
                         "(default: 0.5 * prefix-tokens).")
    args = ap.parse_args()

    prefix = _shared_prefix(args.prefix_tokens)
    min_pt = args.min_prompt_tokens or int(args.prefix_tokens * 0.5)

    print(f"[verify] shared prefix ~{args.prefix_tokens} tokens "
          f"({len(prefix)} chars); sending 1 warm-up + {args.n} test requests")

    # 1) Warm-up: same shared prefix, distinct tail -> populates engine + Redis.
    t_warm = _send(args.chat_url, args.model, args.api_key, prefix, "warmup-0")
    print(f"[verify] warm-up sent; waiting {args.warm_wait}s for KV events -> Redis")
    time.sleep(args.warm_wait)

    # 2) Test burst: each a DIFFERENT affinity key (different tail), SAME prefix.
    t_start = time.time()
    for i in range(args.n):
        _send(args.chat_url, args.model, args.api_key, prefix, f"test-question-{i}")
    t_end = time.time()

    # 3) Read the router latency ring and correlate by time window + size.
    time.sleep(1.0)
    try:
        records: List[Dict[str, Any]] = _http_json(
            args.router_url.rstrip("/") + "/latency_log?last=2000", api_key=args.api_key
        )
    except Exception as e:
        print(f"[verify] ERROR reading /latency_log: {e}")
        return 2

    def _in_window(r: Dict[str, Any]) -> bool:
        t0 = r.get("t0_wall")
        if t0 is None:
            return True  # keep if we can't tell; size filter still applies
        return (t_start - 2.0) <= float(t0) <= (t_end + 30.0)

    test_recs = [
        r for r in records
        if _in_window(r) and int(r.get("prompt_tokens") or 0) >= min_pt
    ]

    if not test_recs:
        print("[verify] FAIL: no matching large-prompt records in /latency_log "
              "(check --chat-url/--router-url, model, and that ROUTER_LOG "
              "captured them).")
        return 1

    hits = [r for r in test_recs if bool(r.get("kv_hit"))]
    matched = [r for r in test_recs if int(r.get("matched_tokens") or 0) > 0]
    eps = {}
    for r in test_recs:
        ep = r.get("endpoint_id") or r.get("endpoint")
        eps[ep] = eps.get(ep, 0) + 1

    mt_vals = sorted(int(r.get("matched_tokens") or 0) for r in test_recs)
    print(f"\n[verify] matched {len(test_recs)} large-prompt requests "
          f"(prompt_tokens >= {min_pt})")
    print(f"  kv_hit=true            : {len(hits)}/{len(test_recs)} "
          f"({len(hits)/len(test_recs):.0%})")
    print(f"  matched_tokens>0       : {len(matched)}/{len(test_recs)} "
          f"({len(matched)/len(test_recs):.0%})")
    if mt_vals:
        print(f"  matched_tokens min/med/max: {mt_vals[0]} / "
              f"{mt_vals[len(mt_vals)//2]} / {mt_vals[-1]}")
    print(f"  endpoint distribution  : {eps}")

    # Verdict: with the fix, the shared prefix should be credited on a clear
    # majority of the test requests.
    ok = len(matched) >= max(1, (len(test_recs) + 1) // 2)
    print("\n[verify] " + ("PASS: shared prefix is being credited by the router "
                           "(targeted lookup working)."
                           if ok else
                           "FAIL: shared prefix not credited -- check "
                           "ROUTER_STRATEGY=both, KV_OWNER_SOURCE=lookup, "
                           "MODEL_NAME/Redis prefix, and PYTHONHASHSEED/KV_BLOCK_SIZE."))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
