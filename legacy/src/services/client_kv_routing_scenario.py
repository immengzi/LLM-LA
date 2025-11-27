#!/usr/bin/env python3
# client_kv_routing_scenario.py
"""
KV-aware routing demo with Kubernetes pod discovery.

Scenario:
  - Discover vLLM pods via K8s (namespace + label selector).
  - Warmup:
      * Send dummy requests to all pods for a few rounds to warm the system
        (NPU, caches, Python, etc.), but NOT used for KV-routing stats.
  - Round 1 (cold KV):
      * KV empty (for our real prompts)
      * Randomly choose a pod per prompt, send request, measure latency.
  - Round 2 (warm KV-aware):
      * For same prompts, compute KV block hashes via CPU hash service.
      * Use Redis KV state + strict-prefix routing to pick the best pod.
      * Send request directly to that pod (via pod IP), measure latency.
  - Print cold vs warm latency comparison with full logging, including
    approximate prefill/decode token counts.

Multi-round per prompt:
  - Controlled by --multi-round-depth N (default: 1).
  - For each prompt:
      * Round 1 uses the base prompt.
      * Each subsequent round appends the prior user+assistant turns
        as plain text and continues.
  - Summary reports the LAST round latency per prompt.
"""

# -------------------------------------------------------------------
# Determinism: set hash seed before anything else
# -------------------------------------------------------------------
import os
os.environ["PYTHONHASHSEED"] = "0"

import asyncio
import json
import random
import time
from typing import Dict, List, Tuple, Optional

import click
import httpx
import redis.asyncio as aioredis
from kubernetes import client as k8s_client, config as k8s_config

# Deterministic RNG for routing choices, etc.
random.seed(0)

# -------------------------------------------------------------------
# Config / environment
# -------------------------------------------------------------------

RUNNING_IN_CLUSTER = os.getenv("KUBERNETES_SERVICE_HOST") is not None

if RUNNING_IN_CLUSTER:
    DEFAULT_HASH_SERVICE_URL = "http://vllm-cpu-hash.vllm.svc.cluster.local:9095/compute_hashes"
    DEFAULT_REDIS_HOST = "redis.vllm.svc.cluster.local"
    DEFAULT_REDIS_PORT = "6379"
else:
    DEFAULT_REDIS_HOST = "127.0.0.1"
    DEFAULT_REDIS_PORT = "30079"
    DEFAULT_HASH_SERVICE_URL = "http://127.0.0.1:30095/compute_hashes"

HASH_SERVICE_URL = os.getenv("HASH_SERVICE_URL", DEFAULT_HASH_SERVICE_URL)
REDIS_HOST = os.getenv("REDIS_HOST", DEFAULT_REDIS_HOST)
REDIS_PORT = int(os.getenv("REDIS_PORT", DEFAULT_REDIS_PORT))
MODEL_NAME = os.getenv("MODEL_NAME", "served-model")

K8S_NAMESPACE = os.getenv("VLLM_NAMESPACE", "vllm")
K8S_LABEL_SELECTOR = os.getenv("VLLM_LABEL_SELECTOR", "app=vllm-qwen")
VLLM_PORT = int(os.getenv("VLLM_PORT", "8200"))

PROMPTS_FILE = os.getenv("PROMPTS_FILE", "prompts_long.json")

BASE_PROMPTS: List[str] = [
    "Explain how transformers work in large language models.",
    "Summarize the benefits of KV cache in LLM inference.",
    "Describe the difference between tensor parallelism and data parallelism.",
]


def load_prompts(path: str) -> List[str]:
    print(f"📥 Loading prompts from {path} ...")
    if not os.path.exists(path):
        print(f"⚠ PROMPTS_FILE {path!r} not found, using fallback.")
        return BASE_PROMPTS
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        prompts = data.get("prompts", BASE_PROMPTS)
        print(f"📄 Loaded {len(prompts)} prompts.")
        return prompts
    except Exception as e:
        print(f"⚠ Failed to read prompt file: {e}, using fallback prompts.")
        return BASE_PROMPTS


PROMPTS = load_prompts(PROMPTS_FILE)


# -------------------------------------------------------------------
# Kubernetes discovery
# -------------------------------------------------------------------

def discover_vllm_pods() -> Dict[str, str]:
    try:
        if RUNNING_IN_CLUSTER:
            k8s_config.load_incluster_config()
        else:
            k8s_config.load_kube_config()
    except Exception as e:
        raise RuntimeError(f"Failed to load Kubernetes config: {e}")

    v1 = k8s_client.CoreV1Api()
    pods = v1.list_namespaced_pod(
        namespace=K8S_NAMESPACE,
        label_selector=K8S_LABEL_SELECTOR,
    ).items

    endpoints: Dict[str, str] = {}
    for pod in pods:
        if pod.status.phase == "Running" and pod.status.pod_ip:
            endpoints[pod.metadata.name] = pod.status.pod_ip
    return endpoints


# -------------------------------------------------------------------
# HTTP helpers
# -------------------------------------------------------------------

def build_vllm_url_for_pod(pod_ip: str) -> str:
    return f"http://{pod_ip}:{VLLM_PORT}/v1/chat/completions"


async def send_prompt(
    client: httpx.AsyncClient,
    pod: str,
    pod_ip: str,
    prompt: str,
    model: str,
) -> Tuple[float, dict, int]:
    """
    Send prompt to a pod (via IP) and measure latency.

    Returns:
        latency_seconds, response_json, decode_tokens_approx
    """
    print(f"         Sending to pod {pod} at {pod_ip}")
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 128,
        "temperature": 0.2,
    }
    t0 = time.monotonic()
    resp = await client.post(build_vllm_url_for_pod(pod_ip), json=payload)
    dt = time.monotonic() - t0
    resp.raise_for_status()
    result = resp.json()

    # Approximate decode tokens by splitting on whitespace.
    # (Not exact tokenizer-based tokens, but good enough for ratio insight.)
    try:
        decoded_text = result["choices"][0]["message"]["content"]
        decode_tokens_approx = len(decoded_text.split())
    except Exception:
        decode_tokens_approx = -1

    print(f"         Latency: {dt:.3f} s | decode_tokens_approx={decode_tokens_approx}")
    return dt, result, decode_tokens_approx


async def fetch_block_hashes(prompt: str) -> Tuple[List[int], List[int]]:
    """
    Ask the CPU hash service for KV block hashes and token IDs
    for the given prompt.
    """
    print("         Requesting block hashes from hash service…")
    async with httpx.AsyncClient(timeout=60.0) as client:
        r = await client.post(
            HASH_SERVICE_URL,
            json={"messages": [{"role": "user", "content": prompt}]},
        )
        r.raise_for_status()
        data = r.json()
        return data["block_hashes"], data["token_ids"]


# -------------------------------------------------------------------
# KV prefix routing
# -------------------------------------------------------------------

async def strict_prefix_route(
    redis,
    block_hashes: List[int],
    model: str,
) -> Optional[Tuple[str, Dict[str, int]]]:
    """
    Strict prefix-based pod selection:

    - Prefix must start from the first block.
    - Once a pod misses a block, it stops accumulating prefix length permanently.
    """
    if not block_hashes:
        return None

    prefix_scores: Dict[str, int] = {}
    active_pods: set[str] = set()
    initialized = False

    for bh in block_hashes:
        print(f"         Checking Redis for block hash: {bh}")
        kv_key = f"{model}:kvblock:{bh}"
        pods_for_block = await redis.hgetall(kv_key)
        print(f"           Redis mapping: {pods_for_block}")

        if not pods_for_block:
            print("           No pods carry this block, stop.")
            break

        if not initialized:
            active_pods = set(pods_for_block.keys())
            prefix_scores = {pod: 1 for pod in active_pods}
            initialized = True
            continue

        still_matching = {pod for pod in active_pods if pod in pods_for_block}
        for pod in still_matching:
            prefix_scores[pod] += 1

        active_pods = still_matching
        if not active_pods:
            break

    if not prefix_scores:
        return None

    best = max(prefix_scores, key=lambda p: prefix_scores[p])
    return best, prefix_scores


async def debug_dump_redis(redis, model: str, max_keys: int = 40) -> None:
    print("\n🧩 Redis KV debug dump:")
    count = 0
    async for key in redis.scan_iter(match=f"{model}:kvblock:*"):
        mapping = await redis.hgetall(key)
        print(f"  - {key} -> {mapping}")
        count += 1
        if count >= max_keys:
            break
    if count == 0:
        print("  (no kvblock keys found)")


# -------------------------------------------------------------------
# Warmup helper (system warm, not KV warm)
# -------------------------------------------------------------------

async def warmup_pods(
    client: httpx.AsyncClient,
    endpoints: Dict[str, str],
    model: str,
    rounds: int = 2,
) -> None:
    """
    Send a short dummy prompt to each pod for a few rounds to warm up:
    - NPU / GPU kernels
    - Python internals
    - Model weights in memory

    This does NOT factor into our cold/warm KV measurements.
    """
    if not endpoints:
        return

    pod_names = sorted(endpoints.keys())
    dummy_prompt = "Warmup: short generic request to stabilize system performance."

    print(f"\n🔥 Warmup phase: {rounds} rounds over {len(pod_names)} pods")
    for r in range(rounds):
        print(f"  Warmup round {r+1}/{rounds}")
        for pod in pod_names:
            pod_ip = endpoints[pod]
            # Ignore results, we only care about warming things up
            try:
                await send_prompt(client, pod, pod_ip, dummy_prompt, model)
            except Exception as e:
                print(f"    ⚠ Warmup request failed for {pod}: {e}")


# -------------------------------------------------------------------
# Main experiment (async core)
# -------------------------------------------------------------------

async def run_experiment(multi_round_depth: int) -> None:
    if multi_round_depth < 1:
        raise ValueError("--multi-round-depth must be >= 1")

    print(f"\n🚀 Starting KV routing experiment")
    print(f"Redis: {REDIS_HOST}:{REDIS_PORT}")
    print(f"Hash Service: {HASH_SERVICE_URL}")
    print(f"Prompts loaded: {len(PROMPTS)}")
    print(f"Multi-round depth per prompt: {multi_round_depth}\n")

    endpoints = discover_vllm_pods()
    if not endpoints:
        print("❌ No vLLM pods discovered, exiting.")
        return

    print("\n🔍 Discovered endpoints:")
    for name, ip in endpoints.items():
        print(f"  {name} @ {ip}")

    # Stable ordering for determinism
    pod_names = sorted(endpoints.keys())

    redis = aioredis.from_url(
        f"redis://{REDIS_HOST}:{REDIS_PORT}",
        decode_responses=True,
    )

    cold: Dict[str, float] = {}
    warm: Dict[str, float] = {}
    chosen: Dict[str, str] = {}

    # For token statistics (record last round)
    prefill_tokens_map: Dict[str, int] = {}
    decode_cold: Dict[str, int] = {}
    decode_warm: Dict[str, int] = {}

    async with httpx.AsyncClient(timeout=60.0) as client:
        # ------------------------------------------------------------
        # Warmup phase (system warm, not KV warm)
        # ------------------------------------------------------------
        await warmup_pods(client, endpoints, MODEL_NAME, rounds=2)

        # ------------------------------------------------------------
        # Round 1: "Cold" KV for our real prompts (multi-round per prompt)
        # ------------------------------------------------------------
        print("\n=== ROUND 1: COLD ROUTING (KV empty for these prompts) ===")
        for prompt in PROMPTS:
            print(f"\n  [COLD] Base prompt length: {len(prompt)} chars")
            conversation_history = ""
            last_dt = -1.0
            last_dec_tok = -1

            for r in range(multi_round_depth):
                print(f"    → Cold round {r+1}/{multi_round_depth}")
                if conversation_history:
                    full_prompt = conversation_history + "\nUser: " + prompt
                else:
                    full_prompt = prompt

                pod = random.choice(pod_names)
                dt, resp, dec_tok = await send_prompt(
                    client,
                    pod,
                    endpoints[pod],
                    full_prompt,
                    MODEL_NAME,
                )

                last_dt = dt
                last_dec_tok = dec_tok

                # Build up per-prompt conversation history
                try:
                    assistant_text = resp["choices"][0]["message"]["content"]
                except Exception:
                    assistant_text = "(no reply)"
                conversation_history += ("" if not conversation_history else "") + f"User: {prompt}\nAssistant: {assistant_text}\n"

            # Record LAST round stats for summary
            cold[prompt] = last_dt
            decode_cold[prompt] = last_dec_tok

        print("\n⏱ Wait 5 seconds for KV event ingestion...\n")
        await asyncio.sleep(5)
        await debug_dump_redis(redis, MODEL_NAME)

        # ------------------------------------------------------------
        # Round 2: Warm KV-aware routing (multi-round per prompt)
        # ------------------------------------------------------------
        print("\n=== ROUND 2: WARM ROUTING (prefix matching, multi-round per prompt) ===")
        for prompt in PROMPTS:
            print(f"\n  [WARM] Base prompt length: {len(prompt)} chars")
            conversation_history = ""
            last_dt = -1.0
            last_dec_tok = -1
            last_prefill = -1
            last_pod = "(n/a)"

            for r in range(multi_round_depth):
                print(f"    → Warm round {r+1}/{multi_round_depth}")
                if conversation_history:
                    full_prompt = conversation_history + "\nUser: " + prompt
                else:
                    full_prompt = prompt

                block_hashes, toks = await fetch_block_hashes(full_prompt)
                prefill_tokens = len(toks)
                last_prefill = prefill_tokens

                print(f"         Block hashes ({len(block_hashes)} blocks): {block_hashes}")
                print(f"         Prefill tokens: {prefill_tokens}")

                route = await strict_prefix_route(redis, block_hashes, MODEL_NAME)

                if route:
                    pod, scores = route
                    print(f"         KV HIT — best pod: {pod}")
                    print(f"         Prefix scores: {scores}")
                else:
                    pod = random.choice(pod_names)
                    print("         KV MISS — using random pod")

                dt, resp, dec_tok = await send_prompt(
                    client,
                    pod,
                    endpoints[pod],
                    full_prompt,
                    MODEL_NAME,
                )

                last_dt = dt
                last_dec_tok = dec_tok
                last_pod = pod

                # Build up per-prompt conversation history
                try:
                    assistant_text = resp["choices"][0]["message"]["content"]
                except Exception:
                    assistant_text = "(no reply)"
                conversation_history += ("" if not conversation_history else "") + f"User: {prompt}\nAssistant: {assistant_text}\n"

            warm[prompt] = last_dt
            chosen[prompt] = last_pod
            decode_warm[prompt] = last_dec_tok
            prefill_tokens_map[prompt] = last_prefill

    # ------------------------------------------------------------
    # Summary (per prompt, last round)
    # ------------------------------------------------------------
    print("\n==============================")
    print(f" SUMMARY: cold vs warm latencies (last round, depth={multi_round_depth})")
    print("==============================\n")
    print(
        f"{'Prompt':40} | {'Cold':>8} | {'Warm':>8} | {'Δ':>8} | "
        f"{'Prefill':>7} | {'DecCold':>7} | {'DecWarm':>7} | Pod"
    )
    print("-" * 120)
    for prompt in PROMPTS:
        delta = cold[prompt] - warm[prompt]
        short = (prompt[:37] + "...") if len(prompt) > 40 else prompt
        prefill = prefill_tokens_map.get(prompt, -1)
        dc = decode_cold.get(prompt, -1)
        dw = decode_warm.get(prompt, -1)
        print(
            f"{short:40} | {cold[prompt]:8.3f} | {warm[prompt]:8.3f} | {delta:8.3f} | "
            f"{prefill:7d} | {dc:7d} | {dw:7d} | {chosen.get(prompt, '(n/a)')}"
        )

    print("\nDone.\n")


# -------------------------------------------------------------------
# CLI entrypoint (Click + asyncio)
# -------------------------------------------------------------------

@click.command()
@click.option(
    "--multi-round-depth",
    default=1,
    show_default=True,
    type=int,
    help="Number of conversational rounds per prompt (>=1). 1 = single shot (original behavior).",
)
def main(multi_round_depth: int) -> None:
    asyncio.run(run_experiment(multi_round_depth))


if __name__ == "__main__":
    main()
