#!/usr/bin/env python3
"""
KV-aware routing demo with Kubernetes pod discovery.

Scenario:
  - Discover vLLM pods via K8s (namespace + label selector).
  - Round 1 (cold):
      * KV empty
      * Randomly choose a pod per prompt, send request, measure latency.
  - Round 2 (warm):
      * For same prompts, compute KV block hashes via CPU hash service.
      * Use Redis KV state + strict-prefix routing to pick the best pod.
      * Send request directly to that pod (via pod IP), measure latency.
  - Print cold vs warm latency comparison.

Dependencies:
  pip install httpx "redis[hiredis]" kubernetes
"""

import asyncio
import os
import random
import time
from typing import Dict, List, Optional, Tuple

import httpx
import redis.asyncio as aioredis
from kubernetes import client as k8s_client, config as k8s_config

# -------------------------------------------------------------------
# Config (these can stay as defaults; no pod list env needed)
# -------------------------------------------------------------------

HASH_SERVICE_URL = os.getenv(
    "HASH_SERVICE_URL",
    "http://vllm-cpu-hash.vllm.svc.cluster.local:9095/compute_hashes",
)

REDIS_HOST = os.getenv("REDIS_HOST", "redis.vllm.svc.cluster.local")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

MODEL_NAME = os.getenv("MODEL_NAME", "served-model")

K8S_NAMESPACE = os.getenv("VLLM_NAMESPACE", "vllm")
K8S_LABEL_SELECTOR = os.getenv("VLLM_LABEL_SELECTOR", "app=vllm-qwen")
VLLM_PORT = int(os.getenv("VLLM_PORT", "8200"))

PROMPTS: List[str] = [
    "Explain how transformers work in large language models.",
    "Summarize the benefits of KV cache in LLM inference.",
    "Describe the difference between tensor parallelism and data parallelism.",
]


# -------------------------------------------------------------------
# Kubernetes discovery (similar spirit to your discover_endpoints)
# -------------------------------------------------------------------

def discover_vllm_pods() -> Dict[str, str]:
    """
    Discover vLLM pods via Kubernetes API and return:
        { pod_name: pod_ip }

    - Uses in-cluster config if available, otherwise local kubeconfig.
    - Filters by namespace + label selector.
    - Only includes pods in Running phase with a pod IP.
    """
    try:
        # In-cluster if possible
        if os.getenv("KUBERNETES_SERVICE_HOST"):
            k8s_config.load_incluster_config()
        else:
            k8s_config.load_kube_config()
    except Exception as e:
        raise RuntimeError(f"Failed to load Kubernetes config: {e}") from e

    v1 = k8s_client.CoreV1Api()
    pods = v1.list_namespaced_pod(
        namespace=K8S_NAMESPACE,
        label_selector=K8S_LABEL_SELECTOR,
    ).items

    endpoints: Dict[str, str] = {}
    for pod in pods:
        phase = pod.status.phase
        ip = pod.status.pod_ip
        name = pod.metadata.name
        if phase == "Running" and ip:
            endpoints[name] = ip

    return endpoints


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

def build_vllm_url_for_pod(pod_ip: str) -> str:
    """
    Build the target URL for a vLLM pod by IP.
    """
    base = f"http://{pod_ip}:{VLLM_PORT}"
    return f"{base}/v1/chat/completions"


async def send_prompt(
    client: httpx.AsyncClient,
    pod_name: str,
    pod_ip: str,
    prompt: str,
    model_name: str,
) -> Tuple[float, dict]:
    """
    Send prompt to a pod (via IP) and measure latency.

    Returns:
        latency_seconds, response_json
    """
    url = build_vllm_url_for_pod(pod_ip)
    payload = {
        "model": model_name,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 128,
        "temperature": 0.2,
    }
    t0 = time.monotonic()
    resp = await client.post(url, json=payload)
    dt = time.monotonic() - t0
    resp.raise_for_status()
    return dt, resp.json()


async def fetch_block_hashes(prompt: str) -> Tuple[List[int], List[int]]:
    """
    Call the CPU hash service for block hashes + token ids.
    """
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(
            HASH_SERVICE_URL,
            json={"prompt": prompt},
        )
        r.raise_for_status()
        data = r.json()
        return data["block_hashes"], data["token_ids"]


async def strict_prefix_route(
    redis,
    block_hashes: List[int],
    model_name: str,
) -> Optional[Tuple[str, Dict[str, int]]]:
    """
    Strict prefix-based pod selection:

    - Prefix must start from the first block.
    - Once a pod misses a block, it stops accumulating prefix length permanently.
    """
    if not block_hashes:
        return None

    key_prefix = f"{model_name}:"
    prefix_scores: Dict[str, int] = {}
    active_pods: set[str] = set()
    initialized = False

    for bh in block_hashes:
        kvblock_key = f"{key_prefix}kvblock:{bh}"
        pods_with_block = await redis.hgetall(kvblock_key)

        if not pods_with_block:
            # no pod has this block -> prefix breaks
            break

        if not initialized:
            active_pods = set(pods_with_block.keys())
            for pod in active_pods:
                prefix_scores[pod] = 1
            initialized = True
            continue

        still_matching = set()
        for pod in list(active_pods):
            if pod in pods_with_block:
                prefix_scores[pod] += 1
                still_matching.add(pod)
        active_pods = still_matching

        if not active_pods:
            break

    if not prefix_scores:
        return None

    best_pod, _score = max(prefix_scores.items(), key=lambda x: x[1])
    return best_pod, prefix_scores


# -------------------------------------------------------------------
# Scenario: Round 1 (cold) then Round 2 (warm KV-aware)
# -------------------------------------------------------------------

async def main():
    # 1) Discover vLLM pods via K8s
    print("\n🔍 Discovering vLLM pods via Kubernetes...")
    endpoints = discover_vllm_pods()  # { pod_name: pod_ip }
    if not endpoints:
        print("❗ No running vLLM pods found with selector:", K8S_LABEL_SELECTOR)
        return

    pod_names = list(endpoints.keys())
    print(f"Found {len(pod_names)} pods:")
    for name, ip in endpoints.items():
        print(f"  - {name} @ {ip}")
    print()

    redis = aioredis.from_url(
        f"redis://{REDIS_HOST}:{REDIS_PORT}",
        decode_responses=True,
    )

    cold_latencies: Dict[str, float] = {}
    warm_latencies: Dict[str, float] = {}
    chosen_pods_warm: Dict[str, str] = {}

    async with httpx.AsyncClient(timeout=60.0) as client:
        # ------------------------------------------------------------
        # Round 1: Cold cache — random routing
        # ------------------------------------------------------------
        print("▶ ROUND 1: cold cache, random pod routing\n")

        for prompt in PROMPTS:
            pod = random.choice(pod_names)
            pod_ip = endpoints[pod]
            print(f"  [COLD] Prompt: {prompt!r}")
            print(f"         Sending to random pod: {pod} ({pod_ip})")
            dt, _resp = await send_prompt(client, pod, pod_ip, prompt, MODEL_NAME)
            cold_latencies[prompt] = dt
            print(f"         Latency: {dt:.3f} s\n")

        # Give some time for KV events to be processed & written to Redis
        print("⏱ Waiting a bit for KV events to propagate into Redis...\n")
        await asyncio.sleep(5.0)

        # Optional: show how many kvblock keys exist now
        key_prefix = f"{MODEL_NAME}:kvblock:"
        count_kvblocks = 0
        async for key in redis.scan_iter(match=f"{key_prefix}*"):
            count_kvblocks += 1
            if count_kvblocks >= 10:
                break
        print(f"📦 Detected at least {count_kvblocks} KV block keys after warm-up.\n")

        # ------------------------------------------------------------
        # Round 2: Warm cache — KV-aware routing (strict prefix)
        # ------------------------------------------------------------
        print("▶ ROUND 2: warm cache, KV-aware routing\n")

        for prompt in PROMPTS:
            print(f"  [WARM] Prompt: {prompt!r}")

            block_hashes, token_ids = await fetch_block_hashes(prompt)
            print(f"         Block hashes: {block_hashes}")
            print(f"         Num tokens: {len(token_ids)}")

            route_result = await strict_prefix_route(redis, block_hashes, MODEL_NAME)

            if route_result is None:
                # Fallback: random pod, same as cold
                pod = random.choice(pod_names)
                print("         No prefix match (cache miss).")
                print(f"         Fallback random pod: {pod}")
            else:
                pod, scores = route_result
                chosen_pods_warm[prompt] = pod
                print(f"         KV HIT — best pod: {pod}")
                print(f"         Prefix scores: {scores}")

            pod_ip = endpoints.get(pod)
            if not pod_ip:
                print(f"         ⚠ Pod {pod} not in discovered endpoints (maybe restarted). Picking random.")
                pod = random.choice(pod_names)
                pod_ip = endpoints[pod]

            dt, _resp = await send_prompt(client, pod, pod_ip, prompt, MODEL_NAME)
            warm_latencies[prompt] = dt
            print(f"         Latency: {dt:.3f} s\n")

    await redis.aclose()

    # ------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------
    print("\n==============================")
    print(" SUMMARY: cold vs warm latencies")
    print("==============================\n")
    print(f"{'Prompt':40} | {'Cold (s)':>9} | {'Warm (s)':>9} | {'Delta (s)':>9} | Pod (warm)")
    print("-" * 90)
    for prompt in PROMPTS:
        cold = cold_latencies.get(prompt, float('nan'))
        warm = warm_latencies.get(prompt, float('nan'))
        delta = cold - warm
        pod = chosen_pods_warm.get(prompt, "(random)")
        short_prompt = (prompt[:37] + "...") if len(prompt) > 40 else prompt
        print(f"{short_prompt:40} | {cold:9.3f} | {warm:9.3f} | {delta:9.3f} | {pod}")

    print("\n(Positive Delta means warm routing was faster than cold.)")


if __name__ == "__main__":
    asyncio.run(main())
