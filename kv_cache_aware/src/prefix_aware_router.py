import asyncio
import os
import sys
import time
from collections import defaultdict, Counter
from typing import List, Optional, Dict
import httpx
from redis import asyncio as aioredis

from prefix_hash_estimation import compute_vllm_block_hashes

# ------------------------------
# Redis config
# ------------------------------
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379")
MODEL_NAME = os.environ.get("MODEL_NAME", "Qwen/Qwen3-0.6B")

# ------------------------------
# vLLM Pod config
# ------------------------------
PODS = {
    "vllm-1": os.environ.get("VLLM_1_URL", "http://vllm-1:8000/generate"),
    "vllm-2": os.environ.get("VLLM_2_URL", "http://vllm-2:8000/generate"),
}

# ------------------------------
# Strict Prefix-based Scoring
# ------------------------------
async def select_best_pod_strict_prefix(redis: aioredis.Redis, block_hashes: List[int]) -> Optional[str]:
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

    # Traverse blocks in order (prefix means order matters)
    for i, bh in enumerate(block_hashes):
        kvblock_key = f"{MODEL_NAME}:kvblock:{bh}"
        # performs an asynchronous Redis command to fetch an entire hash map stored at key kvblock_key
        pods_with_block = await redis.hgetall(kvblock_key)

        # If no pod has this block — prefix ends for all
        if not pods_with_block:
            break

        # On first block — initialize prefix candidates
        if not initialized:
            active_pods = set(pods_with_block.keys())
            for pod in active_pods:
                prefix_scores[pod] = 1
            initialized = True
            continue

        # For next blocks — only continue counting for pods that still match
        still_matching = set()
        for pod in list(active_pods):
            if pod in pods_with_block:
                prefix_scores[pod] += 1
                still_matching.add(pod)
            # else this pod breaks its prefix chain

        # Update active pods to only those still matching so far
        active_pods = still_matching

        # If all pods lost continuity → stop early
        if not active_pods:
            break

    if not prefix_scores:
        return None

    # Pick the pod with longest contiguous prefix
    best_pod, best_score = max(prefix_scores.items(), key=lambda x: x[1])
    print(f"[Router] Strict prefix scores: {prefix_scores}")
    print(f"[Router] Selected pod: {best_pod} (prefix length={best_score})")
    return best_pod

# ------------------------------
# Prefix-based Scoring
# ------------------------------
async def select_best_pod_prefix_based(redis: aioredis.Redis, block_hashes: List[int]) -> Optional[str]:
    """
    Select the pod with the longest contiguous prefix of cached KV blocks.
    Uses new Redis schema:
        kvblock:<block_hash>  ->  { pod_name: timestamp }
    """
    if not block_hashes:
        return None

    prefix_scores = defaultdict(int)
    current_prefix_hits = defaultdict(int)

    for bh in block_hashes:
        kvblock_key = f"{MODEL_NAME}:kvblock:{bh}"
        pods_with_block = await redis.hgetall(kvblock_key)

        if not pods_with_block:
            # Prefix breaks: reset prefix streaks
            current_prefix_hits.clear()
            continue

        # Extract pod names (ignore timestamps for now)
        available_pods = set(pods_with_block.keys())

        # Update prefix continuity
        for pod in list(current_prefix_hits.keys()):
            if pod not in available_pods:
                del current_prefix_hits[pod]  # break streak

        # Increment prefix for those that continue
        for pod in available_pods:
            current_prefix_hits[pod] = current_prefix_hits.get(pod, 0) + 1
            prefix_scores[pod] = max(prefix_scores[pod], current_prefix_hits[pod])

    if not prefix_scores:
        return None

    # Choose pod with longest prefix; if tie, prefer freshest (based on timestamps)
    best_pod, best_score = max(prefix_scores.items(), key=lambda x: x[1])
    print(f"[Router] Prefix scores: {prefix_scores}")
    print(f"[Router] Selected pod: {best_pod} (prefix length={best_score})")

    return best_pod


# ------------------------------
# Frequency-based Scoring
# ------------------------------
async def select_best_pod(redis: aioredis.Redis, block_hashes: List[int]) -> Optional[str]:
    """
    Returns the pod with the maximum number of cached KV blocks for the given block hashes.
    Compatible with new schema:
        kvblock:<block_hash>  ->  { pod_name: timestamp }
    """
    if not block_hashes:
        return None

    counter = Counter()

    for bh in block_hashes:
        kvblock_key = f"{MODEL_NAME}:kvblock:{bh}"
        pods_with_block = await redis.hgetall(kvblock_key)
        for pod in pods_with_block.keys():
            counter[pod] += 1

    if not counter:
        return None

    best_pod, hits = counter.most_common(1)[0]
    print(f"[Router] Hit counts: {dict(counter)}")
    print(f"[Router] Selected pod: {best_pod} (hits={hits})")
    return best_pod


# ------------------------------
# Forward prompt to pod
# ------------------------------
async def forward_prompt_to_pod(pod_name: str, prompt_text: str, max_tokens: int = 100, temperature: float = 0):
    """
    Forward the prompt to the selected vLLM pod.
    """
    url = PODS.get(pod_name)
    if not url:
        raise ValueError(f"No URL configured for pod {pod_name}")

    payload = {
        "prompt": prompt_text,
        "max_tokens": max_tokens,
        "temperature": temperature,
    }

    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"❌ Failed to forward prompt to {pod_name}: {e}")
            return None


# ------------------------------
# Routing Logic
# ------------------------------
async def route_prompt(redis: aioredis.Redis, prompt_text: str, model_path: str):
    """
    Compute block hashes and route prompt to best pod using prefix scoring.
    """
    block_hashes, _ = compute_vllm_block_hashes(prompt_text, model_path)
    print(f"🧩 Block hashes for prompt: {block_hashes}")

    best_pod = await select_best_pod_strict_prefix(redis, block_hashes)
    #best_pod = await select_best_pod_prefix_based(redis, block_hashes)
    #best_pod = await select_best_pod(redis, block_hashes)

    if not best_pod:
        best_pod = list(PODS.keys())[0]
        print(f"No cached blocks found for prompt '{prompt_text[:30]}...', defaulting to {best_pod}")
    else:
        print(f"Routing prompt '{prompt_text[:30]}...' to pod: {best_pod}")

    response = await forward_prompt_to_pod(best_pod, prompt_text)
    if response:
        print(f"✅ Response from {best_pod}: {response}")
    else:
        print(f"⚠️ No response from {best_pod}")


# ------------------------------
# Process multiple prompts
# ------------------------------
async def process_prompts(prompts: List[str], model_path: str):
    redis = aioredis.from_url(REDIS_URL, decode_responses=True)

    tasks = [route_prompt(redis, prompt, model_path) for prompt in prompts]
    await asyncio.gather(*tasks)

    await redis.aclose()


# ------------------------------
# Entry Point
# ------------------------------
if __name__ == "__main__":
    demo_model_path = "/model/qwen-test"
    demo_prompts = sys.argv[1:] if len(sys.argv) > 1 else [
        "Hello world.",
        "Explain how transformers work in large language models.",
        "Yellow, Dive into the complexities of human communication...",
    ]
    asyncio.run(process_prompts(demo_prompts, demo_model_path))
