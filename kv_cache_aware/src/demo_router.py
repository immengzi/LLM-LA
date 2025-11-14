# demo_router.py

import asyncio
import os
import sys
import time
from collections import Counter, defaultdict
from typing import List, Optional, Dict
from redis import asyncio as aioredis

from prefix_hash_estimation import compute_vllm_block_hashes

# ------------------------------
# Redis Config
# ------------------------------
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379")
MODEL_NAME = os.environ.get("MODEL_NAME", "Qwen/Qwen3-0.6B")

# ------------------------------
# Redis Schema
# ------------------------------
# Each block hash key:
#   HGETALL Qwen/Qwen3-0.6B:kvblock:<block_hash>  → { "vllm-1": "timestamp", "vllm-2": "timestamp" }


# ------------------------------
# Scoring Algorithms
# ------------------------------
async def select_best_pod(redis: aioredis.Redis, block_hashes: List[int]) -> Optional[str]:
    """
    Returns the pod with the most KV cache hits across all given block hashes.
    Uses new schema: kvblock:<block_hash> -> { pod_name: timestamp }
    """
    if not block_hashes:
        return None

    counter = Counter()

    for bh in block_hashes:
        kvblock_key = f"{MODEL_NAME}:kvblock:{bh}"
        pods_with_block = await redis.hgetall(kvblock_key)
        for pod_name in pods_with_block.keys():
            counter[pod_name] += 1

    if not counter:
        return None

    best_pod, hits = counter.most_common(1)[0]
    print(f"  [Score] Hit counts: {dict(counter)} → best={best_pod} ({hits})")
    return best_pod

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

async def select_best_pod_prefix_based(redis: aioredis.Redis, block_hashes: List[int]) -> Optional[str]:
    """
    Returns the pod with the longest contiguous prefix match in cached KV blocks.
    Uses: kvblock:<block_hash> -> { pod_name: timestamp }
    """
    if not block_hashes:
        return None

    prefix_scores = defaultdict(int)
    current_prefix_hits = defaultdict(int)

    for bh in block_hashes:
        kvblock_key = f"{MODEL_NAME}:kvblock:{bh}"
        pods_with_block = await redis.hgetall(kvblock_key)

        if not pods_with_block:
            # Prefix breaks
            current_prefix_hits.clear()
            continue

        available_pods = set(pods_with_block.keys())

        # Continue existing streaks
        for pod in list(current_prefix_hits.keys()):
            if pod not in available_pods:
                del current_prefix_hits[pod]

        # Increment for pods that have this block
        for pod in available_pods:
            current_prefix_hits[pod] = current_prefix_hits.get(pod, 0) + 1
            prefix_scores[pod] = max(prefix_scores[pod], current_prefix_hits[pod])

    if not prefix_scores:
        return None

    best_pod, best_prefix = max(prefix_scores.items(), key=lambda x: x[1])
    print(f"  [Prefix] Scores: {dict(prefix_scores)} → best={best_pod} (len={best_prefix})")
    return best_pod


# ------------------------------
# Routing Function
# ------------------------------
async def route_prompt(redis: aioredis.Redis, prompt_text: str, model_path: str, block_size: int = 128):
    """
    Compute block hashes for a prompt and decide which pod to route to.
    """
    block_hashes, _ = compute_vllm_block_hashes(prompt_text, model_path, block_size)
    print(f"\n🔹 Prompt: '{prompt_text[:60]}...'")
    print(f"   Block hashes: {block_hashes}")

    # strict prefix-based first
    best_pod = await select_best_pod_strict_prefix(redis, block_hashes)
    # prefix-based first
    # best_pod = await select_best_pod_prefix_based(redis, block_hashes)
    # best_pod = await select_best_pod(redis, block_hashes)
    # if not best_pod:
    #     # Fallback: overall frequency
    #     best_pod = await select_best_pod(redis, block_hashes)

    if best_pod:
        print(f"Routed to pod: {best_pod}\n")
    else:
        print(f"No cached match, can route to any pod.\n")

    return best_pod


# ------------------------------
# Demo Runner
# ------------------------------
async def process_prompts(prompts: List[str], model_path: str):
    redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    print(f"Connected to Redis: {REDIS_URL}")

    start = time.time()
    results = await asyncio.gather(*[route_prompt(redis, p, model_path) for p in prompts])
    print(f"\n⏱ Completed in {time.time() - start:.2f}s")

    print("\n--- Routing Summary ---")
    for prompt, pod in zip(prompts, results):
        print(f"  '{prompt[:50]}...' → {pod}")

    await redis.aclose()

# ------------------------------
# Entry Point
# ------------------------------
if __name__ == "__main__":
    demo_model_path = "/model/qwen-test"
    demo_prompts = sys.argv[1:] if len(sys.argv) > 1 else [
        "Hello world.",
        "Dive into the complexities of human communication...",
        "Explain how transformers work in large language models.",
        "Translate this sentence into French.",
        "Generate a story about dragons and knights.",
    ]

    asyncio.run(process_prompts(demo_prompts, demo_model_path))
