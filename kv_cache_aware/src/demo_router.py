import asyncio
import os
import time
from collections import Counter
from typing import List, Optional
import sys

from redis import asyncio as aioredis

# Import your compute_vllm_block_hashes function
from prefix_hash_estimation import compute_vllm_block_hashes

# ------------------------------
# Redis config
# ------------------------------
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379")
KVBLOCKS_KEY = "Qwen/Qwen3-0.6B:kvblocks"  # Hash: block_hash -> pod:timestamp

# ------------------------------
# Router Logic
# ------------------------------
async def select_best_pod(redis: aioredis.Redis, block_hashes: List[int]) -> Optional[str]:
    """
    Returns the pod with maximum KV cache hits for given block hashes.
    """
    if not block_hashes:
        return None

    # Convert block hashes to strings to match Redis storage
    block_hashes_str = [str(bh) for bh in block_hashes]

    # Fetch pods for each block hash
    pipe = redis.pipeline()
    for bh in block_hashes_str:
        pipe.hget(KVBLOCKS_KEY, bh)
    results = await pipe.execute()

    # Count hits per pod
    counter = Counter()
    for val in results:
        if val:
            pod_name = val.split(":")[0]  # "pod:timestamp"
            counter[pod_name] += 1

    if not counter:
        return None

    best_pod, hits = counter.most_common(1)[0]
    return best_pod

async def route_prompt(redis: aioredis.Redis, prompt_text: str, model_path: str, block_size: int = 128):
    """
    Compute block hashes and select the best pod to route this prompt.
    """
    block_hashes, _ = compute_vllm_block_hashes(prompt_text, model_path, block_size)
    print(f"\nBlock hashes for prompt: {block_hashes}")
    
    best_pod = await select_best_pod(redis, block_hashes)
    if best_pod:
        print(f"Routing prompt '{prompt_text[:50]}...' to pod: {best_pod}")
    else:
        print(f"No cached blocks found for prompt '{prompt_text[:50]}...', route to any pod")
    return best_pod

async def process_prompts(prompts: List[str], model_path: str):
    """
    Demo main loop to route multiple prompts asynchronously.
    """
    redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    tasks = [route_prompt(redis, prompt, model_path) for prompt in prompts]
    results = await asyncio.gather(*tasks)

    print("\n--- Routing Results ---")
    for prompt, pod in zip(prompts, results):
        print(f"Prompt: '{prompt[:50]}...' -> Pod: {pod}")

    await redis.aclose()

# ------------------------------
# Entry Point
# ------------------------------
if __name__ == "__main__":
    # Accept multiple prompts from command-line arguments
    demo_prompts = sys.argv[1:] if len(sys.argv) > 1 else ["Hello world.", "Test prompt for KV cache routing."]
    demo_model_path = "/model/qwen-test"
    
    asyncio.run(process_prompts(demo_prompts, demo_model_path))

    # demo_prompts = [
    #     "Hello world, how are you?",
    #     "Generate a story about dragons",
    #     "Write a python function to sort a list",
    #     "Translate this sentence to French"
    # ]
    # demo_model_path = "/model/qwen-test"

    # asyncio.run(process_prompts(demo_prompts, demo_model_path))
