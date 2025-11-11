import asyncio
import os
import sys
from collections import Counter
from typing import List, Optional
import httpx

from redis import asyncio as aioredis

# Import your vLLM block hash computation function
from prefix_hash_estimation import compute_vllm_block_hashes

# ------------------------------
# Redis config
# ------------------------------
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379")
MODEL_NAME = os.environ.get("MODEL_NAME", "Qwen/Qwen3-0.6B")
KVBLOCKS_KEY = f"{MODEL_NAME}:kvblocks"  # Namespaced key

# ------------------------------
# vLLM Pod config
# ------------------------------
PODS = {
    "vllm-1": os.environ.get("VLLM_1_URL", "http://vllm-1:8000/generate"),
    "vllm-2": os.environ.get("VLLM_2_URL", "http://vllm-2:8000/generate"),
}


# ------------------------------
# Router Logic
# ------------------------------
async def select_best_pod(redis: aioredis.Redis, block_hashes: List[int]) -> Optional[str]:
    """
    Returns the pod with maximum KV cache hits for given block hashes.
    """
    if not block_hashes:
        return None

    pipe = redis.pipeline()
    for bh in block_hashes:
        pipe.hget(KVBLOCKS_KEY, str(bh))
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
        "temperature": temperature
    }

    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"❌ Failed to forward prompt to {pod_name}: {e}")
            return None


async def route_prompt(redis: aioredis.Redis, prompt_text: str, model_path: str):
    """
    Compute block hashes and route prompt to best pod.
    """
    block_hashes, _ = compute_vllm_block_hashes(prompt_text, model_path)
    print(f" block hashes: {block_hashes}")

    best_pod = await select_best_pod(redis, block_hashes)
    if not best_pod:
        # Default to first pod if no cache hits
        best_pod = list(PODS.keys())[0]
        print(f"No cached blocks found for prompt '{prompt_text[:30]}...', defaulting to {best_pod}")

    else:
        print(f"Routing prompt '{prompt_text[:30]}...' to pod: {best_pod}")

    response = await forward_prompt_to_pod(best_pod, prompt_text)
    if response:
        print(f"✅ Response from {best_pod}: {response}")
    else:
        print(f"⚠️ No response from {best_pod}")


async def process_prompts(prompts: List[str], model_path: str):
    """
    Route multiple prompts asynchronously.
    """
    redis = aioredis.from_url(REDIS_URL, decode_responses=True)

    tasks = [route_prompt(redis, prompt, model_path) for prompt in prompts]
    await asyncio.gather(*tasks)

    await redis.aclose()


# ------------------------------
# Entry Point
# ------------------------------
if __name__ == "__main__":
    demo_model_path = "/model/qwen-test"
    demo_prompts = sys.argv[1:] if len(sys.argv) > 1 else ["Hello world.", "Yellow, Dive into the complexities of human communication..."]
    asyncio.run(process_prompts(demo_prompts, demo_model_path))
