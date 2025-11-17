# lmsys_benchmark_chatbot_arena_conv.py

import asyncio
import time
from redis import asyncio as aioredis

from prefix_aware_router import route_prompt  # replace with your router script import

REDIS_URL = "redis://redis:6379"
MODEL_PATH = "/model/qwen-test"


def extract_user_prompts(ds, limit=1000):
    prompts = []
    for row in ds.select(range(limit)):
        convo_a = row.get("conversation_a", [])
        if not convo_a:
            continue

        first_turn = convo_a[0]
        if isinstance(first_turn, dict):
            if first_turn.get("role") == "user":
                content = first_turn.get("content", "").strip()
                if content:
                    prompts.append(content)
        elif isinstance(first_turn, str):
            # fallback: plain string turn
            prompts.append(first_turn.strip())

    print(f"Extracted {len(prompts)} user prompts")
    return prompts


async def benchmark_router(prompts, model_path):
    redis = aioredis.from_url(REDIS_URL, decode_responses=True)

    total, cache_hits = 0, 0
    latencies = []

    for prompt in prompts:
        start = time.time()
        best_pod = await route_prompt(redis, prompt, model_path)
        latency = time.time() - start
        latencies.append(latency)

        if best_pod and "vllm" in best_pod:
            cache_hits += 1
        total += 1

        if total % 100 == 0:
            print(f"Processed {total} prompts...")

    avg_latency = sum(latencies) / len(latencies)
    print("\n--- Router Benchmark Results ---")
    print(f"Total prompts processed: {total}")
    print(f"Cache hit rate: {cache_hits / total * 100:.2f}%")
    print(f"Average routing latency: {avg_latency:.3f} sec")

    await redis.aclose()


# ------------------------------
# Entry Point
# ------------------------------
if __name__ == "__main__":
    from datasets import load_from_disk

    ds = load_from_disk("/model/chatbot_arena_conversations")
    # ds = load_from_disk("/mnt/storage1/amardeep/chatbot_arena_conversations")

    prompts = extract_user_prompts(ds, limit=500)

    # Run the async function properly
    asyncio.run(benchmark_router(prompts, "/model/qwen-test"))