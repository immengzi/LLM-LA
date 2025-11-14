import asyncio
import time
from datasets import load_dataset
from transformers import AutoTokenizer
from redis import asyncio as aioredis

from prefix_aware_router import route_prompt  # import your routing logic

# ------------------------------
# Config
# ------------------------------
REDIS_URL = "redis://redis:6379"
MODEL_PATH = "/model/qwen-test"
HF_MODEL_NAME = "Qwen/Qwen3-0.6B"

# ------------------------------
# Extract prompts from LMSYS Chat-1M
# ------------------------------
def extract_user_prompts(ds, limit=500):
    prompts = []
    for row in ds.select(range(min(limit, len(ds)))):
        conv = row.get("conversation", [])
        if not isinstance(conv, list) or len(conv) < 2:
            continue

        last_user, last_assistant = None, None
        for msg in conv:
            role = msg.get("role", "").lower()
            text = msg.get("content", "").strip()
            if role == "user":
                last_user = text
            elif role == "assistant" and last_user:
                last_assistant = text
                break  # take first user→assistant pair

        if not last_user or not last_assistant:
            continue

        prompts.append(last_user)

    print(f"Extracted {len(prompts)} user prompts from LMSYS Chat-1M")
    return prompts

# ------------------------------
# Benchmark prefix-aware router
# ------------------------------
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
# Entry point
# ------------------------------
if __name__ == "__main__":
    print("Loading LMSYS Chat-1M subset...")
    ds = load_dataset(
        "/model/datasets--lmsys--lmsys-chat-1m",
        split="train[:1000]",  # small subset for benchmark
    )

    print("=== Dataset Schema ===")
    print(f"Columns: {ds.column_names}")
    print(f"Total rows in full train split: {ds.info.splits['train'].num_examples}")
    print(f"Sample keys: {list(ds[0].keys())}")
    print("======================================")

    tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH, trust_remote_code=True)

    prompts = extract_user_prompts(ds, limit=500)

    # Optional sanity check: token lengths
    token_counts = [len(tokenizer.tokenize(p)) for p in prompts[:5]]
    print(f"Example prompt token counts: {token_counts}")

    # Run benchmark
    asyncio.run(benchmark_router(prompts, MODEL_PATH))
