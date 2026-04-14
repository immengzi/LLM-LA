# multi_round_router.py

import asyncio
import json
import os
import sys
from collections import defaultdict, Counter
from typing import List, Optional, Dict

import httpx
from redis import asyncio as aioredis

from prefix_hash_estimation import compute_vllm_block_hashes

# ------------------------------
# Configuration
# ------------------------------
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379")
MODEL_NAME = os.environ.get("MODEL_NAME", "Qwen/Qwen3-0.6B")
MODEL_PATH = os.environ.get("MODEL_PATH", "/model/qwen-test")

PODS = {
    "vllm-1": os.environ.get("VLLM_1_URL", "http://vllm-1:8000"),
    "vllm-2": os.environ.get("VLLM_2_URL", "http://vllm-2:8000"),
}

SYSTEM_PROMPT = "You are a helpful and professional assistant."

# ------------------------------
# Prefix-based Scoring Logic
# ------------------------------
async def select_best_pod_strict_prefix(redis: aioredis.Redis, block_hashes: List[int]) -> Optional[str]:
    if not block_hashes:
        return None

    prefix_scores: Dict[str, int] = {}
    active_pods: set[str] = set()
    initialized = False

    for bh in block_hashes:
        kvblock_key = f"{MODEL_NAME}:kvblock:{bh}"
        pods_with_block = await redis.hgetall(kvblock_key)
        if not pods_with_block:
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

    best_pod, best_score = max(prefix_scores.items(), key=lambda x: x[1])
    print(f"[Router] Strict prefix scores: {prefix_scores}")
    print(f"[Router] Selected pod: {best_pod} (prefix length={best_score})")
    return best_pod


# ------------------------------
# Frequency-based Scoring
# ------------------------------
async def select_best_pod(redis: aioredis.Redis, block_hashes: List[int]) -> Optional[str]:
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
# Forwarding
# ------------------------------
async def forward_prompt_to_pod(pod_name: str, payload: dict, endpoint_suffix: str):
    """
    Send prompt or chat request to vLLM instance.
    """
    url = f"{PODS[pod_name]}{endpoint_suffix}"
    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"Failed to forward to {pod_name}: {e}")
            return None

# ------------------------------
# Single-Shot Routing
# ------------------------------
async def route_prompt(redis: aioredis.Redis, prompt_text: str):
    """
    Single prompt routing (non-conversational).
    """
    block_hashes, _ = compute_vllm_block_hashes(prompt_text, MODEL_PATH)
    print(f"Block hashes: {block_hashes}")

    best_pod = await select_best_pod_strict_prefix(redis, block_hashes)
    if not best_pod:
        best_pod = list(PODS.keys())[0]
        print(f"No cached blocks found, defaulting to {best_pod}")

    payload = {
        "prompt": prompt_text,
        "max_tokens": 100,
        "temperature": 0,
    }

    response = await forward_prompt_to_pod(best_pod, payload, "/generate")
    if response:
        print(f"Response from {best_pod}: {response}")
    else:
        print(f"No response from {best_pod}")


# ------------------------------
# Multi-Round Conversation Routing
# ------------------------------
async def route_conversation_turn(redis: aioredis.Redis, session_id: str, user_input: str):
    """
    Handle one conversational turn with prefix-based routing.
    Keeps conversation history in Redis.
    """
    session_key = f"{MODEL_NAME}:session:{session_id}"
    session_data = await redis.get(session_key)
    history = json.loads(session_data) if session_data else []

    # Build full prompt text (simple concatenation of dialogue)
    conversation_context = ""
    for msg in history:
        role = msg["role"]
        conversation_context += f"{role.upper()}: {msg['content']}\n"
    conversation_context += f"USER: {user_input}\nASSISTANT:"

    # Compute block hashes for prefix-aware routing
    block_hashes, _ = compute_vllm_block_hashes(conversation_context, MODEL_PATH)
    best_pod = await select_best_pod_strict_prefix(redis, block_hashes)
    if not best_pod:
        best_pod = list(PODS.keys())[0]

    print(f"[Conversation] Routing session '{session_id}' → {best_pod}")

    payload = {
        "prompt": conversation_context,
        "max_tokens": 150,
        "temperature": 0.7,
    }

    response = await forward_prompt_to_pod(best_pod, payload, "/generate")

    if response and "text" in response:
        # Handle both list and string responses
        if isinstance(response["text"], list):
            assistant_text = " ".join(response["text"]).strip()
        else:
            assistant_text = str(response["text"]).strip()

        print(f"Assistant reply: {assistant_text[:100]}...")

        new_history = history + [
            {"role": "user", "content": user_input},
            {"role": "assistant", "content": assistant_text},
        ]
        await redis.set(session_key, json.dumps(new_history))
    else:
        print(f"[Conversation] No valid response for session {session_id}: {response}")



# ------------------------------
# Entry Points
# ------------------------------
async def process_single_prompts(prompts: List[str]):
    redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    for prompt in prompts:
        await route_prompt(redis, prompt)
    await redis.aclose()


async def process_multi_round_session(session_id: str, user_turns: List[str]):
    redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    for i, msg in enumerate(user_turns, 1):
        print(f"\n[Turn {i}] {msg}")
        await route_conversation_turn(redis, session_id, msg)
        await asyncio.sleep(1)
    await redis.aclose()


# ------------------------------
# Main CLI Entrypoint
# ------------------------------
if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "single"

    if mode == "single":
        demo_prompts = [
            "Hello world.",
            "Explain how transformers work in large language models.",
            "Describe quantum entanglement simply.",
        ]
        asyncio.run(process_single_prompts(demo_prompts))

    elif mode == "multi":
        session_turns = [
            "Ahoy! Why would ye be the perfect mate for my ship?",
            "Aye, but what be the name of the kraken guarding the treasure?",
            "Suppose we’re stranded—how do we survive with coconuts and rum?",
            "What’s the most important rule in the Pirate’s Code?",
        ]
        asyncio.run(process_multi_round_session("pirate_demo", session_turns))
