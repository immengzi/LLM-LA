import argparse
import random
import sys
import redis
import xxhash
from transformers import AutoTokenizer
import struct

# --- Configuration ---
# CRITICAL: This MUST match the server's block size, which is 128 according to your logs.
BLOCK_SIZE = 128

TOKENIZER_PATH = "/mnt/storage1/amardeep/qwen-test"
MODEL_NAME_IN_PAYLOAD = "qwen-test"

REDIS_HOST = "localhost"
REDIS_PORT = 6379

VLLM_ENDPOINTS = {
    "vllm-1": "http://localhost:8001/generate",
    "vllm-2": "http://localhost:8002/generate",
}

def compute_block_hash(token_ids: list[int]) -> int:
    """
    Computes the block hash by packing the list of integers into a byte string.
    """
    token_bytes = struct.pack(f"<{len(token_ids)}I", *token_ids)
    digest = xxhash.xxh64_digest(token_bytes, seed=0)
    block_hash = int.from_bytes(digest, 'little', signed=True)
    return block_hash

def main():
    parser = argparse.ArgumentParser(description="Finds the optimal vLLM endpoint based on prefix cache.")
    parser.add_argument("prompt", type=str, help="The prompt to check for in the cache.")
    args = parser.parse_args()
    prompt = args.prompt

    print(f"--- Cache-Aware Routing Helper ---", file=sys.stderr)

    try:
        print(f"Loading local tokenizer from '{TOKENIZER_PATH}'...", file=sys.stderr)
        tokenizer = AutoTokenizer.from_pretrained(TOKENIZER_PATH, trust_remote_code=True)
        redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
        redis_client.ping()
    except Exception as e:
        print(f"Error during initialization: {e}", file=sys.stderr)
        sys.exit(1)

    token_ids = tokenizer.encode(prompt)
    num_tokens = len(token_ids)

    print(f"Prompt: '{prompt[0:50]}...'", file=sys.stderr)
    print(f"Total tokens in prompt: {num_tokens}", file=sys.stderr)
    print(f"Using server block size: {BLOCK_SIZE}", file=sys.stderr)

    # --- FINAL LOGIC: Generate hashes for prefixes AND the final sequence ---
    hashes_to_check = []
    lengths_hashed = set()

    # 1. Hash all block-aligned prefixes (for prompts longer than one block)
    for i in range(BLOCK_SIZE, num_tokens + 1, BLOCK_SIZE):
        prefix_tokens = token_ids[:i]
        block_hash = compute_block_hash(prefix_tokens)
        hashes_to_check.append(block_hash)
        lengths_hashed.add(i)
        print(f"  - Computed hash for first {i} tokens: {block_hash}", file=sys.stderr)

    # 2. CRITICAL FIX: Always hash the full prompt sequence if it's not a perfect multiple of BLOCK_SIZE.
    #    This handles all prompts shorter than BLOCK_SIZE and the tail of longer prompts.
    if num_tokens > 0 and num_tokens not in lengths_hashed:
        block_hash = compute_block_hash(token_ids)
        hashes_to_check.append(block_hash)
        print(f"  - Computed hash for full {num_tokens} tokens: {block_hash}", file=sys.stderr)

    # --- Check for the longest prefix match first ---
    target_endpoint = None
    
    # Iterate in reverse to find the longest possible match
    for block_hash in reversed(hashes_to_check):
        cached_container_bytes = redis_client.get(str(block_hash))
        if cached_container_bytes:
            cached_container = cached_container_bytes.decode('utf-8')
            if cached_container in VLLM_ENDPOINTS:
                target_endpoint = VLLM_ENDPOINTS[cached_container]
                print(f"✅ Cache HIT! Found prefix in container '{cached_container}'.", file=sys.stderr)
                break # Exit after finding the best match
            else:
                print(f"⚠️ Cache warning: Hash found for unknown container '{cached_container}'.", file=sys.stderr)

    if not target_endpoint:
        chosen_container = random.choice(list(VLLM_ENDPOINTS.keys()))
        target_endpoint = VLLM_ENDPOINTS[chosen_container]
        print(f"❌ Cache MISS. Routing to random container '{chosen_container}'.", file=sys.stderr)

    print(target_endpoint)

if __name__ == "__main__":
    main()