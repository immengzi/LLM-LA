import argparse
import random
import sys
import redis
import requests
import xxhash
from transformers import AutoTokenizer

# --- Configuration ---
# This must match the model loaded in your vLLM containers
MODEL_NAME = "Qwen/Qwen-7B-Chat" 

REDIS_HOST = "localhost"
REDIS_PORT = 6379

# Mapping of container names (from Redis) to their API endpoints
VLLM_ENDPOINTS = {
    "vllm-1": "http://localhost:8001",
    "vllm-2": "http://localhost:8002",
}

def compute_block_hash(token_ids: list[int]) -> int:
    """
    Computes the block hash in the same way vLLM does.
    vLLM uses xxhash64 with a seed of 0 on the byte representation of the token list.
    """
    # Convert token list to a byte array
    token_bytes = bytes(token_ids)
    
    # Compute xxhash64 digest
    digest = xxhash.xxh64_digest(token_bytes, seed=0)
    
    # Convert the 8-byte digest to a signed 64-bit integer (little-endian)
    # This mimics how a C-style int64_t would be interpreted.
    block_hash = int.from_bytes(digest, 'little', signed=True)
    return block_hash

def main():
    parser = argparse.ArgumentParser(description="A smart client for vLLM with prefix cache lookup.")
    parser.add_argument("prompt", type=str, help="The prompt to send to the language model.")
    args = parser.parse_args()
    prompt = args.prompt

    print("--- Smart vLLM Client ---")
    
    # 1. Initialize Tokenizer and Redis Client
    try:
        print(f"Loading tokenizer for '{MODEL_NAME}'...")
        tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, trust_remote_code=True)
        redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
        redis_client.ping()
        print("Successfully connected to Redis.")
    except Exception as e:
        print(f"Error during initialization: {e}", file=sys.stderr)
        sys.exit(1)

    # 2. Tokenize the prompt and compute the potential block hash
    token_ids = tokenizer.encode(prompt)
    
    # In vLLM, the first block is created from the prompt tokens.
    # Subsequent blocks are created for generated tokens. We are only interested in the prompt prefix.
    # Note: vLLM may have block size constraints, but for prefix matching, 
    # the hash of the full prompt token sequence is what identifies the sequence.
    block_hash = compute_block_hash(token_ids)

    print(f"Prompt: '{prompt}'")
    print(f"Token IDs: {token_ids}")
    print(f"Computed Block Hash: {block_hash}")

    # 3. Lookup the hash in Redis
    cached_container_bytes = redis_client.get(str(block_hash))
    target_endpoint = None
    
    # 4. Routing Logic
    if cached_container_bytes:
        cached_container = cached_container_bytes.decode('utf-8')
        if cached_container in VLLM_ENDPOINTS:
            target_endpoint = VLLM_ENDPOINTS[cached_container]
            print(f"\n✅ Cache HIT! Prefix found in container '{cached_container}'. Routing to {target_endpoint}")
        else:
            print(f"⚠️ Cache warning: Hash found for container '{cached_container}', but no endpoint is configured. Falling back to load balancing.")

    if not target_endpoint:
        # Fallback to simple load balancing (random choice)
        chosen_container = random.choice(list(VLLM_ENDPOINTS.keys()))
        target_endpoint = VLLM_ENDPOINTS[chosen_container]
        print(f"\n❌ Cache MISS. Routing to container '{chosen_container}' via load balancing: {target_endpoint}")

    # 5. Send the request to the chosen vLLM server
    api_url = f"{target_endpoint}/v1/completions"
    payload = {
        "model": MODEL_NAME, # vLLM needs the model name
        "prompt": prompt,
        "max_tokens": 50,
        "temperature": 0.1,
    }
    
    print(f"Sending request to {api_url}...")
    try:
        response = requests.post(api_url, json=payload)
        response.raise_for_status()
        print("\n--- vLLM Response ---")
        print(response.json()['choices'][0]['text'])
        print("---------------------\n")
    except requests.exceptions.RequestException as e:
        print(f"\nError sending request to vLLM: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()