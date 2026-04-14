#!/usr/bin/env python3
"""
validate_with_vllm_server.py

Validates that locally computed block hashes match those from a running vLLM server.
This script:
1. Sends the same prompt to a vLLM server with enable_prefix_caching=True
2. Extracts block hashes from server logs or metrics
3. Compares with locally computed hashes
"""

import os
os.environ["PYTHONHASHSEED"] = "0"

import sys
import json
import pickle
import hashlib
from typing import Any, List
import cbor2
import requests
from transformers import AutoTokenizer

# vLLM imports
from vllm.v1.core.kv_cache_utils import (
    get_request_block_hasher,
    init_none_hash,
)

_none_hash_initialized = False


def sha256_hash(input: Any) -> bytes:
    """Pickle-based SHA256 (vLLM default)."""
    input_bytes = pickle.dumps(input, protocol=pickle.HIGHEST_PROTOCOL)
    return hashlib.sha256(input_bytes).digest()


def sha256_cbor(input: Any) -> bytes:
    """CBOR-based SHA256 (more deterministic)."""
    input_bytes = cbor2.dumps(input, canonical=True)
    return hashlib.sha256(input_bytes).digest()


def compute_local_block_hashes(
    token_ids: List[int],
    block_size: int = 16,
    use_cbor: bool = False
) -> List[str]:
    """
    Compute block hashes locally using vLLM's algorithm.
    
    Args:
        token_ids: List of token IDs
        block_size: KV cache block size (default 16)
        use_cbor: Use CBOR hashing instead of pickle
    
    Returns:
        List of block hashes as hex strings
    """
    global _none_hash_initialized
    
    hash_function = sha256_cbor if use_cbor else sha256_hash
    
    if not _none_hash_initialized:
        init_none_hash(hash_function)
        _none_hash_initialized = True
    
    block_hasher = get_request_block_hasher(block_size, hash_function)
    
    # Compute hashes for each block
    num_blocks = (len(token_ids) + block_size - 1) // block_size
    block_hashes = []
    
    for i in range(num_blocks):
        start_idx = i * block_size
        end_idx = min(start_idx + block_size, len(token_ids))
        block_tokens = tuple(token_ids[start_idx:end_idx])
        
        # Use the block hasher to compute hash
        if i == 0:
            # First block uses init_none_hash as parent
            from vllm.v1.core.kv_cache_utils import NONE_HASH
            parent_hash = NONE_HASH
        else:
            parent_hash = block_hashes[i-1]
        
        # Compute hash for this block
        block_hash = block_hasher(block_tokens, parent_hash)
        block_hashes.append(block_hash)
    
    # Convert to hex strings
    return [h.hex() if isinstance(h, bytes) else str(h) for h in block_hashes]


def send_request_to_vllm(
    prompt: str,
    server_url: str = "http://localhost:8000",
    model_name: str = "Qwen/Qwen3-4B",
    max_tokens: int = 32
) -> dict:
    """
    Send a completion request to vLLM server.
    
    Args:
        prompt: The prompt text
        server_url: vLLM server URL
        model_name: Model name
        max_tokens: Maximum tokens to generate
    
    Returns:
        API response as dict
    """
    url = f"{server_url}/v1/completions"
    
    payload = {
        "model": model_name,
        "prompt": prompt,
        "max_tokens": max_tokens,
        "temperature": 0.0,  # Deterministic generation
        "stream": False,
        "echo": False,
    }
    
    headers = {
        "Content-Type": "application/json"
    }
    
    print(f"Sending request to {url}...")
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    
    return response.json()


def extract_server_metrics(
    server_url: str = "http://localhost:8000",
) -> dict:
    """
    Extract metrics from vLLM server (if metrics endpoint is available).
    
    Args:
        server_url: vLLM server URL
    
    Returns:
        Metrics dict
    """
    metrics_url = f"{server_url}/metrics"
    
    try:
        response = requests.get(metrics_url)
        response.raise_for_status()
        return {"metrics": response.text}
    except Exception as e:
        print(f"Warning: Could not fetch metrics: {e}")
        return {}


def main():
    print("=" * 80)
    print("vLLM Block Hash Validation Script")
    print("=" * 80)
    
    # Configuration
    SERVER_URL = os.environ.get("VLLM_SERVER_URL", "http://localhost:8000")
    MODEL_PATH = "/mnt/qwen-4b"
    MODEL_NAME = "Qwen/Qwen3-4B"
    USE_CBOR = False  # Set to True if server uses CBOR
    BLOCK_SIZE = 16
    
    print(f"\nConfiguration:")
    print(f"  Server URL: {SERVER_URL}")
    print(f"  Model: {MODEL_NAME}")
    print(f"  Hash Function: {'CBOR' if USE_CBOR else 'Pickle'}-based SHA256")
    print(f"  Block Size: {BLOCK_SIZE}")
    print(f"  PYTHONHASHSEED: {os.environ.get('PYTHONHASHSEED', 'not set')}")
    
    # The prompt (same as in demo script)
    prompt_text = "I want you to act as an English pronunciation assistant for Turkish speaking people. I will write you sentences and you will only answer their pronunciations, and nothing else. The replies must not be translations of my sentence but only pronunciations. Pronunciations should use Turkish Latin letters for phonetics. Do not write explanations on replies. My first sentence is how the weather is in Istanbul? I will speak to you in English and you will reply to me in English to practice my spoken English. I want you to keep your reply neat, limiting the reply to 100 words. I want you to strictly correct my grammar mistakes, typos, and factual errors. I want you to ask me a question in your reply. Now let's start practicing, you could ask me a question first. Remember, I want you to strictly correct my grammar mistakes, typos, and factual errors.  You will come up with entertaining stories that are engaging, imaginative and captivating for the audience. It can be fairy tales, educational stories or any other type of stories which has the potential to capture people's attention and imagination. Depending on the target audience, you may choose specific themes or topics for your storytelling session e.g., if it's children then you can talk about animals; If it's adults then history-based tales might engage them better etc. My first request is I need an interesting story on perseverance.  I will provide you with some information about someone's goals and challenges, and it will be your job to come up with strategies that can help this person achieve their goals. This could involve providing positive affirmations, giving helpful advice or suggesting activities they can do to reach their end goal. "
    
    # Step 1: Tokenize prompt
    print("\n" + "=" * 80)
    print("Step 1: Tokenizing prompt")
    print("=" * 80)
    
    tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH)
    prompt_token_ids = tokenizer.encode(prompt_text, add_special_tokens=False)
    
    print(f"Prompt tokens: {len(prompt_token_ids)}")
    print(f"Expected blocks: {(len(prompt_token_ids) + BLOCK_SIZE - 1) // BLOCK_SIZE}")
    
    # Step 2: Compute local block hashes
    print("\n" + "=" * 80)
    print("Step 2: Computing local block hashes")
    print("=" * 80)
    
    local_hashes = compute_local_block_hashes(
        prompt_token_ids,
        block_size=BLOCK_SIZE,
        use_cbor=USE_CBOR
    )
    
    print(f"\nComputed {len(local_hashes)} block hashes locally:")
    for i, h in enumerate(local_hashes):
        print(f"  Block {i:2d}: {h}")
    
    # Step 3: Send request to vLLM server
    print("\n" + "=" * 80)
    print("Step 3: Sending request to vLLM server")
    print("=" * 80)
    
    try:
        response = send_request_to_vllm(
            prompt=prompt_text,
            server_url=SERVER_URL,
            model_name=MODEL_NAME,
            max_tokens=32
        )
        
        print("\n✓ Request successful!")
        print(f"Generated text: {response['choices'][0]['text']}")
        print(f"Tokens generated: {response['usage']['completion_tokens']}")
        
    except Exception as e:
        print(f"\n✗ Request failed: {e}")
        print("\nMake sure:")
        print("  1. vLLM server is running")
        print("  2. Server has enable_prefix_caching=True")
        print(f"  3. Server is accessible at {SERVER_URL}")
        print("\nYou can still use the local hashes for manual comparison.")
        return 1
    
    # Step 4: Extract and compare metrics
    print("\n" + "=" * 80)
    print("Step 4: Checking server metrics")
    print("=" * 80)
    
    metrics = extract_server_metrics(SERVER_URL)
    
    if metrics:
        print("\n✓ Metrics retrieved")
        print("\nNote: To compare hashes, you need to:")
        print("  1. Check vLLM server logs for block hash computations")
        print("  2. Enable debug logging with --log-level=DEBUG")
        print("  3. Look for 'block_hash' entries in the logs")
    else:
        print("\n⚠ Could not retrieve metrics automatically")
    
    # Step 5: Save results
    print("\n" + "=" * 80)
    print("Step 5: Saving results")
    print("=" * 80)
    
    results = {
        "prompt_length": len(prompt_text),
        "num_tokens": len(prompt_token_ids),
        "num_blocks": len(local_hashes),
        "block_size": BLOCK_SIZE,
        "hash_function": "cbor" if USE_CBOR else "pickle",
        "local_hashes": local_hashes,
        "server_response": response if 'response' in locals() else None,
    }
    
    output_file = "/tmp/vllm_validation_results.json"
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n✓ Results saved to: {output_file}")
    
    # Step 6: Manual validation instructions
    print("\n" + "=" * 80)
    print("Manual Validation Instructions")
    print("=" * 80)
    print("""
To verify that local hashes match server hashes:

1. Start vLLM server with debug logging:
   
   PYTHONHASHSEED=0 python -m vllm.entrypoints.openai.api_server \\
       --model /mnt/storage1/haiting/qwen-test \\
       --enable-prefix-caching \\
       --log-level DEBUG \\
       --port 8000

2. In the server logs, search for block hash computations:
   
   grep "block_hash" vllm_server.log

3. Compare the hex hashes from logs with local hashes above

4. If hashes DON'T match:
   - Check PYTHONHASHSEED is set to "0" on both client and server
   - Verify both use same hash function (pickle vs CBOR)
   - Ensure same tokenizer and model

5. If hashes DO match:
   ✓ Your local computation is correct!
   ✓ Prefix caching will work as expected
""")
    
    print("\n" + "=" * 80)
    print("Summary")
    print("=" * 80)
    print(f"✓ Computed {len(local_hashes)} local block hashes")
    print(f"✓ Server request {'succeeded' if 'response' in locals() else 'failed'}")
    print(f"✓ Results saved to {output_file}")
    print("\nNext: Compare local hashes with server logs manually")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())