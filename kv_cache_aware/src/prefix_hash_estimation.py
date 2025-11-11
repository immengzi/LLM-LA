#!/usr/bin/env python3
"""
Compute vLLM prefix cache block hashes by creating an actual Request object.
This uses the exact same code path as the vLLM server.
"""

# --- FIX 1: SET HASH SEED FIRST ---
import os
os.environ["PYTHONHASHSEED"] = "0"

# --- FIX 2: MOCK 'torch_npu' SECOND ---
# This mock is needed for vllm to import without a real NPU.
import sys
import types

# # Create a fake, empty "torch_npu" module
# sys.modules["torch_npu"] = types.ModuleType("torch_npu")

# --- Original script imports (now safe) ---
import time
from transformers import AutoTokenizer

# vLLM imports
from vllm.sampling_params import SamplingParams
from vllm.v1.request import Request
from vllm.v1.core.kv_cache_utils import (
    get_request_block_hasher,
    init_none_hash,
    maybe_convert_block_hash,
)
# from vllm.utils.hashing import sha256_cbor

import hashlib
import cbor2

def sha256_cbor(obj):
    """
    Replacement for vllm.utils.hashing.sha256_cbor
    Computes SHA256 of CBOR-encoded object.
    """
    cbor_bytes = cbor2.dumps(obj)
    return hashlib.sha256(cbor_bytes).digest()
    # return hashlib.sha256(cbor_bytes).hexdigest()

# Note: Your server used sha256_cbor, ensuring this util is correct
# If the above import fails, it might be:
# from vllm.utils import sha256_cbor_64bit

# Initialize NONE_HASH once
# Using the same hash function as the server
init_none_hash(sha256_cbor) # or sha256_cbor_64bit

def compute_vllm_block_hashes(prompt_text, model_path, block_size=128):
    """
    Compute block hashes using vLLM's actual Request class.
    """
    # Load tokenizer
    # tokenizer = AutoTokenizer.from_pretrained(model_path, trust_remote_code=True)
    tokenizer = AutoTokenizer.from_pretrained(
    model_path, 
    trust_remote_code=True, 
    local_files_only=True)
    
    # Tokenize prompt
    prompt_token_ids = tokenizer.encode(prompt_text, add_special_tokens=False)
    
    print("="*80)
    print("COMPUTING BLOCK HASHES WITH VLLM REQUEST")
    print("="*80)
    print(f"Total tokens: {len(prompt_token_ids)}")
    print(f"Block size: {block_size}")
    print(f"Prompt token IDs: {prompt_token_ids}")
    print()
    
    # Create sampling params
    sampling_params = SamplingParams(
        ignore_eos=False,
        max_tokens=32,
        stop_token_ids=None,
        prompt_logprobs=None,
    )
    
    # Create block hasher
    block_hasher = get_request_block_hasher(block_size, sha256_cbor) # or sha256_cbor_64bit
    
    # Create Request object
    req = Request(
        request_id="compute-hash-demo",
        prompt_token_ids=prompt_token_ids,
        sampling_params=sampling_params,
        pooling_params=None,
        eos_token_id=151643,  # Qwen's EOS token
        client_index=0,
        arrival_time=time.time(),
        prompt_embeds=None,
        mm_features=None,
        lora_request=None,
        # structured_output_request=None,
        cache_salt=None,
        priority=0,
        trace_headers=None,
        block_hasher=block_hasher,
    )
    
    print("="*80)
    print("BLOCK HASHES FROM VLLM REQUEST")
    print("="*80)
    print(f"Number of block hashes: {len(req.block_hashes)}")
    print()
    
    # Convert block hashes to the format used in KV events (int)
    block_hashes_as_int = []
    for i, block_hash in enumerate(req.block_hashes):
        # block_hash is bytes, convert to int like vLLM does for events
        hash_int = maybe_convert_block_hash(block_hash)
        block_hashes_as_int.append(hash_int)
        
        # Show tokens for this block
        start_idx = i * block_size
        end_idx = min(start_idx + block_size, len(prompt_token_ids))
        block_tokens = prompt_token_ids[start_idx:end_idx]
        
        print(f"Block {i}:")
        print(f"  Hash (bytes): {block_hash.hex()}")
        print(f"  Hash (int):   {hash_int}")
        print(f"  Tokens [{start_idx}:{end_idx}]: {block_tokens}")
        print()
    
    return block_hashes_as_int, prompt_token_ids



if __name__ == "__main__":
    # Your prompt
    prompt_text = "Hello world."
    model_path = "/model/qwen-test"
    
    # Compute hashes using vLLM's Request
    computed_hashes, token_ids = compute_vllm_block_hashes(prompt_text, model_path)
    
    print("="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Total tokens: {len(token_ids)}")
    print(f"Full blocks: {len(computed_hashes)}")
    print(f"\nComputed hashes:")
    print(computed_hashes)
    print()
