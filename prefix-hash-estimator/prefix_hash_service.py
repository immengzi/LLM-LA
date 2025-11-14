#!/usr/bin/env python3
"""
vLLM prefix hash - WEB SERVICE
"""

# --- FIX 1: SET HASH SEED FIRST ---
import os
os.environ["PYTHONHASHSEED"] = "0"

# --- FIX 2: MOCK 'torch_npu' SECOND ---
import sys
import types
# sys.modules["torch_npu"] = types.ModuleType("torch_npu")

# --- Imports ---
import time
from transformers import AutoTokenizer
from flask import Flask, request, jsonify

# vLLM imports
from vllm.sampling_params import SamplingParams
from vllm.v1.request import Request
from vllm.v1.core.kv_cache_utils import (
    get_request_block_hasher,
    init_none_hash,
    maybe_convert_block_hash,
)
from vllm.utils.hashing import sha256_cbor

# --- Global Setup (Run ONCE at startup) ---

# Initialize NONE_HASH
init_none_hash(sha256_cbor)

# --- Hardcoded model path ---
MODEL_PATH = "/home/haiting/llm-lb/prefix-hash-estimator/qwen-test"

# --- Load Tokenizer ONCE ---
print(f"Loading tokenizer from {MODEL_PATH}...")
TOKENIZER = AutoTokenizer.from_pretrained(
    MODEL_PATH, 
    trust_remote_code=True, 
    local_files_only=True
)
print("Tokenizer loaded. Starting service...")

# --- Create Flask App ---
app = Flask(__name__)


def compute_vllm_block_hashes(prompt_text, tokenizer, block_size=128):
    """
    Compute block hashes using vLLM's actual Request class.
    This version takes a tokenizer object instead of a model_path.
    """
    
    # Tokenize prompt (uses the pre-loaded tokenizer)
    prompt_token_ids = tokenizer.encode(prompt_text, add_special_tokens=False)
    
    # Create sampling params
    sampling_params = SamplingParams(
        ignore_eos=False,
        max_tokens=32,
        stop_token_ids=None,
        prompt_logprobs=None,
    )
    
    # Create block hasher
    block_hasher = get_request_block_hasher(block_size, sha256_cbor)
    
    # Create Request object
    req = Request(
        request_id="compute-hash-demo",
        prompt_token_ids=prompt_token_ids,
        sampling_params=sampling_params,
        pooling_params=None,
        eos_token_id=151643,   # Qwen's EOS token
        client_index=0,
        arrival_time=time.time(),
        prompt_embeds=None,
        mm_features=None,
        lora_request=None,
        cache_salt=None,
        priority=0,
        trace_headers=None,
        block_hasher=block_hasher,
    )
    
    # Convert block hashes to the format used in KV events (int)
    block_hashes_as_int = []
    for block_hash in req.block_hashes:
        hash_int = maybe_convert_block_hash(block_hash)
        block_hashes_as_int.append(hash_int)
    
    # Return both hashes and token count for logging
    return block_hashes_as_int, len(prompt_token_ids)


# --- API Endpoint ---
@app.route("/compute_hash", methods=["POST"])
def handle_compute_hash():
    """
    API endpoint to compute hashes.
    Expects JSON: {"prompt": "your prompt text"}
    """
    data = request.json
    prompt = data.get("prompt")
    
    if not prompt:
        return jsonify({"error": "No 'prompt' key found in JSON request"}), 400
        
    block_size = data.get("block_size", 128) # Optionally allow block_size override

    # Use the main function to compute
    # The TOKENIZER object is already in memory
    computed_hashes, token_count = compute_vllm_block_hashes(
        prompt_text=prompt,
        tokenizer=TOKENIZER,
        block_size=block_size
    )
    
    # Log to server console
    print(f"Received request. Prompt tokens: {token_count}. Computed hashes: {len(computed_hashes)}")

    # Return the result as JSON
    return jsonify({
        "prompt_tokens": token_count,
        "computed_hashes": computed_hashes
    })

# To run this script:
# 1. Make sure flask is installed: pip install flask
# 2. Run in terminal: flask --app prefix_hash_service run --host=0.0.0.0 --port=5000