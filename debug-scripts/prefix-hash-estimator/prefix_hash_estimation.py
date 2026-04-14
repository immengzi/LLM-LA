#!/usr/bin/env python3
"""
Compute vLLM prefix cache block hashes by creating an actual Request object.
This uses the exact same code path as the vLLM server.
"""

# --- FIX 1: SET HASH SEED FIRST ---
# This MUST be done before any other imports to ensure
# reproducible hashing.
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
from vllm.utils.hashing import sha256_cbor# Note: Your server used sha256_cbor, ensuring this util is correct
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


def compare_with_vllm_event(computed_hashes, vllm_event_hashes):
    """Compare computed hashes with hashes from vLLM KV events."""
    print("="*80)
    print("COMPARISON WITH VLLM KV EVENT")
    print("="*80)
    
    if len(computed_hashes) != len(vllm_event_hashes):
        print(f"⚠️  Number of blocks mismatch!")
        print(f"   Computed: {len(computed_hashes)}")
        print(f"   vLLM event: {len(vllm_event_hashes)}")
        print()
    
    all_match = True
    for i, (computed, vllm_hash) in enumerate(zip(computed_hashes, vllm_event_hashes)):
        match = computed == vllm_hash
        if match:
            print(f"Block {i}: ✓")
        else:
            print(f"Block {i}: ✗")
            print(f"  Computed:   {computed}")
            print(f"  vLLM event: {vllm_hash}")
            all_match = False
    
    print()
    if all_match:
        print("🎉 ALL HASHES MATCH PERFECTLY!")
    else:
        print("❌ Some hashes don't match")
    
    return all_match


if __name__ == "__main__":
    # Your prompt
    # prompt_text = "I want you to act as an English pronunciation assistant for Turkish speaking people. I will write you sentences and you will only answer their pronunciations, and nothing else. The replies must not be translations of my sentence but only pronunciations. Pronunciations should use Turkish Latin letters for phonetics. Do not write explanations on replies. My first sentence is how the weather is in Istanbul? I will speak to you in English and you will reply to me in English to practice my spoken English. I want you to keep your reply neat, limiting the reply to 100 words. I want you to strictly correct my grammar mistakes, typos, and factual errors. I want you to ask me a question in your reply. Now let's start practicing, you could ask me a question first. Remember, I want you to strictly correct my grammar mistakes, typos, and factual errors. You will come up with entertaining stories that are engaging, imaginative and captivating for the audience. It can be fairy tales, educational stories or any other type of stories which has the potential to capture people's attention and imagination. Depending on the target audience, you may choose specific themes or topics for your storytelling session e.g., if it's children then you can talk about animals; If it's adults then history-based tales might engage them better etc. My first request is I need an interesting story on perseverance. I will provide you with some information about someone's goals and challenges, and it will be your job to come up with strategies that can help this person achieve their goals. This could involve providing positive affirmations, giving helpful advice or suggesting activities they can do to reach their end goal."
    prompt_text = "Yellow, Dive into the complexities of human communication, focusing on how emotions shape interactions across different circumstances—whether in moments of happiness, loss, or disagreement. Explore the role of non-verbal cues like body language, facial expressions, and eye contact in conveying meaning, and how tone and word choice further influence the message being communicated. Examine how cultural backgrounds, societal norms, and the rise of digital communication impact the way we connect with others. In your exploration, consider how empathy, openness, and vulnerability foster deeper connections. Share personal examples where communication either strengthened or created distance between individuals, and provide strategies for enhancing understanding in these exchanges. Explore the depths of human emotion and connection, examining how people communicate in diverse situations—whether in moments of joy, sorrow, or conflict. How do subtle body language cues, tone of voice, and word choice influence interactions? Consider the impact of cultural differences, social contexts, and technology on these exchanges. In your analysis, discuss how empathy, understanding, and vulnerability can build stronger relationships. Reflect on personal experiences where communication either deepened or hindered a connection, and offer insights into improving these dynamics."
    # --- FIX 3: USE CONTAINER-SIDE MODEL PATH ---
    # model_path = "/mnt/storage1/haiting/qwen-test"
    model_path = "/home/haiting/llm-lb/prefix-hash-estimator/qwen-test"
    
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
    
    # vLLM event hashes you received (from your prompt)
    # The first one was 11201884133783637249
    # vllm_event_hashes = [7989735615340656730, 13354895012625416692]
    
    # # Compare
    # compare_with_vllm_event(computed_hashes, vllm_event_hashes)