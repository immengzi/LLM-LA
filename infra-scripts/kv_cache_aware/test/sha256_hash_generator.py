import hashlib
from typing import Callable, Any, Optional, Sequence, NewType, List, Tuple
from transformers import AutoTokenizer

# --- Type Definitions to match vLLM's structure ---

# BlockHash is just a type alias for bytes, giving it semantic meaning.
BlockHash = NewType("BlockHash", bytes)

# The initial "parent" hash for the very first block in a sequence.
# It's a constant, empty byte string.
NONE_HASH = BlockHash(b"")


# --- A concrete hash function that fits the required signature ---

def sha256_hash_function(data: Any) -> bytes:
    """
    A concrete implementation for the `hash_function` argument.
    It serializes the input data using repr() and computes its SHA256 hash.
    """
    # `repr()` creates a canonical string representation of the tuple.
    # `.encode('utf-8')` converts this string to bytes for the hash algorithm.
    s = repr(data)
    return hashlib.sha256(s.encode("utf-8")).digest()

def compute_prompt_prefix_hashes(
    prompt: str,
    tokenizer: Any,
    block_size: int
) -> List[Tuple[int, List[int], BlockHash]]:
    """
    Simulates vLLM's prefix hashing process for a given prompt.

    Returns a list of tuples: (block_number, tokens_in_block, cumulative_hash)
    """
    print("-" * 50)
    print(f"Processing prompt: \"{prompt}\"")
    print(f"Using block size: {block_size}")
    print("-" * 50)

    # 1. Tokenize the prompt
    token_ids = tokenizer.encode(prompt)
    print(f"Generated {len(token_ids)} tokens:\n{token_ids}\n")

    # 2. Split tokens into blocks
    token_blocks = [
        token_ids[i : i + block_size]
        for i in range(0, len(token_ids), block_size)
    ]

    # 3. Iterate through blocks and compute chained hashes
    parent_hash: Optional[BlockHash] = None
    results = []
    
    print("--- Block-by-Block Hashing ---")
    for i, current_block_tokens in enumerate(token_blocks):
        # The core logic: compute the hash for the current block
        block_hash = hash_block_tokens(
            hash_function=sha256_hash_function,
            parent_block_hash=parent_hash,
            curr_block_token_ids=current_block_tokens,
        )

        # Store the result for this prefix
        results.append((i, current_block_tokens, block_hash))
        
        print(f"Block {i}:")
        print(f"  - Parent Hash (Input) : {parent_hash.hex() if parent_hash else 'None'}")
        print(f"  - Tokens              : {current_block_tokens}")
        print(f"  - Cumulative Hash (Output): {block_hash.hex()}")

        # The current block's hash becomes the parent for the next iteration
        parent_hash = block_hash
        
    return results

# --- Main execution ---
if __name__ == "__main__":
    # In a real vLLM engine, this would be a system-wide configuration
    BLOCK_SIZE = 16 

    # We use a standard tokenizer
    # Using 'gpt2' as a simple, common example
    tokenizer = AutoTokenizer.from_pretrained("gpt2")

    prompt1 = "The capital of France is Paris. The capital of the United Kingdom is"
    prompt2 = "The capital of France is Paris. The capital of the United Kingdom is London."

    # Process the first prompt
    hashes1 = compute_prompt_prefix_hashes(prompt1, tokenizer, BLOCK_SIZE)

    # Process a second, longer prompt that shares a prefix
    hashes2 = compute_prompt_prefix_hashes(prompt2, tokenizer, BLOCK_SIZE)
    
    print("\n" + "="*50)
    print("VERIFICATION: Comparing Hashes")
    print("="*50)
    
    # Find the length of the common prefix in blocks
    common_prefix_len = len(hashes1)
    
    print(f"Prompt 1 has {len(hashes1)} blocks.")
    print(f"Prompt 2 has {len(hashes2)} blocks.")
    print(f"They share a common prefix of {common_prefix_len} blocks.\n")

    for i in range(common_prefix_len):
        hash1 = hashes1[i][2]
        hash2 = hashes2[i][2]
        
        print(f"Block {i} Hash 1: {hash1.hex()}")
        print(f"Block {i} Hash 2: {hash2.hex()}")
        
        if hash1 == hash2:
            print("  --> MATCH! vLLM would reuse the KV cache for this block.\n")
        else:
            print("  --> MISMATCH! This indicates an error in the logic.\n")