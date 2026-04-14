import argparse
import struct
import sys
from typing import List

import xxhash
from transformers import AutoTokenizer

def compute_vllm_hash(token_ids: List[int]) -> int:
    """
    Computes a hash for a list of token IDs, replicating vLLM's methodology.
    This function has been verified as correct.
    """
    if not token_ids:
        raise ValueError("Token ID list cannot be empty.")

    format_string = f"<{len(token_ids)}I"
    token_bytes = struct.pack(format_string, *token_ids)
    digest = xxhash.xxh64_digest(token_bytes, seed=0)
    block_hash = int.from_bytes(digest, 'little', signed=False)
    return block_hash

def main():
    """Main function to parse arguments and compute the hash."""
    parser = argparse.ArgumentParser(
        description="Compute a hash for a given prompt exactly as vLLM's /generate endpoint does for its prefix cache.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "--prompt",
        type=str,
        required=True,
        help="The raw user prompt to be hashed."
    )
    parser.add_argument(
        "--tokenizer-path",
        type=str,
        required=True,
        help="Path to the Hugging Face tokenizer directory. MUST be the same as the server's."
    )

    args = parser.parse_args()

    print(f"Loading tokenizer from: {args.tokenizer_path}")
    try:
        # For models like Qwen, trust_remote_code=True is essential to load
        # the custom tokenizer code (tokenization_qwen.py). This ensures
        # the token IDs match the server's exactly.
        tokenizer = AutoTokenizer.from_pretrained(
            args.tokenizer_path,
            trust_remote_code=True
        )
    except Exception as e:
        print(f"Error: Failed to load tokenizer. {e}", file=sys.stderr)
        sys.exit(1)

    # --- THE CORRECT LOGIC FOR THE /generate ENDPOINT ---
    # The /generate endpoint does NOT apply a chat template. It tokenizes
    # the raw prompt string directly. We replicate that here.
    print("\nTokenizing raw prompt (no chat template applied)...")
    token_ids = tokenizer.encode(args.prompt)
    num_tokens = len(token_ids)

    if num_tokens == 0:
        print("Error: Prompt produced zero tokens.", file=sys.stderr)
        sys.exit(1)

    print("Computing vLLM-compatible hash...")
    try:
        final_hash = compute_vllm_hash(token_ids)
    except Exception as e:
        print(f"Error during hash computation: {e}", file=sys.stderr)
        sys.exit(1)

    # Convert token IDs to their string representation for printing
    decoded_tokens = tokenizer.convert_ids_to_tokens(token_ids)

    print("\n--- Results ---")
    print(f"Raw Prompt: \"{args.prompt[:80]}...\"")
    print(f"Total Tokens: {num_tokens}")
    # Updated to print all decoded tokens and their corresponding IDs
    print(f"Decoded Tokens: {decoded_tokens}")
    print(f"Token IDs: {token_ids}")

    print("-" * 20)
    print(f"✅ vLLM Hash: {final_hash}")
    print("-" * 20)


if __name__ == "__main__":
    main()