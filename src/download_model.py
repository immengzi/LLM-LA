from transformers import AutoTokenizer

# Hardcode the tokenizer name
TOKENIZER_NAME = "Qwen/Qwen3-8B"

# Load the tokenizer using the default cache location
tokenizer = AutoTokenizer.from_pretrained(
    TOKENIZER_NAME, 
    local_files_only=False)

print(f"Tokenizer for {TOKENIZER_NAME} loaded and cached by default.")
