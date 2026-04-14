from datasets import load_dataset
import os

# Set the cache directory (optional, but good practice)
# This tells the library where to save data.
# By default, it saves to ~/.cache/huggingface/datasets
os.environ['HF_DATASETS_CACHE'] = os.path.expanduser('~/.cache/huggingface/datasets')

print("Attempting to load the dataset...")

try:
    # This one command does everything:
    # 1. Checks your login authentication (from `huggingface-cli login`).
    # 2. Verifies your account has agreed to the terms on the website.
    # 3. Downloads all the .arrow files (shards) to your cache directory.
    # 4. Assembles them into a single Dataset object.
    dataset = load_dataset("lmsys/lmsys-chat-1m")

    print("\n✅ Success! Dataset loaded.")
    
    # You can now use the dataset
    print("\n--- Dataset Info ---")
    print(dataset)
    
    print("\n--- First Example ---")
    print(dataset['train'][0])

except Exception as e:
    print(f"\n❌ FAILED to load dataset.")
    
    # THIS IS THE IMPORTANT CHANGE:
    print("\n--- THE ACTUAL ERROR IS ---")
    print(e)  
    print("---------------------------\n")
    
    print("Please double-check that you have:")
    print("1. Logged in via `huggingface-cli login`")
    print("2. Agreed to the terms on the dataset's Hugging Face page.")