import json
import random # Make sure random is imported
from datasets import load_dataset
import os

# --- Configuration ---
DATASET_NAME = "lmsys/lmsys-chat-1m"
NUM_SAMPLES = 100
CACHE_DIR = os.path.expanduser('~/.cache/huggingface/datasets')

TRANSLATION_KEYWORDS = [
    "translate", "translation", "in english", "in spanish", "in french",
    "in german", "in chinese", "in japanese", "language", "meaning in"
]

# --- (Keep the extract_conversations function exactly the same as before) ---
def extract_conversations():
    """Loads the dataset and extracts different types of conversations."""

    print(f"Attempting to load dataset: {DATASET_NAME}")
    try:
        # Load the dataset (will use cache if already downloaded)
        # Using streaming=True initially to avoid loading all 1M rows into memory at once
        dataset = load_dataset(DATASET_NAME, split='train', cache_dir=CACHE_DIR, streaming=True)
        print("Dataset loaded successfully (in streaming mode).")
    except Exception as e:
        print(f"\n❌ FAILED to load dataset.")
        print("\n--- THE ACTUAL ERROR IS ---")
        print(e)
        print("---------------------------\n")
        print("Please double-check that you have:")
        print("1. Logged in via `huggingface-cli login`")
        print("2. Agreed to the terms on the dataset's Hugging Face page.")
        print("3. Have sufficient disk space and correct permissions in your cache directory.")
        return [], [], [] # Return empty lists on failure

    multi_round_conversations = []
    translation_conversations = []
    random_conversations_temp = [] # Temporarily store random samples during iteration
    
    processed_count = 0
    translation_ids = set() # To avoid duplicates for translation list

    print(f"Iterating through dataset to find {NUM_SAMPLES} of each category...")

    try:
        # Iterate through the streamed dataset
        for example in dataset:
            processed_count += 1
            conversation = example.get('conversation')
            convo_id = example.get('conversation_id') # Get ID for uniqueness check

            if not conversation:
                continue # Skip if conversation data is missing

            # 1. Check for Multi-Round Conversations
            if len(multi_round_conversations) < NUM_SAMPLES and len(conversation) > 2:
                multi_round_conversations.append(conversation)

            # 2. Check for Translation-Related Conversations
            if len(translation_conversations) < NUM_SAMPLES and convo_id not in translation_ids:
                is_translation_related = False
                for message in conversation:
                    content = message.get('content', '').lower()
                    if any(keyword in content for keyword in TRANSLATION_KEYWORDS):
                        is_translation_related = True
                        break 
                
                if is_translation_related:
                    translation_conversations.append(conversation)
                    translation_ids.add(convo_id) 

            # 3. Collect potential Random Conversations
            if len(random_conversations_temp) < NUM_SAMPLES * 5: 
                 random_conversations_temp.append(conversation)

            # Stop iterating if we have enough samples
            if (len(multi_round_conversations) >= NUM_SAMPLES and
                len(translation_conversations) >= NUM_SAMPLES and
                len(random_conversations_temp) >= NUM_SAMPLES * 5):
                print(f"\nCollected enough samples after processing {processed_count} entries.")
                break
            
            if processed_count % 10000 == 0:
                 print(f"Processed {processed_count} entries...")

    except Exception as e:
         print(f"\nAn error occurred during iteration: {e}")
         print("Returning partially collected lists.")

    # Finalize Random Selection
    if len(random_conversations_temp) >= NUM_SAMPLES:
         random_conversations = random.sample(random_conversations_temp, NUM_SAMPLES)
    else:
         print(f"Warning: Only collected {len(random_conversations_temp)} potential random samples. Using all collected.")
         random_conversations = random_conversations_temp 

    print("\n--- Collection Summary ---")
    print(f"Multi-round conversations collected: {len(multi_round_conversations)}")
    print(f"Translation conversations collected: {len(translation_conversations)}")
    print(f"Random conversations collected: {len(random_conversations)}")

    return multi_round_conversations, translation_conversations, random_conversations


# --- Run extraction, COMBINE, SHUFFLE, and SAVE ---
if __name__ == "__main__":
    multi_round, translation, random_convs = extract_conversations()

    # --- Combine the lists ---
    combined_conversations = []
    if multi_round:
        combined_conversations.extend(multi_round)
    if translation:
        combined_conversations.extend(translation)
    if random_convs:
        combined_conversations.extend(random_convs)

    total_collected = len(combined_conversations)
    print(f"\nTotal conversations collected: {total_collected}")

    # --- Shuffle the combined list ---
    if total_collected > 0:
        print("Shuffling the combined list...")
        random.shuffle(combined_conversations) # Shuffles the list in-place
        print("✅ Shuffling complete.")

        # --- Define the output file name ---
        combined_file = "combined_mixed_conversations.json"

        # --- Save the shuffled combined list ---
        print(f"\nSaving {total_collected} shuffled conversations to {combined_file}...")
        try:
            with open(combined_file, 'w', encoding='utf-8') as f:
                # Use indent=2 for readability, or remove it for one line per conversation
                json.dump(combined_conversations, f, ensure_ascii=False, indent=2) 
            print(f"✅ Saved successfully to {combined_file}.")
        except Exception as e:
            print(f"❌ Error saving {combined_file}: {e}")

        # --- Optional: Print first 3 examples from the SHUFFLED list ---
        print("\n--- First 3 Shuffled Conversation Examples ---")
        for idx, convo in enumerate(combined_conversations[:3]):
            print(f"\nExample {idx+1}:")
            for i, msg in enumerate(convo):
                print(f"  Turn {i}: Role='{msg['role']}', Content='{msg['content'][:100]}...'")

    else:
        print("\nNo data was collected, nothing to shuffle or save.")