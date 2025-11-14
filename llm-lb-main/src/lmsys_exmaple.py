from datasets import load_dataset
from transformers import AutoTokenizer


ds = load_dataset("lmsys/lmsys-chat-1m", split="train[:5]")

print("=== Dataset Schema ===")
print(f"Columns: {ds.column_names}")
print(f"Total rows in full train split: {ds.info.splits['train'].num_examples}")
print(f"Sample row keys: {list(ds[0].keys())}")
print("======================================")


tok = AutoTokenizer.from_pretrained("gpt2")


for i, row in enumerate(ds):
    conv = row.get("conversation", [])
    if not conv or len(conv) < 2:
        continue

    last_user, last_assistant = None, None
    for msg in conv:
        role = msg.get("role", "").lower()
        text = msg.get("content", "").strip()
        if role == "user":
            last_user = text
        elif role == "assistant" and last_user:
            last_assistant = text
            break

    if not last_user or not last_assistant:
        continue

    user_tokens = tok.tokenize(last_user)
    reply_tokens = tok.tokenize(last_assistant)

    print(f"\n=== Chat {i} ===")
    print(f"User: {last_user[:200]}")
    print(f"Assistant: {last_assistant[:200]}")
    print(f"Prompt tokens: {len(user_tokens)} | Reply tokens: {len(reply_tokens)}")

print("\nDone.")
