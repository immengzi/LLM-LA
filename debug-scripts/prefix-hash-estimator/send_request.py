import requests
import json
from transformers import AutoTokenizer

# Load the tokenizer
# tokenizer = AutoTokenizer.from_pretrained("/mnt/storage1/haiting/qwen-test", trust_remote_code=True)
tokenizer = AutoTokenizer.from_pretrained("/home/haiting/llm-lb/prefix-hash-estimator/qwen-test", trust_remote_code=True)
# prompt_text = "I want you to act as an English pronunciation assistant for Turkish speaking people. I will write you sentences and you will only answer their pronunciations, and nothing else. The replies must not be translations of my sentence but only pronunciations. Pronunciations should use Turkish Latin letters for phonetics. Do not write explanations on replies. My first sentence is how the weather is in Istanbul? I will speak to you in English and you will reply to me in English to practice my spoken English. I want you to keep your reply neat, limiting the reply to 100 words. I want you to strictly correct my grammar mistakes, typos, and factual errors. I want you to ask me a question in your reply. Now let's start practicing, you could ask me a question first. Remember, I want you to strictly correct my grammar mistakes, typos, and factual errors. You will come up with entertaining stories that are engaging, imaginative and captivating for the audience. It can be fairy tales, educational stories or any other type of stories which has the potential to capture people's attention and imagination. Depending on the target audience, you may choose specific themes or topics for your storytelling session e.g., if it's children then you can talk about animals; If it's adults then history-based tales might engage them better etc. My first request is I need an interesting story on perseverance. I will provide you with some information about someone's goals and challenges, and it will be your job to come up with strategies that can help this person achieve their goals. This could involve providing positive affirmations, giving helpful advice or suggesting activities they can do to reach their end goal."

prompt_text = "Yellow, Dive into the complexities of human communication, focusing on how emotions shape interactions across different circumstances—whether in moments of happiness, loss, or disagreement. Explore the role of non-verbal cues like body language, facial expressions, and eye contact in conveying meaning, and how tone and word choice further influence the message being communicated. Examine how cultural backgrounds, societal norms, and the rise of digital communication impact the way we connect with others. In your exploration, consider how empathy, openness, and vulnerability foster deeper connections. Share personal examples where communication either strengthened or created distance between individuals, and provide strategies for enhancing understanding in these exchanges. Explore the depths of human emotion and connection, examining how people communicate in diverse situations—whether in moments of joy, sorrow, or conflict. How do subtle body language cues, tone of voice, and word choice influence interactions? Consider the impact of cultural differences, social contexts, and technology on these exchanges. In your analysis, discuss how empathy, understanding, and vulnerability can build stronger relationships. Reflect on personal experiences where communication either deepened or hindered a connection, and offer insights into improving these dynamics."

# Tokenize the prompt
token_ids = tokenizer.encode(prompt_text)

print("="*80)
print("TOKENIZATION INFORMATION")
print("="*80)
print(f"Total tokens: {len(token_ids)}")
print(f"\nComplete token ID list:")
print(token_ids)

print(f"\n{'='*80}")
print("TOKEN IDs BY BLOCK (block_size=128)")
print(f"{'='*80}")
block_size = 128 # vllm-ascend may use block size 128, double check
num_blocks = (len(token_ids) + block_size - 1) // block_size

for block_idx in range(num_blocks):
    start_idx = block_idx * block_size
    end_idx = min(start_idx + block_size, len(token_ids))
    block_tokens = token_ids[start_idx:end_idx]
    
    print(f"\nBlock {block_idx}:")
    print(f"  Position: tokens[{start_idx}:{end_idx}]")
    print(f"  Size: {len(block_tokens)} tokens")
    print(f"  Token IDs: {block_tokens}")

# Send request to vLLM server
print(f"\n{'='*80}")
print("SENDING REQUEST TO VLLM SERVER")
print(f"{'='*80}\n")

# UPDATED: Port changed from 8080 to 8099 to match your docker -p 8099:8000 mapping
url = "http://localhost:8100/v1/completions"
headers = {"Content-Type": "application/json"}

payload = {
    "model": "qwen-local",
    "prompt": prompt_text,
    "max_tokens": 100,
    "temperature": 0.0,
    "seed": 42  # For reproducibility
}

print(f"Sending request to {url}...")
response = requests.post(url, headers=headers, json=payload)

if response.status_code == 200:
    result = response.json()
    print("\n=== Response ===")
    print(json.dumps(result, indent=2))
    print(f"\n{'='*80}")
    print(f"Prompt tokens: {result['usage']['prompt_tokens']}")
    print(f"Completion tokens: {result['usage']['completion_tokens']}")
    print(f"Total tokens: {result['usage']['total_tokens']}")
    print(f"{'='*80}")
else:
    print(f"Error: {response.status_code}")
    print(response.text)
