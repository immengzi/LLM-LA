import json
import asyncio
import time
from openai import AsyncOpenAI

# Configuration matching your startup script
MODEL_NAME = "qwen3-8b"
API_URL = "http://localhost:10000/v1"
PROMPTS_FILE = "prompt/prompts_long_prefix_example.json"  # The file you provided

client = AsyncOpenAI(api_key="token-is-ignored", base_url=API_URL)

async def send_request(prompt, request_id):
    """Sends a single request to the vLLM engine."""
    print(f"[Request {request_id}] Sending prompt (len: {len(prompt)})...")
    
    start_time = time.perf_counter()
    try:
        response = await client.completions.create(
            model=MODEL_NAME,
            prompt=prompt,
            max_tokens=100,  # Adjust as needed
            temperature=0.0   # Deterministic for testing
        )
        latency = time.perf_counter() - start_time
        print(f"[Response {request_id}] Latency: {latency:.2f}s | Result: {response.choices[0].text[:50].strip()}...")
        
    except Exception as e:
        print(f"[Error {request_id}] Failed: {e}")

async def main():
    # 1. Load your JSON file
    with open(PROMPTS_FILE, 'r') as f:
        data = json.load(f)
    
    prompts = data.get("prompts", [])
    print(f"Loaded {len(prompts)} prompts. Starting routing...")

    # 2. Route requests
    # Note: To send them one-by-one, use a loop. 
    # To send them concurrently, use asyncio.gather.
    for i, prompt in enumerate(prompts):
        await send_request(prompt, i)
        # Optional: Add a small delay to avoid overwhelming the scheduler immediately
        # await asyncio.sleep(0.1)

if __name__ == "__main__":
    asyncio.run(main())
