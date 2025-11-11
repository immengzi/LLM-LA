import httpx
import asyncio

async def forward_prompt_to_pod(pod_name: str, prompt: str, max_tokens: int = 100):
    url = f"http://{pod_name}:8000/generate"
    payload = {
        "prompt": prompt,
        "max_tokens": max_tokens,
        "temperature": 0
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(url, json=payload)
        resp.raise_for_status()
        return resp.json()

# Example usage
async def main():
    pod = "vllm-1"  # or vllm-2
    prompt = "Hello, world!"
    result = await forward_prompt_to_pod(pod, prompt)
    print(result)

asyncio.run(main())
