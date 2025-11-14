# client.py
import asyncio
import aiohttp
import json
import time
from datetime import datetime

# --- Configuration (Modify based on your environment) ---
# ❗️IMPORTANT❗️: Before running, change this IP address to your K8s GPU node's IP.
K8S_NODE_IP = "localhost"

# ❗️IMPORTANT❗️: Ensure this port matches the nodePort in your Service YAML.
K8S_NODE_PORT = 30003
# --- Script Constants ---
# This path should point to the location of your prompts file.
PROMPT_FILE = "/home/saeid/llm-lb/prompts/short.json" 
# This must match the '--served-model-name' argument in your Deployment YAML.
SERVED_MODEL_NAME = "served-model"
API_URL = f"http://{K8S_NODE_IP}:{K8S_NODE_PORT}/v1/completions"
HEADERS = {"Content-Type": "application/json"}

async def send_request(session: aiohttp.ClientSession, prompt: str, request_num: int):
    """Asynchronously sends a single request and logs the timing."""
    payload = {
        "model": SERVED_MODEL_NAME,
        "prompt": prompt,
        "max_tokens": 1024,
        "temperature": 0,
        "ignore_eos": True,
    }
    
    start_dt = datetime.now()
    start_timestamp_str = start_dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    print(f"[{start_timestamp_str}] [Request {request_num:02d}] >> Sent. Prompt length: {len(prompt)} chars.")
    
    try:
        async with session.post(API_URL, headers=HEADERS, data=json.dumps(payload), timeout=1000) as response:
            end_dt = datetime.now()
            end_timestamp_str = end_dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            duration = (end_dt - start_dt).total_seconds()

            if response.status == 200:
                result = await response.json()
                generated_text = result['choices'][0]['text'].strip().replace('\n', ' ')
                output_tokens = result['usage']['completion_tokens']
                print(
                    f"[{end_timestamp_str}] [Request {request_num:02d}] << Received after {duration:.2f}s. "
                    f"Output Tokens: {output_tokens}. "
                    f"Preview: '{generated_text[:50]}...'"
                )
            else:
                error_text = await response.text()
                print(f"[{end_timestamp_str}] [Request {request_num:02d}] !! Error after {duration:.2f}s. Status: {response.status}, Details: {error_text}")
    except aiohttp.ClientConnectorError as e:
        print(f"!! Connection Error. Is the K8S_NODE_IP '{K8S_NODE_IP}' correct? Details: {e}")
    except asyncio.TimeoutError:
        print(f"!! Timeout Error. The request to {API_URL} took too long to complete.")

async def main():
    """Main function to load prompts and execute requests concurrently."""
    print("="*60)
    print(f"Targeting vLLM service at: {API_URL}")
    
    
    try:
        with open(PROMPT_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Check if the loaded data is a list
        if not isinstance(data, list) or len(data) == 0:
            print(f"Error: JSON file at '{PROMPT_FILE}' must contain a non-empty list.")
            return

        # Check the format inside the list
        if isinstance(data[0], dict):
            # This is the expected format: list of objects
            prompts = [item['prompt'] for item in data]
        elif isinstance(data[0], str):
            # This handles your current format: list of strings
            print("Note: JSON is a simple list of strings. Processing directly.")
            prompts = data
        else:
            print(f"Error: Unsupported format inside the JSON list at '{PROMPT_FILE}'.")
            return
    except FileNotFoundError:
        print(f"Error: Prompt file not found at '{PROMPT_FILE}'")
        return
    except (json.JSONDecodeError, KeyError) as e:
        print(f"Error: Could not parse prompts from '{PROMPT_FILE}'. Please check the file format. Details: {e}")
        return

    total_start_time = time.time()
    async with aiohttp.ClientSession() as session:
        tasks = [send_request(session, prompt, i+1) for i, prompt in enumerate(prompts)]
        await asyncio.gather(*tasks)
        
    total_duration = time.time() - total_start_time
    print("\n" + "="*60)
    print(f"All {len(prompts)} requests completed. Total time: {total_duration:.2f} seconds.")
    print("="*60)

if __name__ == "__main__":
    if "YOUR_K8S_NODE_IP" in K8S_NODE_IP:
        print("\n❗️❗️❗️ ERROR: Please modify the 'K8S_NODE_IP' variable in the script before running! \n")
    else:
        asyncio.run(main())