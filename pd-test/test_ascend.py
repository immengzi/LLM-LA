# from vllm import LLM, SamplingParams

# prompts = [
#     "Hello, my name is",
#     "The president of the United States is",
#     "The capital of France is",
#     "The future of AI is",
# ]

# # Create a sampling params object.
# # Note: DeepSeek-R1 models often output reasoning steps inside <think> tags.
# # You might want to increase max_tokens if the model cuts off early.
# sampling_params = SamplingParams(temperature=0.8, top_p=0.95, max_tokens=200)

# # Create an LLM.
# # Pointing directly to your local path
# llm = LLM(model="/mnt/nvme1/haiting_jd/DeepSeek-R1-Distill-Qwen-1.5B")

# # Generate texts from the prompts.
# outputs = llm.generate(prompts, sampling_params)

# for output in outputs:
#     prompt = output.prompt
#     generated_text = output.outputs[0].text
#     print(f"Prompt: {prompt!r}, Generated text: {generated_text!r}")


import os
import re
import gc
import torch
from vllm import LLM, SamplingParams

# --- NEW IMPORTS FOR CLEAN SHUTDOWN ---
from vllm.distributed.parallel_state import destroy_model_parallel, destroy_distributed_environment

# 1. Set Ascend-specific env var for stability
os.environ["HCCL_OP_EXPANSION_MODE"] = "AIV"

def main():
    prompts = [
        "Hello, my name is",
        "The president of the United States is",
        "The capital of France is",
        "The future of AI is",
    ]

    # 2. Setup Sampling
    sampling_params = SamplingParams(temperature=0.6, top_p=0.95, max_tokens=512)

    # 3. Load Model
    llm = LLM(
        model="/mnt/nvme1/haiting_jd/DeepSeek-R1-Distill-Qwen-1.5B",
        trust_remote_code=True
    )

    # 4. Generate
    outputs = llm.generate(prompts, sampling_params)

    # 5. Print Results
    print("\n" + "="*50)
    for output in outputs:
        prompt = output.prompt
        raw_text = output.outputs[0].text
        # Clean <think> tags
        clean_text = re.sub(r'<think>.*?</think>', '', raw_text, flags=re.DOTALL).strip()
        print(f"Prompt: {prompt}")
        print(f"Result: {clean_text}")
        print("-" * 50)
    
    # 6. RETURN THE LLM OBJECT SO WE CAN DELETE IT
    return llm

if __name__ == "__main__":
    # --- PROPER CLEANUP SEQUENCE ---
    llm_instance = main()

    # Explicitly delete the vLLM object
    del llm_instance
    
    # Force Python to clear memory now
    gc.collect()
    
    # Destroy the distributed parallel environment (The magic fix)
    destroy_model_parallel()
    destroy_distributed_environment()
    
    # Clear NPU cache
    torch.npu.empty_cache()
    
    print("Clean exit successful.")