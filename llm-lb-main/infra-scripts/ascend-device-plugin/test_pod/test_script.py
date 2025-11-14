from vllm import LLM, SamplingParams

try:
    import torch_npu
    # Attempt to access an NPU-specific function to confirm linkage
    _ = torch.npu.get_device_name(0)
    print("torch_npu package found and seems functional.")
    NPU_AVAILABLE = True
except ImportError:
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print("!!! WARNING: torch_npu package not found. NPU functionality unavailable. !!!")
    print("!!! Please install torch_npu compatible with your PyTorch and CANN versions. !!!")
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    NPU_AVAILABLE = False
except Exception as e:
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print(f"!!! WARNING: torch_npu imported but error occurred: {e} !!!")
    print("!!! NPU functionality might be unavailable or limited. Check setup.    !!!")
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    NPU_AVAILABLE = False # Treat as unavailable if basic functions fail

prompts = [
    "Hello, my name is",
    # "The president of the United States is",
    # "The capital of France is",
    # "The future of AI is",
]

# Create a sampling params object.
sampling_params = SamplingParams(temperature=0.8, top_p=0.95)
# Create an LLM.
llm = LLM(model="/models/deepseek-ai/DeepSeek-R1-Distill-Qwen-1.5B", trust_remote_code=True)

# Generate texts from the prompts.
outputs = llm.generate(prompts, sampling_params)
for output in outputs:
    prompt = output.prompt
    generated_text = output.outputs[0].text
    print(f"Prompt: {prompt!r}, Generated text: {generated_text!r}")

