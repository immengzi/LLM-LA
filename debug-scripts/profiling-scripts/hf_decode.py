import time
import torch
from transformers import AutoModelForCausalLM, AutoTokenizer

print("cuda_available:", torch.cuda.is_available(), "device:", torch.cuda.get_device_name(0))
torch.backends.cuda.matmul.allow_tf32 = True

mname = "sshleifer/tiny-gpt2"  # quick to download; swap for a bigger model later
tok = AutoTokenizer.from_pretrained(mname)
model = AutoModelForCausalLM.from_pretrained(mname).eval().cuda()

prompt = "Once upon a time"
batch = 16
x = tok([prompt] * batch, return_tensors="pt").to("cuda")

# warmup
with torch.no_grad():
    _ = model.generate(**x, max_new_tokens=64)
torch.cuda.synchronize()

t0 = time.time()
with torch.no_grad():
    _ = model.generate(**x, max_new_tokens=256)
torch.cuda.synchronize()
print("Elapsed:", time.time() - t0)
