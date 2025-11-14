import time
import torch

print("cuda_available:", torch.cuda.is_available(), "device:", torch.cuda.get_device_name(0))
torch.backends.cuda.matmul.allow_tf32 = True  # give the profiler some beefy kernels

M = N = K = 4096
A = torch.randn(M, K, device='cuda', dtype=torch.float32)
B = torch.randn(K, N, device='cuda', dtype=torch.float32)

# warmup
for _ in range(10):
    _ = A @ B
torch.cuda.synchronize()

t0 = time.time()
# run long enough for profilers to catch multiple kernels
for _ in range(600):
    _ = A @ B
torch.cuda.synchronize()
print("Elapsed:", time.time() - t0)
