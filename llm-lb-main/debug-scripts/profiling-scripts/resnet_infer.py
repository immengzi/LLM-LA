import time
import torch
import torchvision as tv

print("cuda_available:", torch.cuda.is_available(), "device:", torch.cuda.get_device_name(0))
torch.backends.cuda.matmul.allow_tf32 = True
torch.backends.cudnn.benchmark = True

model = tv.models.resnet50(weights=None).eval().cuda()
x = torch.randn(32, 3, 224, 224, device='cuda', dtype=torch.float32)

# warmup
with torch.no_grad():
    for _ in range(10):
        _ = model(x)
torch.cuda.synchronize()

t0 = time.time()
with torch.no_grad():
    for _ in range(60):
        _ = model(x)
torch.cuda.synchronize()
print("Elapsed:", time.time() - t0)
