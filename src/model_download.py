#!/usr/bin/env python3
import os
from pathlib import Path
from huggingface_hub import snapshot_download, HfApi

# Belt-and-braces: set BOTH legacy and current flags BEFORE import (already imported here)
os.environ["HF_HUB_DISABLE_XET"] = "1"  # new-style disable
os.environ["HF_HUB_ENABLE_XET"] = "0"  # some builds check enable flag
os.environ["HF_HUB_ENABLE_HF_TRANSFER"] = "0"  # disable Rust accelerator
os.environ.pop("HF_ENDPOINT", None)  # avoid custom mirrors

model_id = "Qwen/Qwen2-0.5B-Instruct"
local_dir = "./qwen2-0p5b-mini"

print(f"Checking if Hugging Face is reachable for model: {model_id}")
api = HfApi()
files = api.list_repo_files(model_id)
print(f"✅ Model reachable. Found {len(files)} files in repo.")

Path(local_dir).mkdir(parents=True, exist_ok=True)

# Use the plain downloader; replace deprecated args
snapshot_download(
    repo_id=model_id,
    local_dir=local_dir,
    local_dir_use_symlinks=False,
    force_download=True,
    max_workers=4,
)

print("✅ Download completed.")
