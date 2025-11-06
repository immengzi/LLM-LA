#!/usr/bin/env python3
"""
download_lmsys_dataset.py
Downloads the LMSYS chat dataset locally for offline use.
After running, set HF_DATASET_PATH in your config to the saved folder.
"""

from datasets import load_dataset
import os

# === configure where to save ===
LOCAL_DATASET_PATH = "/mnt/nvme1/saeid/datasets/lmsys_chat_1m"
SPLIT = "train"
DATASET_NAME = "lmsys/lmsys-chat-1m"

print(f"[DL] Downloading {DATASET_NAME}:{SPLIT} to {LOCAL_DATASET_PATH} ...")
os.makedirs(LOCAL_DATASET_PATH, exist_ok=True)

# download and save to disk (non-streaming)
ds = load_dataset(DATASET_NAME, split=SPLIT, streaming=False)
ds.save_to_disk(LOCAL_DATASET_PATH)

print(f"[DL] Done. Dataset saved at: {LOCAL_DATASET_PATH}")
print(f"[DL] You can now set HF_DATASET_PATH: \"{LOCAL_DATASET_PATH}\" in your config.")
