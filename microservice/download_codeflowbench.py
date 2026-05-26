#!/usr/bin/env python3
"""Download CodeFlowBench-2505 dataset from HuggingFace."""
from datasets import load_dataset

DATASET_NAME = "WaterWang-001/CodeFlowBench-2505"
LOCAL_PATH = "./data/codeflowbench"
SPLIT = "train"

print(f"Downloading {DATASET_NAME} (split={SPLIT})...")
ds = load_dataset(DATASET_NAME, split=SPLIT, streaming=False)
ds.save_to_disk(LOCAL_PATH)
print(f"Downloaded {len(ds)} examples to {LOCAL_PATH}")