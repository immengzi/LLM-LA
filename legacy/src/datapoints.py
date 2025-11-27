# -*- coding: utf-8 -*-
"""
Show how many datapoints exist in each split of lmsys/lmsys-chat-1m.
"""

from datasets import load_dataset

def show_lmsys_split_counts(dataset_name="lmsys/lmsys-chat-1m"):
    print(f"[LMSYS] Checking dataset: {dataset_name}")
    all_splits = load_dataset(dataset_name)
    for split_name, ds in all_splits.items():
        print(f"  • {split_name:<10}: {len(ds):,} examples")

if __name__ == "__main__":
    show_lmsys_split_counts()
