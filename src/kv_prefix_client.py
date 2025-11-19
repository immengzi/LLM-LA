# -*- coding: utf-8 -*-
# kv_prefix_client.py
"""
Small sync client for the CPU KV-prefix hash service.

Contract:
    compute_hashes_for_prompt(prompt: str, timeout: float = 10.0)
        -> (block_hashes: list[int], token_ids: list[int])

It calls the same /compute_hashes HTTP endpoint you used in
client_kv_routing_scenario.py.
"""

import os
from typing import List, Tuple

import requests

# TODO get these from config
# Detect in-cluster vs local (same logic as your demo script)
RUNNING_IN_CLUSTER = os.getenv("KUBERNETES_SERVICE_HOST") is not None

if RUNNING_IN_CLUSTER:
    DEFAULT_HASH_SERVICE_URL = (
        "http://vllm-cpu-hash.vllm.svc.cluster.local:9095/compute_hashes"
    )
else:
    # NodePort or port-forward on your machine
    DEFAULT_HASH_SERVICE_URL = "http://127.0.0.1:30095/compute_hashes"

HASH_SERVICE_URL = os.getenv("HASH_SERVICE_URL", DEFAULT_HASH_SERVICE_URL)


def compute_hashes_for_prompt(
    prompt: str,
    timeout: float = 10.0,
) -> Tuple[List[int], List[int]]:
    """
    Call the CPU hash service and return (block_hashes, token_ids).

    The service is expected to accept:
        POST HASH_SERVICE_URL
        {
            "messages": [{"role": "user", "content": "<prompt>"}]
        }

    And return:
        {
            "block_hashes": [int, ...],
            "token_ids": [int, ...]
        }
    """
    payload = {
        "messages": [
            {"role": "user", "content": prompt},
        ]
    }

    resp = requests.post(HASH_SERVICE_URL, json=payload, timeout=timeout)
    resp.raise_for_status()
    data = resp.json() or {}

    block_hashes = data.get("block_hashes") or []
    token_ids = data.get("token_ids") or []

    # Normalize to int lists
    block_hashes = [int(bh) for bh in block_hashes]
    token_ids = [int(t) for t in token_ids]

    return block_hashes, token_ids
