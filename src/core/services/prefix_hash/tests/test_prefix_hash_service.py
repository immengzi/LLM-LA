# tests/test_prefix_hash_service.py
# -*- coding: utf-8 -*-
"""Tests for the standalone (external) prefix-hash service.

This service builds on vLLM's Request block hasher and therefore needs `vllm`
+ `transformers` + a local model. Those are not available in the lightweight
test environment (or on non-NPU CI runners), so the whole module is skipped
unless the heavy deps import cleanly. The default *inline* hashing path
(router/prefix_hash.py) is fully covered by the router unit tests instead.
"""
import pytest

pytest.importorskip("vllm", reason="vLLM not installed in the test environment")
pytest.importorskip("transformers", reason="transformers not installed")


def test_block_hash_computer_importable():
    # If the heavy deps are present, at least verify the public surface exists.
    from prefix_hash_estimation import BlockHashComputer  # noqa: F401

    assert hasattr(BlockHashComputer, "compute")
    assert hasattr(BlockHashComputer, "compute_from_messages")
