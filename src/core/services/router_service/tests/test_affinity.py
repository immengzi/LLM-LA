# tests/test_affinity.py
# -*- coding: utf-8 -*-
"""Unit tests for conversation key-affinity helpers.

These cover the pure pieces (key derivation + AffinityMap) that do not require
the full router dependency stack, so they run without pydantic/prometheus.
"""
import time

from router.affinity import AffinityMap, derive_affinity_key


def test_key_stable_across_turns():
    turn1 = [
        {"role": "system", "content": "You are helpful"},
        {"role": "user", "content": "hi"},
        {"role": "assistant", "content": "hello"},
    ]
    turn2 = turn1 + [{"role": "user", "content": "second turn"}]
    k1 = derive_affinity_key("m", turn1)
    k2 = derive_affinity_key("m", turn2)
    assert k1 and k1 == k2


def test_content_block_array_matches_string():
    block = [{"role": "user", "content": [{"type": "text", "text": "hi"}]}]
    plain = [{"role": "user", "content": "hi"}]
    assert derive_affinity_key("m", block) == derive_affinity_key("m", plain)


def test_no_user_message_returns_none():
    assert derive_affinity_key("m", [{"role": "system", "content": "x"}]) is None
    assert derive_affinity_key("m", []) is None


def test_model_is_part_of_key():
    msgs = [{"role": "user", "content": "hi"}]
    assert derive_affinity_key("m1", msgs) != derive_affinity_key("m2", msgs)


def test_affinity_map_claim_lookup_and_ttl():
    am = AffinityMap(0.05)
    assert am.lookup("k") is None
    am.claim("k", "ep-a")
    assert am.lookup("k") == "ep-a"
    assert am.size() == 1
    time.sleep(0.07)
    assert am.lookup("k") is None


def test_affinity_map_prune():
    am = AffinityMap(0.01)
    am.claim("a", "ep")
    time.sleep(0.02)
    am.prune()
    assert am.size() == 0
