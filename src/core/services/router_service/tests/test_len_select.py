# tests/test_len_select.py
# -*- coding: utf-8 -*-
"""Unit tests for length-aware ordering (router.len_select)."""
from router.len_select import select_len_aware
from router.predictors import SimpleLengthPredictor


def _cand(rid, prompt):
    return (rid, prompt, 0.0, {})


def test_short_first_orders_by_predicted_length():
    pred = SimpleLengthPredictor()  # predict_out_tokens = len(prompt)//2
    cands = [_cand("long", "x" * 100), _cand("short", "xx"), _cand("mid", "x" * 20)]
    out = select_len_aware(cands, pred, "short_first")
    assert [c[0] for c in out] == ["short", "mid", "long"]


def test_long_first_reverses_order():
    pred = SimpleLengthPredictor()
    cands = [_cand("short", "xx"), _cand("long", "x" * 100), _cand("mid", "x" * 20)]
    out = select_len_aware(cands, pred, "long_first")
    assert [c[0] for c in out] == ["long", "mid", "short"]


def test_preserves_tuple_shape():
    pred = SimpleLengthPredictor()
    cands = [_cand("a", "aaaa"), _cand("b", "bb")]
    out = select_len_aware(cands, pred, "short_first")
    for item in out:
        assert len(item) == 4  # (req_id, prompt, t_enq, meta)


def test_empty_pool():
    assert select_len_aware([], SimpleLengthPredictor(), "short_first") == []
