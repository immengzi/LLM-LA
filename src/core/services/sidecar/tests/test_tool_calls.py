# tests/test_tool_calls.py
# -*- coding: utf-8 -*-
"""Unit tests for streaming tool-call accumulation (sidecar.vllm_client)."""
from sidecar.vllm_client import _merge_tool_call_delta, _complete_tool_calls


def test_merge_accumulates_arguments_across_chunks():
    acc = {}
    _merge_tool_call_delta(acc, [
        {"index": 0, "id": "call_1", "type": "function",
         "function": {"name": "get_weather", "arguments": '{"ci'}},
    ])
    _merge_tool_call_delta(acc, [
        {"index": 0, "function": {"arguments": 'ty": "SF"}'}},
    ])
    completed = _complete_tool_calls(acc)
    assert len(completed) == 1
    assert completed[0]["id"] == "call_1"
    assert completed[0]["function"]["name"] == "get_weather"
    assert completed[0]["function"]["arguments"] == '{"city": "SF"}'


def test_merge_handles_multiple_indices():
    acc = {}
    _merge_tool_call_delta(acc, [
        {"index": 0, "function": {"name": "a", "arguments": "{}"}},
        {"index": 1, "function": {"name": "b", "arguments": "{}"}},
    ])
    completed = _complete_tool_calls(acc)
    assert [c["function"]["name"] for c in completed] == ["a", "b"]


def test_complete_skips_entries_without_name():
    acc = {}
    _merge_tool_call_delta(acc, [
        {"index": 0, "function": {"arguments": "{}"}},  # no name -> dropped
    ])
    assert _complete_tool_calls(acc) == []


def test_merge_ignores_non_list_input():
    acc = {}
    _merge_tool_call_delta(acc, None)
    _merge_tool_call_delta(acc, {"not": "a list"})
    assert _complete_tool_calls(acc) == []


def test_merge_defaults_index_from_position():
    acc = {}
    # No explicit index -> falls back to enumerate position.
    _merge_tool_call_delta(acc, [{"function": {"name": "x", "arguments": "{}"}}])
    completed = _complete_tool_calls(acc)
    assert len(completed) == 1
    assert completed[0]["function"]["name"] == "x"


def test_complete_orders_by_index():
    acc = {}
    _merge_tool_call_delta(acc, [
        {"index": 2, "function": {"name": "c", "arguments": ""}},
        {"index": 0, "function": {"name": "a", "arguments": ""}},
        {"index": 1, "function": {"name": "b", "arguments": ""}},
    ])
    assert [c["function"]["name"] for c in _complete_tool_calls(acc)] == ["a", "b", "c"]
