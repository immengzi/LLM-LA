# tests/test_local_queue.py
# -*- coding: utf-8 -*-
"""Unit tests for the sidecar LocalQueue (sidecar.local_queue)."""
from sidecar.local_queue import LocalQueue


def test_put_get_roundtrip():
    q = LocalQueue("pod-a")
    q.put("r1", "hello", {"k": "v"})
    item = q.get_nowait()
    assert item == ("r1", "hello", {"k": "v"})


def test_get_nowait_empty_returns_none():
    q = LocalQueue("pod-a")
    assert q.get_nowait() is None


def test_state_tracks_pending_and_inflight():
    q = LocalQueue("pod-a")
    q.put("r1", "a", {})
    q.put("r2", "b", {})
    assert q.state() == (2, 0)
    q.get_nowait()               # one becomes inflight
    assert q.state() == (1, 1)
    q.task_done()                # inflight released
    assert q.state() == (1, 0)


def test_size_is_pending_only():
    q = LocalQueue("pod-a")
    q.put("r1", "a", {})
    q.get_nowait()
    # size() reflects only pending items in the underlying queue.
    assert q.size() == 0


def test_task_done_never_negative():
    q = LocalQueue("pod-a")
    q.task_done()
    q.task_done()
    _, inflight = q.state()
    assert inflight == 0


def test_fifo_ordering():
    q = LocalQueue("pod-a")
    for i in range(3):
        q.put(f"r{i}", "p", {})
    got = [q.get_nowait()[0] for _ in range(3)]
    assert got == ["r0", "r1", "r2"]
