# tests/conftest.py
# -*- coding: utf-8 -*-
"""Shared fixtures for the sidecar test suite."""
import pytest


@pytest.fixture
def fake_redis():
    """An in-memory Redis stand-in (fakeredis) with decode_responses semantics."""
    import fakeredis
    return fakeredis.FakeStrictRedis(decode_responses=True)
