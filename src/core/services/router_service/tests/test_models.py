# tests/test_models.py
# -*- coding: utf-8 -*-
"""Unit tests for the Pydantic request/response models (router.models)."""
import pytest
from pydantic import ValidationError

from router.models import EnqueueRequest, PullRequest, JobItem, PullResponse, now_s


def test_enqueue_request_defaults():
    r = EnqueueRequest(prompt="hi")
    assert r.prompt == "hi"
    assert r.meta == {}
    assert r.model == ""
    assert r.slo_type is None


def test_enqueue_request_requires_prompt():
    with pytest.raises(ValidationError):
        EnqueueRequest()


def test_enqueue_request_carries_slo_fields():
    r = EnqueueRequest(prompt="x", slo_type="ttft", slo_ttft_ms=500,
                       output_len_hint=128)
    assert r.slo_type == "ttft"
    assert r.slo_ttft_ms == 500
    assert r.output_len_hint == 128


def test_pull_request_and_response_roundtrip():
    req = PullRequest(endpoint="pod-a", want=4)
    assert req.model == ""
    item = JobItem(req_id="r1", prompt="p", t_enq_client=123.0)
    resp = PullResponse(items=[item])
    assert resp.items[0].req_id == "r1"
    assert resp.items[0].meta == {}


def test_pull_request_want_coerces_int():
    req = PullRequest(endpoint="pod-a", want="5")  # pydantic coerces str->int
    assert req.want == 5


def test_pull_request_optional_kv_usage():
    req = PullRequest(endpoint="pod-a", want=1)
    assert req.kv_usage is None
    req2 = PullRequest(endpoint="pod-a", want=1, kv_usage=0.42)
    assert req2.kv_usage == pytest.approx(0.42)


def test_now_s_is_time():
    a = now_s()
    assert isinstance(a, float)
    assert a > 0
