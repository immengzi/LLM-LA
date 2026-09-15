# -*- coding: utf-8 -*-
"""Unit tests for the prefill/decode (P/D) proxy.

The proxy lives as a standalone module (`vllm-kv-stack/files/pd_proxy.py`) and
is mounted into proxy pods through the `pd_proxy.py` data key of the
41-vllm-pd-disagg.yaml ConfigMap. We inspect the module with the `ast` module,
which keeps the test self-contained (no aiohttp needed) while guarding the
proxy against syntax breakage, structure drift, and chart wiring regressions.
"""
import ast
import os
import re

HERE = os.path.dirname(__file__)
CHART = os.path.normpath(os.path.join(HERE, "..", "..", "vllm-kv-stack"))
PROXY_FILE = os.path.join(CHART, "files", "pd_proxy.py")
TEMPLATE = os.path.join(CHART, "templates", "41-vllm-pd-disagg.yaml")


def _proxy_source() -> str:
    with open(PROXY_FILE, encoding="utf-8") as f:
        return f.read()


def _template_source() -> str:
    with open(TEMPLATE, encoding="utf-8") as f:
        return f.read()


PROXY_SRC = _proxy_source()
TEMPLATE_SRC = _template_source()


def test_proxy_is_wired_into_pd_disagg_configmap():
    # The ConfigMap must embed the standalone proxy file rather than a stale
    # inline copy, so the shipped code stays in sync with what is tested.
    assert '.Files.Get "files/pd_proxy.py"' in TEMPLATE_SRC
    assert "pd_proxy.py: |" in TEMPLATE_SRC


def test_proxy_checksum_annotation_is_not_nested_under_labels():
    # Regression: checksum/config must be an annotation (sibling of labels).
    # When nested under labels, helm 3-way merge fails with
    # "cannot unmarshal object into ... labels of type string".
    assert re.search(
        r"model: \{\{ \$modelName \}\}\n      annotations:\n        checksum/config:",
        TEMPLATE_SRC,
    )


def test_proxy_compiles():
    # The embedded proxy must be valid Python (guards against template breakage).
    compile(PROXY_SRC, "pd_proxy.py", "exec")


def test_proxy_defines_expected_handlers():
    defs = (ast.FunctionDef, ast.AsyncFunctionDef)  # handlers are async defs
    names = {n.name for n in ast.walk(ast.parse(PROXY_SRC)) if isinstance(n, defs)}
    for fn in ("health", "handle", "_relay", "_passthrough", "main", "status", "drain", "_wait_for_drain"):
        assert fn in names, f"proxy missing expected function {fn!r}"


def test_prefill_phase_caps_to_single_token():
    # The prefill phase must generate exactly one token (compute+publish KV only).
    assert 'prefill_body["max_tokens"] = 1' in PROXY_SRC
    # min_tokens > 1 would make vLLM reject max_tokens=1 with 400 and silently
    # degrade the request to decode-only; the prefill phase must drop the floor.
    assert 'prefill_body["min_tokens"] = 0' in PROXY_SRC


def test_proxy_registers_drain_and_status_endpoints():
    assert 'app.router.add_get("/status", status)' in PROXY_SRC
    assert 'app.router.add_post("/drain", drain)' in PROXY_SRC
    assert "DrainTimeout" in PROXY_SRC


def test_proxy_exposes_decode_inflight_metric():
    assert "pd_proxy_decode_inflight" in PROXY_SRC
    assert "pd_proxy_decode_requests_total" in PROXY_SRC


def test_proxy_retries_prefill_once_before_fallback():
    # Phase-1 retry loop keeps disaggregation alive through terminating
    # prefill pods; the retry picks a different base and skips recently
    # failed ones, and 4xx client errors are intentionally not retried.
    assert "for attempt, base in enumerate(prefill_candidates, start=1):" in PROXY_SRC
    assert "400 <= prefill_status < 500" in PROXY_SRC
    # The candidate list comes from the prefill pool with the base that just
    # failed excluded (skip=skip | {first}) and the recent-failure set applied,
    # so a retry can never land back on the pod that is going away.
    assert "_candidates(_prefill_rr, PREFILL, prefill_base, failed)" in PROXY_SRC
    assert "skip=skip | {first}" in PROXY_SRC
    assert "await _remember_failure(base)" in PROXY_SRC


def test_proxy_decode_retry_skips_failed_bases():
    # Phase-2 retry must not blindly land on the next round-robin pod when
    # several decode pods are draining at once: each base is tried at most
    # once, failed bases are remembered briefly, and picks avoid known
    # failures so a retry can reach a surviving instance.
    assert "for base in decode_candidates:" in PROXY_SRC
    assert "_decode_rr, DECODE, decode_base, failed" in PROXY_SRC
    # "at most once per base" is the de-duplication inside _candidates.
    assert "if base not in picked:" in PROXY_SRC
    assert "await _remember_failure(base)" in PROXY_SRC
    assert "_cycle_candidates" in PROXY_SRC


def test_proxy_drain_window_blocks_new_pd_requests():
    # Planned unavailability answers 503 + Retry-After through one helper, so
    # every drain-timeout path also increments reject_503_total.
    assert '_unavailable("pd-proxy is draining; retry later")' in PROXY_SRC
    assert '{"error": message}, status=503, headers={"Retry-After": "1"}' in PROXY_SRC
    assert 'await _metrics.inc("reject_503_total", 1.0)' in PROXY_SRC
