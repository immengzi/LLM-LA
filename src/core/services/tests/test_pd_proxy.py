# tests/test_pd_proxy.py
# -*- coding: utf-8 -*-
"""Unit tests for the prefill/decode (P/D) proxy shipped in the
41-vllm-pd-disagg.yaml ConfigMap.

The proxy is embedded as a ConfigMap data key (`pd_proxy.py`), not a standalone
module, so we extract the block from the chart template and inspect it with the
`ast` module. This keeps the test self-contained (no aiohttp needed) while still
guarding the proxy against syntax breakage and verifying its structure.
"""
import ast
import os

HERE = os.path.dirname(__file__)
TEMPLATE = os.path.normpath(
    os.path.join(HERE, "..", "..", "vllm-kv-stack", "templates", "41-vllm-pd-disagg.yaml")
)


def _extract_proxy_source() -> str:
    """Pull the `pd_proxy.py: |` block out of the chart template, de-indented."""
    lines = open(TEMPLATE, encoding="utf-8").read().split("\n")
    start = None
    for i, ln in enumerate(lines):
        if ln.strip() == "pd_proxy.py: |":
            start = i + 1
            break
    assert start is not None, "pd_proxy.py block not found in template"
    body = []
    for ln in lines[start:]:
        if ln.strip() == "":
            body.append("")
            continue
        indent = len(ln) - len(ln.lstrip())
        if indent < 4:  # dedent past the block scalar -> end of the embedded script
            break
        body.append(ln[4:])
    return "\n".join(body)


PROXY_SRC = _extract_proxy_source()


def test_proxy_compiles():
    # The embedded proxy must be valid Python (guards against template breakage).
    compile(PROXY_SRC, "pd_proxy.py", "exec")


def test_proxy_defines_expected_handlers():
    defs = (ast.FunctionDef, ast.AsyncFunctionDef)  # handlers are async defs
    names = {n.name for n in ast.walk(ast.parse(PROXY_SRC)) if isinstance(n, defs)}
    for fn in ("health", "handle", "_relay", "_passthrough", "main"):
        assert fn in names, f"proxy missing expected function {fn!r}"


def test_prefill_phase_caps_to_single_token():
    # The prefill phase must generate exactly one token (compute+publish KV only).
    assert 'prefill_body["max_tokens"] = 1' in PROXY_SRC
