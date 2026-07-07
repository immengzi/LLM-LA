#!/usr/bin/env python3
"""Idempotently guard negative Prometheus counter increments in vLLM.

Fixes the crash:
    ValueError: Counters can only be incremented by non-negative amounts.
    at vllm/v1/metrics/loggers.py

Triggered when an LMCache rollback / preemption under KV-cache pressure
produces a negative per-source prompt-token delta (see vLLM issues #36755,
#36533). Prometheus Counter.inc() rejects negative amounts and the unhandled
ValueError kills the engine.

This guards the three counters whose value is derived from the brittle
cache-accounting subtraction and can therefore go negative:
    - counter_prompt_tokens_by_source  (the crash site)
    - counter_prompt_tokens_cached
    - counter_prompt_tokens_recomputed

Scope note: only these three are guarded because they are the only counters in
record() that can be negative on this (P2P / kv_both host-staging) topology.
Pure event counts (prefix/mm cache queries/hits, preempted/corrupted reqs,
generation tokens) are mathematically non-negative. `counter_prompt_tokens`
can only go negative on disaggregated P/D (NixlConnector, issue #38839), which
this deployment does not run. See infra/patches/README.md audit notes if that
changes.

Safe to run multiple times (idempotent).

Usage:
    python3 apply_metrics_fix.py [/path/to/vllm/v1/metrics/loggers.py]
"""

import ast
import sys
from pathlib import Path

DEFAULT_TARGET = "/vllm-workspace/vllm/vllm/v1/metrics/loggers.py"

MARKER = "            delta = pts.get_by_source(source)\n"

OLD = (
    "        for source in PromptTokenStats.ALL_SOURCES:\n"
    "            self.counter_prompt_tokens_by_source[source][engine_idx].inc(\n"
    "                pts.get_by_source(source)\n"
    "            )\n"
    "        self.counter_prompt_tokens_cached[engine_idx].inc(pts.cached_tokens)\n"
    "        self.counter_prompt_tokens_recomputed[engine_idx].inc(pts.recomputed_tokens)\n"
)

NEW = (
    "        for source in PromptTokenStats.ALL_SOURCES:\n"
    "            delta = pts.get_by_source(source)\n"
    "            if delta > 0:\n"
    "                self.counter_prompt_tokens_by_source[source][engine_idx].inc(\n"
    "                    delta)\n"
    "        if pts.cached_tokens > 0:\n"
    "            self.counter_prompt_tokens_cached[engine_idx].inc(\n"
    "                pts.cached_tokens)\n"
    "        if pts.recomputed_tokens > 0:\n"
    "            self.counter_prompt_tokens_recomputed[engine_idx].inc(\n"
    "                pts.recomputed_tokens)\n"
)


def main() -> int:
    target = Path(sys.argv[1] if len(sys.argv) > 1 else DEFAULT_TARGET)

    if not target.is_file():
        print(f"ERROR: target not found: {target}", file=sys.stderr)
        return 2

    text = target.read_text(encoding="utf-8")

    if MARKER in text:
        print(f"Already patched: {target}")
        return 0

    if OLD not in text:
        print(
            f"ERROR: expected block not found in {target}. "
            "vLLM version may differ; inspect the file manually.",
            file=sys.stderr,
        )
        return 3

    text = text.replace(OLD, NEW, 1)

    # Fail loudly if we somehow produced invalid Python.
    ast.parse(text)
    target.write_text(text, encoding="utf-8")
    print(f"Patched OK: {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
