"""Unit tests for the P/D proxy transition semantics (files/pd_proxy.py)."""

from __future__ import annotations

import asyncio
import importlib.util
import time
from pathlib import Path
from typing import Awaitable, Callable

import pytest

aiohttp = pytest.importorskip("aiohttp")

REPO_ROOT = Path(__file__).resolve().parents[4]
PROXY = REPO_ROOT / "src" / "core" / "vllm-kv-stack" / "files" / "pd_proxy.py"


def _run_with_proxy(  # type: ignore[no-untyped-def]
    scenario: "Callable[[object], Awaitable[object]]",
):
    """Load the proxy module and run one scenario on a dedicated event loop.

    The module creates ``asyncio`` primitives at import time; importing it while
    another test's loop is still current would bind them to a closed loop, so
    each scenario gets a fresh loop and loads the module inside it.
    """
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        spec = importlib.util.spec_from_file_location("pd_proxy_under_test", PROXY)
        proxy = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(proxy)
        return loop.run_until_complete(scenario(proxy))
    finally:
        loop.close()
        asyncio.set_event_loop(None)


def test_planned_unavailability_is_503_with_retry_after() -> None:
    async def scenario(proxy):
        response = await proxy._unavailable("pd-proxy is draining; retry later")
        return response, await proxy._metrics.snapshot()

    response, snapshot = _run_with_proxy(scenario)
    assert response.status == 503
    assert response.headers["Retry-After"] == "1"
    assert snapshot["reject_503_total"] == 1


def test_inflight_request_ids_are_deduplicated() -> None:
    async def scenario(proxy):
        results = [
            await proxy._register_inflight("rid-1"),
            await proxy._register_inflight("rid-1"),
        ]
        await proxy._clear_inflight("rid-1")
        results.append(await proxy._register_inflight("rid-1"))
        await proxy._clear_inflight("rid-1")
        return results

    assert _run_with_proxy(scenario) == [True, False, True]


def test_reject_counters_are_exposed_in_metrics() -> None:
    async def scenario(proxy):
        await proxy._metrics.inc("reject_409_total", 1.0)
        return await proxy._metrics.snapshot(), (await proxy.metrics(None)).text

    snapshot, text = _run_with_proxy(scenario)
    assert snapshot["reject_409_total"] == 1
    assert "pd_proxy_reject_409_total 1" in text


def test_drain_wait_is_queued_and_counted() -> None:
    async def scenario(proxy):
        proxy._drain_state["paused"] = True
        started = time.monotonic()

        async def release():
            await asyncio.sleep(0.2)
            async with proxy._drain_cond:
                proxy._drain_state["paused"] = False
                proxy._drain_cond.notify_all()

        releaser = asyncio.create_task(release())
        await proxy._wait_for_drain_counted()
        await releaser
        return time.monotonic() - started, await proxy._metrics.snapshot()

    waited, snapshot = _run_with_proxy(scenario)
    assert waited >= 0.15
    assert snapshot["drain_wait_count"] == 1
    assert snapshot["drain_wait_seconds"] > 0


def test_drain_timeout_raises_drain_timeout() -> None:
    async def scenario(proxy):
        proxy.DRAIN_MAX_WAIT = 0.05
        proxy._drain_state["paused"] = True
        try:
            await proxy._wait_for_drain()
        finally:
            proxy._drain_state["paused"] = False
            async with proxy._drain_cond:
                proxy._drain_cond.notify_all()

    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        spec = importlib.util.spec_from_file_location("pd_proxy_under_test", PROXY)
        proxy = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(proxy)
        with pytest.raises(proxy.DrainTimeout):
            loop.run_until_complete(scenario(proxy))
    finally:
        loop.close()
        asyncio.set_event_loop(None)
