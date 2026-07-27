# tests/test_direct_delivery.py
# -*- coding: utf-8 -*-
"""Integration test for the sidecar-less / external direct-delivery dispatch path.

Sidecar-less central-push reuses ``ExternalPushDispatcher`` over a k8s-backed
registry. This drives that dispatcher against a real ``RouterState`` (central
queue + pull scheduling + in-flight accounting) with a fake registry and a fake
direct-vLLM client, proving the full loop:

    enqueue -> dispatch pass -> pull_for_endpoint (in-flight++) ->
    direct deliver -> ingest result -> release_inflight (in-flight--)

No Kubernetes, Redis, ZMQ or network is involved.
"""
import asyncio

from router.router_state import RouterState
from router.external_push import ExternalPushDispatcher


class _FakeRegistry:
    def __init__(self, ids):
        self._ids = list(ids)

    def healthy_ids(self):
        return list(self._ids)

    async def refresh_health(self, *, force=False):
        return None

    def get(self, ep_id):  # unused: the fake client doesn't need the endpoint
        return None


class _FakeClient:
    def __init__(self):
        self.delivered = []

    async def deliver(self, ep, req_id, prompt, meta):
        self.delivered.append((ep, req_id, prompt))
        return {"output": f"echo:{prompt}", "finish_reason": "stop",
                "endpoint_id": ep}


def _run_pass(disp):
    async def _go():
        await disp._dispatch_pass()
        # Deliveries run as background tasks; wait for them to ingest.
        if disp._inflight_tasks:
            await asyncio.gather(*list(disp._inflight_tasks))
    asyncio.run(_go())


def test_direct_delivery_end_to_end_releases_inflight():
    rs = RouterState()
    rids = [rs.enqueue(f"p{i}", None, {}, model="") for i in range(3)]

    client = _FakeClient()
    ingested = []

    def ingest(payload):
        ingested.append(payload)
        rs.release_inflight(payload["req_id"], payload.get("endpoint"))

    disp = ExternalPushDispatcher(
        rs, _FakeRegistry(["pod-a"]), client, ingest, cap=8, interval_s=0.05,
    )
    _run_pass(disp)

    # All three requests were delivered directly to the one healthy endpoint...
    assert len(client.delivered) == 3
    assert {d[1] for d in client.delivered} == set(rids)
    assert all(d[0] == "pod-a" for d in client.delivered)
    # ... ingested back through the /result path ...
    assert len(ingested) == 3
    # ... and in-flight fully released (per-pod cap reclaimed), queue drained.
    assert rs.endpoint_inflight_snapshot().get("pod-a", 0) == 0
    assert rs.size() == 0


def test_direct_delivery_respects_per_pod_cap():
    rs = RouterState()
    for i in range(10):
        rs.enqueue(f"p{i}", None, {}, model="")

    client = _FakeClient()

    # Ingest WITHOUT releasing in-flight, so the cap stays consumed and the pass
    # can never pull more than `cap` items for the pod.
    def ingest(payload):
        pass

    disp = ExternalPushDispatcher(
        rs, _FakeRegistry(["pod-a"]), client, ingest, cap=4, interval_s=0.05,
    )
    _run_pass(disp)

    assert len(client.delivered) == 4
    assert rs.endpoint_inflight_snapshot().get("pod-a", 0) == 4
    # Remaining 6 stay queued for a later pass.
    assert rs.size() == 6
