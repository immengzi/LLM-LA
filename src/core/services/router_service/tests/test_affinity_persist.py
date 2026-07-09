# tests/test_affinity_persist.py
# -*- coding: utf-8 -*-
"""Tests for the durable (Redis-backed) affinity map.

Covers the persistence layer end to end with an injected in-memory fake Redis
client (no real Redis / fakeredis dependency):

  * write-through claim -> Redis
  * in-memory lookup hit (hot path never hits Redis)
  * prefetch: in-memory miss -> Redis GET -> populate
  * startup reload (warm) from Redis
  * redeploy persistence: a fresh store over the SAME backing reloads the map
  * per-key TTL (ex) honored; no-expiry when ttl==0
  * in-memory cache bound eviction (durable copy kept in Redis)
  * concurrent write-through for the same key is safe (last-write-wins)

Plus the RouterState dispatch-flow integration:

  * hard-mode: pinned-to-available-other-pod -> held
  * hard-mode: pinned-to-UNAVAILABLE pod (scaled down / renamed) -> fall back
  * dispatch write-back re-claims (and persists) the new pod
  * feature-off gating: availability check is a no-op (legacy behavior)
"""
import fnmatch
import threading
import time

from router.affinity import AffinityMap
from router.affinity_store import RedisAffinityStore


# ----------------------------------------------------------------------
# In-memory fake Redis client (duck-typed subset used by the store)
# ----------------------------------------------------------------------
class _FakePipe:
    def __init__(self, backing):
        self._backing = backing
        self._ops = []

    def set(self, k, v, ex=None):
        self._ops.append((k, v, ex))
        return self

    def execute(self):
        now = time.time()
        n = len(self._ops)
        for k, v, ex in self._ops:
            self._backing[k] = (v, (now + ex) if ex else None)
        self._ops = []
        return [True] * n


class FakeRedis:
    """Minimal in-memory stand-in. `backing` can be shared to simulate a Redis
    whose AOF/RDB survived a restart (durability across a fresh client)."""

    def __init__(self, backing=None):
        self.store = backing if backing is not None else {}

    def _alive(self, k):
        item = self.store.get(k)
        if item is None:
            return None
        v, exp = item
        if exp is not None and time.time() > exp:
            self.store.pop(k, None)
            return None
        return v

    def pipeline(self, transaction=False):
        return _FakePipe(self.store)

    def get(self, k):
        return self._alive(k)

    def set(self, k, v, ex=None):
        self.store[k] = (v, (time.time() + ex) if ex else None)

    def scan_iter(self, match=None, count=None):
        for k in list(self.store.keys()):
            if self._alive(k) is None:
                continue
            if match is None or fnmatch.fnmatch(k, match):
                yield k

    def close(self):
        pass


def _wait_until(pred, timeout=2.0, interval=0.01):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return pred()


def _make_store(backing=None, ttl_seconds=0):
    return RedisAffinityStore(
        client=FakeRedis(backing=backing),
        namespace="affinity:test:model-x",
        ttl_seconds=ttl_seconds,
        writer_idle_s=0.02,
    )


# ----------------------------------------------------------------------
# Store + AffinityMap unit tests
# ----------------------------------------------------------------------
def test_write_through_claim_persists_to_redis():
    backing = {}
    store = _make_store(backing=backing)
    try:
        am = AffinityMap(300.0, store=store)
        assert am.persistent is True
        am.claim("conv1", "pod-a")
        # in-memory is immediate; Redis is async write-through
        assert am.lookup("conv1") == "pod-a"
        assert _wait_until(lambda: store.get("conv1") == "pod-a")
        # namespaced raw key present in backing
        assert "affinity:test:model-x:conv1" in backing
    finally:
        store.close()


def test_lookup_hit_is_in_memory_only():
    store = _make_store()
    try:
        am = AffinityMap(300.0, store=store)
        am.claim("c", "pod-a")
        # Corrupt the store; a hot-path lookup must NOT consult Redis.
        store._client.store.clear()
        assert am.lookup("c") == "pod-a"
    finally:
        store.close()


def test_prefetch_loads_from_redis_on_memory_miss():
    backing = {}
    store = _make_store(backing=backing)
    try:
        # Seed Redis directly (as if written by a previous router instance).
        store._client.set("affinity:test:model-x:c", "pod-z")
        am = AffinityMap(300.0, store=store)
        assert am.lookup("c") is None          # not in memory yet
        assert am.prefetch("c") == "pod-z"      # GET populates memory
        assert am.lookup("c") == "pod-z"        # now in-memory
    finally:
        store.close()


def test_warm_reloads_map_at_startup():
    backing = {}
    store = _make_store(backing=backing)
    try:
        store._client.set("affinity:test:model-x:a", "pod-1")
        store._client.set("affinity:test:model-x:b", "pod-2")
        store._client.set("other:namespace:c", "pod-3")  # different ns -> ignored
        am = AffinityMap(300.0, store=store)
        loaded = am.warm()
        assert loaded == 2
        assert am.lookup("a") == "pod-1"
        assert am.lookup("b") == "pod-2"
        assert am.lookup("c") is None
    finally:
        store.close()


def test_redeploy_persistence_reload_from_same_backing():
    # Simulate: router writes mappings, then router + client are replaced
    # (redeploy) but Redis kept its data (shared backing). New store warms it.
    backing = {}
    store1 = _make_store(backing=backing)
    try:
        am1 = AffinityMap(300.0, store=store1)
        am1.claim("conv", "pod-old")
        assert _wait_until(lambda: store1.get("conv") == "pod-old")
    finally:
        store1.close()

    # Fresh store instance (new client) over the SAME durable backing.
    store2 = _make_store(backing=backing)
    try:
        am2 = AffinityMap(300.0, store=store2)
        assert am2.warm() == 1
        assert am2.lookup("conv") == "pod-old"
    finally:
        store2.close()


def test_redis_ttl_expiry_and_no_expiry():
    # ttl>0: key expires from Redis after the window.
    store = _make_store(ttl_seconds=1)
    try:
        am = AffinityMap(300.0, store=store)
        am.claim("k", "pod-a")
        assert _wait_until(lambda: store.get("k") == "pod-a")
        assert _wait_until(lambda: store.get("k") is None, timeout=3.0)
    finally:
        store.close()

    # ttl==0: key persists (no expiry) in Redis.
    store0 = _make_store(ttl_seconds=0)
    try:
        am0 = AffinityMap(300.0, store=store0)
        am0.claim("k", "pod-a")
        assert _wait_until(lambda: store0.get("k") == "pod-a")
        time.sleep(0.3)
        assert store0.get("k") == "pod-a"
    finally:
        store0.close()


def test_cache_bound_eviction_keeps_durable_copy():
    store = _make_store()
    try:
        am = AffinityMap(300.0, store=store, cache_max=2)
        am.claim("a", "pod-a")
        time.sleep(0.005)
        am.claim("b", "pod-b")
        time.sleep(0.005)
        am.claim("c", "pod-c")  # exceeds bound -> oldest ("a") evicted from memory
        assert am.size() == 2
        assert am.lookup("a") is None            # evicted from memory
        assert _wait_until(lambda: store.get("a") == "pod-a")  # still durable in Redis
        assert am.prefetch("a") == "pod-a"       # can be brought back from Redis
    finally:
        store.close()


def test_concurrent_writethrough_same_key_is_safe():
    store = _make_store()
    try:
        am = AffinityMap(300.0, store=store)

        def worker(ep):
            for _ in range(50):
                am.claim("hot", ep)

        threads = [threading.Thread(target=worker, args=(f"pod-{i}",)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        final = am.lookup("hot")
        assert final in {"pod-0", "pod-1", "pod-2", "pod-3"}
        # Redis converges to the in-memory winner (last write).
        assert _wait_until(lambda: store.get("hot") == final)
    finally:
        store.close()


def test_no_store_is_byte_identical_legacy():
    am = AffinityMap(300.0)  # no store
    assert am.persistent is False
    am.claim("c", "pod-a")
    assert am.lookup("c") == "pod-a"
    # prefetch with no store just mirrors lookup and never raises.
    assert am.prefetch("c") == "pod-a"
    assert am.prefetch("missing") is None
    assert am.warm() == 0


# ----------------------------------------------------------------------
# RouterState dispatch-flow integration
# ----------------------------------------------------------------------
def _new_router_state_with_persist(store):
    from router.router_state import RouterState

    rs = RouterState()
    rs._affinity = AffinityMap(300.0, store=store)
    rs._affinity_persist = True
    rs._seen_endpoints = {}
    return rs


def _item(rid, key, ts=None):
    meta = {"__affinity_key__": key, "__affinity_ts__": ts if ts is not None else time.time()}
    return (rid, f"prompt-{rid}", meta["__affinity_ts__"], meta)


def test_hard_filter_holds_when_pinned_to_available_other_pod():
    store = _make_store()
    try:
        rs = _new_router_state_with_persist(store)
        rs._affinity.claim("c", "pod-b")
        rs._seen_endpoints["pod-b"] = time.time()  # pod-b is alive
        pool = [_item("r1", "c")]
        available, held = rs._affinity_filter_hard(pool, "pod-a")
        assert available == []
        assert [i[0] for i in held] == ["r1"]  # withheld for pod-b
    finally:
        store.close()


def test_hard_filter_falls_back_when_pinned_pod_unavailable():
    # Post-redeploy: mapping points at a pod name that no longer pulls.
    store = _make_store()
    try:
        rs = _new_router_state_with_persist(store)
        rs._affinity.claim("c", "pod-old")   # renamed/scaled-down pod
        # pod-old NOT in _seen_endpoints -> unavailable
        rs._seen_endpoints["pod-new"] = time.time()
        pool = [_item("r1", "c")]
        available, held = rs._affinity_filter_hard(pool, "pod-new")
        assert [i[0] for i in available] == ["r1"]  # fell back, servable now
        assert held == []
    finally:
        store.close()


def test_dispatch_writeback_persists_new_pod():
    store = _make_store()
    try:
        rs = _new_router_state_with_persist(store)
        chosen = [_item("r1", "c")]
        rs._affinity_record_dispatch(chosen, "pod-new")
        assert rs._affinity.lookup("c") == "pod-new"
        assert _wait_until(lambda: store.get("c") == "pod-new")
    finally:
        store.close()


def test_endpoint_available_gating_off_when_not_persisted():
    from router.router_state import RouterState

    rs = RouterState()
    rs._affinity_persist = False
    # With persistence off, availability is always True (legacy behavior:
    # a stale in-memory pin is still honored / held, unchanged from before).
    assert rs._endpoint_available("anything") is True


def test_endpoint_available_respects_stale_window():
    store = _make_store()
    try:
        rs = _new_router_state_with_persist(store)
        rs._seen_endpoints["pod-fresh"] = time.time()
        rs._seen_endpoints["pod-stale"] = time.time() - 10_000.0
        assert rs._endpoint_available("pod-fresh") is True
        assert rs._endpoint_available("pod-stale") is False
        assert rs._endpoint_available("pod-never") is False
    finally:
        store.close()


# ---- Single-signal readiness anchor (pull == ready) ------------------
def test_cold_start_continuity_honored_after_first_pull():
    # A warmed cross-pod mapping is NOT honored before the target pod pulls
    # (still loading weights => never health-gated a pull), and IS honored
    # right after the target pod's first (health-gated) pull.
    store = _make_store()
    try:
        rs = _new_router_state_with_persist(store)
        rs._affinity.claim("c", "podB")  # warmed mapping (e.g. from warm())

        # Before podB has ever pulled: podA's pull falls back (not held).
        avail, held = rs._affinity_filter_hard([_item("r1", "c")], "podA")
        assert [i[0] for i in avail] == ["r1"]
        assert held == []
        assert rs._endpoint_available("podB") is False

        # podB becomes ready and issues its first (health-gated) pull. The pull
        # stamps _seen_endpoints even though the queue is empty (stamp happens
        # before the empty-queue early return).
        rs.pull_for_endpoint("podB", want=1)
        assert rs._endpoint_available("podB") is True

        # Now podA's pull withholds the pinned item for podB (pin honored).
        avail2, held2 = rs._affinity_filter_hard([_item("r2", "c")], "podA")
        assert avail2 == []
        assert [i[0] for i in held2] == ["r2"]
    finally:
        store.close()


def test_never_ready_pod_is_never_honored():
    # A mapping to a pod that never becomes ready (never pulls) always reads
    # miss -> fall back to LB; it is never withheld for that ghost pod.
    store = _make_store()
    try:
        rs = _new_router_state_with_persist(store)
        rs._affinity.claim("c", "ghost")   # ghost never pulls
        rs.pull_for_endpoint("podA", want=1)  # a different, live pod pulls
        assert rs._endpoint_available("ghost") is False
        avail, held = rs._affinity_filter_hard([_item("r1", "c")], "podA")
        assert [i[0] for i in avail] == ["r1"]
        assert held == []
    finally:
        store.close()


def test_scale_down_ages_out_past_stale():
    from router.config import get_config

    stale = float(get_config().AFFINITY_ENDPOINT_STALE_S)
    store = _make_store()
    try:
        rs = _new_router_state_with_persist(store)
        rs._affinity.claim("c", "podB")

        # podB last pulled longer ago than STALE_S -> aged out -> fall back.
        rs._seen_endpoints["podB"] = time.time() - (stale + 10.0)
        assert rs._endpoint_available("podB") is False
        avail, held = rs._affinity_filter_hard([_item("r1", "c")], "podA")
        assert [i[0] for i in avail] == ["r1"]
        assert held == []

        # A fresh pull re-validates it.
        rs._seen_endpoints["podB"] = time.time()
        assert rs._endpoint_available("podB") is True
    finally:
        store.close()


def test_stale_default_is_1800():
    from router.config import get_config

    assert float(get_config().AFFINITY_ENDPOINT_STALE_S) == 1800.0


def test_feature_off_hard_filter_is_byte_identical_legacy():
    # With persistence OFF, _endpoint_available is a no-op (always True), so the
    # hard filter behaves exactly as the legacy in-memory path: a pin to a pod
    # that was never observed is still honored (held), NOT dropped by an
    # availability check.
    store = _make_store()
    try:
        rs = _new_router_state_with_persist(store)
        rs._affinity.claim("c", "podB")   # podB never seen pulling
        rs._affinity_persist = False       # persist OFF -> legacy path
        assert rs._endpoint_available("anything") is True
        avail, held = rs._affinity_filter_hard([_item("r1", "c")], "podA")
        assert avail == []
        assert [i[0] for i in held] == ["r1"]  # held for podB (legacy behavior)
    finally:
        store.close()
