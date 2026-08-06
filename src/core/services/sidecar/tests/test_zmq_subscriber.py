# tests/test_zmq_subscriber.py
# -*- coding: utf-8 -*-
"""Unit tests for KV event decode + Redis projection (sidecar.zmq_subscriber).

The ZMQ socket loop itself is not exercised (it needs a live publisher); instead
we test the pure DNS/endpoint resolution, the msgpack wire decode, and the Redis
projection logic (_handle_batch) against an in-memory fakeredis.
"""
import msgspec
import pytest
from unittest.mock import ANY, MagicMock

from sidecar import zmq_subscriber as Z
from sidecar import zmq_subscriber as subscriber_module
from sidecar.kv_redis import clear_pod_ownership
from sidecar.zmq_subscriber import (
    AllBlocksCleared,
    BlockRemoved,
    BlockStored,
    DiscoveryMismatch,
    KVEventBatch,
    KVSubscriber,
    PublisherEndpoint,
    ReplayResult,
    _decode_sequence,
    _resolve_topic,
    _resolve_zmq_endpoints,
)


def test_resolve_endpoints_single_dp_is_local_only():
    resolved, pending = _resolve_zmq_endpoints(
        "vllm-leader", "127.0.0.1", 5557, dp_size=1, dp_size_local=1
    )
    assert resolved == ["tcp://127.0.0.1:5557"]
    assert pending == []


def test_resolve_endpoints_unresolvable_workers_go_pending(monkeypatch):
    import socket

    def _boom(_fqdn):
        raise socket.gaierror("no dns in test")

    monkeypatch.setattr(Z.socket, "gethostbyname", _boom)
    resolved, pending = _resolve_zmq_endpoints(
        "vllm-leader", "127.0.0.1", 5557, dp_size=3, dp_size_local=1
    )
    # Local rank 0 always resolves; ranks 1..2 are pending DNS retry.
    assert resolved == ["tcp://127.0.0.1:5557"]
    assert [rank for rank, _f, _p in pending] == [1, 2]
    # Port offset = base + rank * dp_size_local.
    assert [port for _r, _f, port in pending] == [5558, 5559]


def test_msgpack_roundtrip_decode():
    batch = KVEventBatch(
        ts=123.0,
        events=[
            BlockStored(block_hashes=[10, 20], parent_block_hash=None,
                        token_ids=[1, 2, 3], block_size=16, lora_id=None),
            BlockRemoved(block_hashes=[10]),
            AllBlocksCleared(),
        ],
    )
    payload = msgspec.msgpack.encode(batch)
    decoder = msgspec.msgpack.Decoder(type=KVEventBatch)
    decoded = decoder.decode(payload)
    assert decoded.ts == 123.0
    assert isinstance(decoded.events[0], BlockStored)
    assert decoded.events[0].block_hashes == [10, 20]
    assert isinstance(decoded.events[1], BlockRemoved)
    assert isinstance(decoded.events[2], AllBlocksCleared)


def _subscriber_with_fake_redis(fake_redis):
    sub = KVSubscriber()
    sub.redis = fake_redis
    sub.model = "m"
    sub.pod_name = "pod-a"
    return sub


def test_handle_block_stored_writes_ownership(fake_redis):
    sub = _subscriber_with_fake_redis(fake_redis)
    batch = KVEventBatch(ts=0.0, events=[
        BlockStored(block_hashes=[111, 222], parent_block_hash=None,
                    token_ids=[1], block_size=16, lora_id=None),
    ])
    sub._apply_batch(batch)

    assert fake_redis.hget("m:kvblock:111", "pod-a") is not None
    assert fake_redis.sismember("m:podblocks:pod-a", "111")
    assert fake_redis.hget("m:kvblocks", "222") == "m:kvblock:222"


def test_handle_block_removed_clears_ownership(fake_redis):
    sub = _subscriber_with_fake_redis(fake_redis)
    sub._apply_batch(KVEventBatch(ts=0.0, events=[
        BlockStored(block_hashes=[111], parent_block_hash=None,
                    token_ids=[1], block_size=16, lora_id=None),
    ]))
    sub._apply_batch(KVEventBatch(ts=0.0, events=[
        BlockRemoved(block_hashes=[111]),
    ]))
    assert fake_redis.hget("m:kvblock:111", "pod-a") is None
    assert not fake_redis.sismember("m:podblocks:pod-a", "111")


def test_handle_all_blocks_cleared(fake_redis):
    sub = _subscriber_with_fake_redis(fake_redis)
    sub._apply_batch(KVEventBatch(ts=0.0, events=[
        BlockStored(block_hashes=[1, 2, 3], parent_block_hash=None,
                    token_ids=[1], block_size=16, lora_id=None),
    ]))
    sub._apply_batch(KVEventBatch(ts=0.0, events=[AllBlocksCleared()]))
    assert fake_redis.hget("m:kvblock:1", "pod-a") is None
    assert fake_redis.scard("m:podblocks:pod-a") == 0


def batch(events, rank_marker=...):
    value = [123.5, events]
    if rank_marker is not ...:
        value.append(rank_marker)
    return msgspec.msgpack.encode(value)


def stored(blocks=(101,), *, medium_marker=..., block_size=16):
    value = [
        "BlockStored",
        list(blocks),
        None,
        [1, 2, 3],
        block_size,
        None,
    ]
    if medium_marker is not ...:
        value.append(medium_marker)
    return value


def removed(blocks=(101,), *, medium_marker=...):
    value = ["BlockRemoved", list(blocks)]
    if medium_marker is not ...:
        value.append(medium_marker)
    return value


def frames(topic, sequence, payload):
    return [topic.encode(), sequence.to_bytes(8, "big"), payload]


@pytest.fixture
def subscriber(monkeypatch):
    fake_redis = MagicMock()
    fake_pipe = MagicMock()
    fake_redis.pipeline.return_value = fake_pipe
    fake_redis.sscan_iter.return_value = []
    fake_pipe.smembers.return_value = []
    monkeypatch.setattr(subscriber_module.redis, "Redis", lambda **_kwargs: fake_redis)
    monkeypatch.setattr(subscriber_module._cfg, "INFERENCE_ENGINE", "sglang")
    monkeypatch.setattr(subscriber_module._cfg, "INFERENCE_URL", "http://engine:30000")
    monkeypatch.setattr(subscriber_module._cfg, "INFERENCE_HOST", "engine")
    monkeypatch.setattr(subscriber_module._cfg, "KV_EVENT_PORT", 5557)
    monkeypatch.setattr(subscriber_module._cfg, "KV_EVENT_REPLAY_PORT", 5558)
    monkeypatch.setattr(subscriber_module._cfg, "KV_EVENT_TOPIC", "topic")
    monkeypatch.setattr(subscriber_module._cfg, "KV_EVENT_EXPECTED_PAGE_SIZE", 16)
    monkeypatch.setattr(subscriber_module._cfg, "KV_EVENT_DISCOVERY_ENABLED", True)
    monkeypatch.setattr(subscriber_module._cfg, "KV_EVENT_DISCOVERY_TIMEOUT_S", 0.25)
    monkeypatch.setattr(subscriber_module._cfg, "MODEL_NAME_REDIS", "model")
    monkeypatch.setattr(subscriber_module._cfg, "CONTAINER_NAME", "pod")
    monkeypatch.setattr(subscriber_module._cfg, "DP_SIZE", 2)
    monkeypatch.setattr(subscriber_module._cfg, "DP_SIZE_LOCAL", 1)
    instance = KVSubscriber()
    instance._test_redis = fake_redis
    instance._test_pipe = fake_pipe
    return instance


def test_resolves_engine_specific_topics():
    assert _resolve_topic("vllm", "kv@", "pod", "model") == "kv@"
    assert _resolve_topic("sglang", "", "pod", "model") == "kv@pod@model"
    assert _resolve_topic("sglang", "kv@", "pod", "model") == "kv@pod@model"
    assert _resolve_topic("sglang", "configured", "pod", "model") == "configured"


def test_sequence_is_unsigned_big_endian_and_exactly_eight_bytes():
    assert _decode_sequence(b"\xff" * 8) == 2**64 - 1
    with pytest.raises(ValueError, match="8 bytes"):
        _decode_sequence(b"\x00")


def test_official_three_frame_batch_decodes_optional_rank_and_gpu_events(subscriber):
    payload = batch(
        [stored((2**63 + 5,), medium_marker="GPU"), removed((7,), medium_marker="GPU")],
        rank_marker=1,
    )
    subscriber._process_frames("tcp://engine:5558", frames("topic", 9, payload))

    pipe = subscriber._test_pipe
    pipe.hset.assert_any_call("model:kvblock:9223372036854775813", "pod", ANY)
    pipe.sadd.assert_called_with("model:podblocks:pod", "9223372036854775813")
    pipe.hdel.assert_called_with("model:kvblock:7", "pod")
    assert subscriber._last_sequence["tcp://engine:5558"] == 9


def test_legacy_batch_and_events_without_optional_fields_are_supported(subscriber):
    payload = batch([stored((11,)), removed((12,)), ["AllBlocksCleared"]])
    subscriber._test_pipe.smembers.return_value = ["11", "99"]

    subscriber._process_frames("tcp://engine:5557", frames("topic", 0, payload))

    pipe = subscriber._test_pipe
    pipe.hset.assert_any_call("model:kvblock:11", "pod", ANY)
    pipe.hdel.assert_any_call("model:kvblock:12", "pod")
    pipe.hdel.assert_any_call("model:kvblock:99", "pod")
    pipe.delete.assert_called_with("model:podblocks:pod")
    assert pipe.execute.call_count >= 2


def test_sglang_ignores_non_gpu_storage_tiers(subscriber):
    payload = batch(
        [
            stored((1,), medium_marker="CPU_PINNED"),
            stored((2,), medium_marker="DISK"),
            removed((3,), medium_marker="EXTERNAL"),
            stored((4,), medium_marker="GPU"),
        ],
        rank_marker=0,
    )

    subscriber._process_frames("tcp://engine:5557", frames("topic", 0, payload))

    block_keys = [args[0] for args, _kwargs in subscriber._test_pipe.hset.call_args_list]
    assert "model:kvblock:4" in block_keys
    assert "model:kvblock:1" not in block_keys
    assert "model:kvblock:2" not in block_keys
    subscriber._test_pipe.hdel.assert_not_called()


def test_discovery_validates_descriptor_and_builds_per_rank_ports(
    subscriber, monkeypatch
):
    response = MagicMock()
    response.json.return_value = {
        "version": "0.5.15",
        "kv_events": {
            "publisher": "zmq",
            "block_size": 16,
            "topic": "topic",
            "endpoint_port_base": 5557,
            "dp_size": 2,
        },
    }
    monkeypatch.setattr(subscriber_module.requests, "get", lambda *_args, **_kwargs: response)

    assert subscriber._discover_sglang() == [
        PublisherEndpoint(0, "tcp://engine:5557", "tcp://engine:5558"),
        PublisherEndpoint(1, "tcp://engine:5558", "tcp://engine:5559"),
    ]
    response.raise_for_status.assert_called_once()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("publisher", "null"),
        ("block_size", 32),
        ("topic", "wrong"),
        ("endpoint_port_base", 6000),
        ("dp_size", 3),
    ],
)
def test_discovery_rejects_each_contract_mismatch(subscriber, monkeypatch, field, value):
    descriptor = {
        "publisher": "zmq",
        "block_size": 16,
        "topic": "topic",
        "endpoint_port_base": 5557,
        "dp_size": 2,
    }
    descriptor[field] = value
    response = MagicMock()
    response.json.return_value = {"version": "0.5.15", "kv_events": descriptor}
    monkeypatch.setattr(subscriber_module.requests, "get", lambda *_args, **_kwargs: response)

    with pytest.raises(DiscoveryMismatch, match=field):
        subscriber._discover_sglang()


def test_gap_replay_is_scoped_to_the_endpoint(subscriber, monkeypatch):
    endpoint_a = "tcp://engine:5557"
    endpoint_b = "tcp://engine:5558"
    subscriber._process_frames(endpoint_a, frames("topic", 3, batch([])))
    subscriber._process_frames(endpoint_b, frames("topic", 20, batch([], rank_marker=1)))
    replay = MagicMock(return_value=False)
    monkeypatch.setattr(subscriber, "_replay_gap", replay)

    assert not subscriber._consume_frames(
        endpoint_a, frames("topic", 5, batch([]))
    )

    replay.assert_called_once_with(endpoint_a, 4, 5)
    assert subscriber._last_sequence == {}
    subscriber._test_pipe.delete.assert_called_with("model:podblocks:pod")


def test_successful_replay_avoids_invalidation(subscriber, monkeypatch):
    endpoint = "tcp://engine:5557"
    subscriber._process_frames(endpoint, frames("topic", 10, batch([])))

    def replay(_endpoint, start, live):
        assert (start, live) == (11, 12)
        subscriber._last_sequence[endpoint] = 11
        return True

    monkeypatch.setattr(subscriber, "_replay_gap", replay)
    invalidate = MagicMock()
    monkeypatch.setattr(subscriber, "_invalidate_ownership", invalidate)

    subscriber._process_frames(endpoint, frames("topic", 12, batch([])))

    invalidate.assert_not_called()
    assert subscriber._last_sequence[endpoint] == 12


def test_replay_client_uses_sglang_router_protocol(subscriber, monkeypatch):
    endpoint = "tcp://engine:5557"
    publisher = PublisherEndpoint(0, endpoint, "tcp://engine:5558")
    subscriber._publishers[endpoint] = publisher
    dealer = MagicMock()
    dealer.recv_multipart.side_effect = [
        [b"", (4).to_bytes(8, "big"), batch([stored((40,))])],
        [b"", (5).to_bytes(8, "big"), batch([stored((50,))])],
        [b"", b"\xff" * 8, b""],
    ]
    context = MagicMock()
    context.socket.return_value = dealer
    monkeypatch.setattr(
        subscriber_module.zmq.Context,
        "instance",
        lambda: context,
    )

    assert subscriber._replay_gap(endpoint, 4, 6)

    dealer.connect.assert_called_once_with("tcp://engine:5558")
    dealer.send_multipart.assert_called_once_with([b"", (4).to_bytes(8, "big")])
    assert subscriber._last_sequence[endpoint] == 5
    subscriber._test_pipe.hset.assert_any_call("model:kvblock:40", "pod", ANY)
    subscriber._test_pipe.hset.assert_any_call("model:kvblock:50", "pod", ANY)


def test_restart_invalidates_stale_ownership_before_resuming(subscriber, monkeypatch):
    endpoint = "tcp://engine:5557"
    subscriber._process_frames(endpoint, frames("topic", 8, batch([])))
    invalidate = MagicMock(wraps=subscriber._invalidate_ownership)
    monkeypatch.setattr(subscriber, "_invalidate_ownership", invalidate)

    assert not subscriber._consume_frames(
        endpoint, frames("topic", 0, batch([stored((55,))]))
    )

    invalidate.assert_called_once()
    assert endpoint not in subscriber._last_sequence
    assert not any(
        args[0] == "model:kvblock:55"
        for args, _kwargs in subscriber._test_pipe.hset.call_args_list
    )


def test_duplicate_sequence_zero_with_same_payload_is_idempotent(
    subscriber, monkeypatch
):
    endpoint = "tcp://engine:5557"
    payload = batch([stored((55,))])
    subscriber._process_frames(endpoint, frames("topic", 0, payload))
    subscriber._test_pipe.reset_mock()
    invalidate = MagicMock()
    monkeypatch.setattr(subscriber, "_invalidate_ownership", invalidate)

    subscriber._process_frames(endpoint, frames("topic", 0, payload))

    invalidate.assert_not_called()
    subscriber._test_pipe.hset.assert_not_called()
    assert subscriber._last_sequence[endpoint] == 0


def test_duplicate_sequence_zero_with_changed_payload_rebootstraps(
    subscriber, monkeypatch
):
    endpoint = "tcp://engine:5557"
    subscriber._process_frames(endpoint, frames("topic", 0, batch([stored((55,))])))
    invalidate = MagicMock(wraps=subscriber._invalidate_ownership)
    monkeypatch.setattr(subscriber, "_invalidate_ownership", invalidate)

    assert not subscriber._consume_frames(
        endpoint, frames("topic", 0, batch([stored((99,))]))
    )

    invalidate.assert_called_once()
    assert endpoint not in subscriber._last_sequence


@pytest.mark.parametrize(
    "bad_frames",
    [
        [b"topic", b"payload"],
        [b"wrong", (0).to_bytes(8, "big"), batch([])],
        [b"topic", b"\x00", batch([])],
        [b"topic", (0).to_bytes(8, "big"), b"not-msgpack"],
    ],
)
def test_malformed_live_events_invalidate_ownership(subscriber, bad_frames):
    subscriber._test_pipe.smembers.return_value = ["stale"]

    subscriber._set_status(
        ready=True, healthy=True, fail_closed=False, phase="ready"
    )
    assert not subscriber._consume_frames("tcp://engine:5557", bad_frames)

    subscriber._test_pipe.hdel.assert_called_with("model:kvblock:stale", "pod")
    subscriber._test_pipe.delete.assert_called_with("model:podblocks:pod")
    assert subscriber.status.ready is False
    assert subscriber.status.fail_closed is True


def test_wrong_event_page_size_invalidates_ownership(subscriber):
    payload = batch([stored((1,), block_size=32)], rank_marker=0)

    assert not subscriber._consume_frames(
        "tcp://engine:5557", frames("topic", 0, payload)
    )

    subscriber._test_pipe.delete.assert_called_with("model:podblocks:pod")


def test_cold_start_retries_redis_and_discovery_with_capped_backoff(
    subscriber, monkeypatch
):
    publisher = PublisherEndpoint(0, "tcp://engine:5557", "tcp://engine:5558")
    invalidate = MagicMock(
        side_effect=[ConnectionError("redis loading"), None]
    )
    discover = MagicMock(
        side_effect=[
            ConnectionError("engine loading"),
            ConnectionError("still loading"),
            ([publisher], []),
        ]
    )
    waits = []
    monkeypatch.setattr(subscriber, "_invalidate_ownership", invalidate)
    monkeypatch.setattr(subscriber, "_initial_publishers", discover)
    monkeypatch.setattr(
        subscriber,
        "_wait_for_stop",
        lambda delay: waits.append(delay) or False,
    )
    subscriber._BACKOFF_INITIAL_S = 0.1
    subscriber._BACKOFF_MAX_S = 0.2

    assert subscriber._retry_startup() == ([publisher], [])
    assert waits == [0.1, 0.1, 0.2]
    assert invalidate.call_count == 2
    assert discover.call_count == 3
    assert subscriber.ready is False


def test_startup_retry_stops_promptly(subscriber, monkeypatch):
    invalidate = MagicMock(side_effect=ConnectionError("redis down"))
    monkeypatch.setattr(subscriber, "_invalidate_ownership", invalidate)

    def stop_on_wait(_delay):
        subscriber._stop.set()
        return True

    monkeypatch.setattr(subscriber, "_wait_for_stop", stop_on_wait)

    assert subscriber._retry_startup() is None
    assert invalidate.call_count == 1
    assert subscriber.status.phase == "invalidating"
    assert subscriber.status.fail_closed is True


def test_full_bootstrap_replays_before_first_live_event(subscriber, monkeypatch):
    endpoint = "tcp://engine:5557"
    monkeypatch.setattr(
        subscriber,
        "_collect_replay",
        lambda _endpoint, start: (
            True,
            [
                (0, subscriber._decode_batch(batch([stored((10,))]))),
                (1, subscriber._decode_batch(batch([stored((11,))]))),
            ],
        ),
    )

    result = subscriber._bootstrap_endpoint(endpoint)

    assert result == ReplayResult(True, True, 1)
    assert subscriber._last_sequence[endpoint] == 1
    subscriber._test_pipe.hset.assert_any_call("model:kvblock:10", "pod", ANY)
    subscriber._test_pipe.hset.assert_any_call("model:kvblock:11", "pod", ANY)


def test_truncated_bootstrap_does_not_fabricate_ownership(subscriber, monkeypatch):
    endpoint = "tcp://engine:5557"
    monkeypatch.setattr(
        subscriber,
        "_collect_replay",
        lambda _endpoint, start: (
            True,
            [
                (7, subscriber._decode_batch(batch([stored((70,))]))),
                (8, subscriber._decode_batch(batch([stored((80,))]))),
            ],
        ),
    )

    result = subscriber._bootstrap_endpoint(endpoint)

    assert result == ReplayResult(True, False, 8)
    assert subscriber._last_sequence[endpoint] == 8
    subscriber._test_pipe.hset.assert_not_called()


def test_unavailable_bootstrap_accepts_first_live_after_clear(subscriber, monkeypatch):
    endpoint = "tcp://engine:5557"
    monkeypatch.setattr(
        subscriber, "_collect_replay", lambda _endpoint, start: (False, [])
    )

    assert subscriber._bootstrap_endpoint(endpoint) == ReplayResult(
        False, False, None
    )
    subscriber._process_frames(
        endpoint, frames("topic", 42, batch([stored((42,))]))
    )

    assert subscriber._last_sequence[endpoint] == 42
    subscriber._test_pipe.hset.assert_any_call("model:kvblock:42", "pod", ANY)


def test_bootstrap_discards_already_replayed_live_frames(subscriber, monkeypatch):
    endpoint = "tcp://engine:5557"
    replay_zero = batch([stored((10,))])
    replay_one = batch([stored((11,))])
    monkeypatch.setattr(
        subscriber,
        "_collect_replay",
        lambda _endpoint, start: (
            True,
            [
                (0, subscriber._decode_batch(replay_zero)),
                (1, subscriber._decode_batch(replay_one)),
            ],
        ),
    )
    subscriber._bootstrap_endpoint(endpoint)
    subscriber._test_pipe.reset_mock()

    subscriber._process_frames(
        endpoint, frames("topic", 0, replay_zero)
    )
    subscriber._process_frames(
        endpoint, frames("topic", 1, replay_one)
    )
    subscriber._process_frames(
        endpoint, frames("topic", 2, batch([stored((12,))]))
    )

    assert not any(
        args[0] == "model:kvblock:999"
        for args, _kwargs in subscriber._test_pipe.hset.call_args_list
    )
    subscriber._test_pipe.hset.assert_any_call("model:kvblock:12", "pod", ANY)


def test_redis_invalidation_failure_keeps_subscriber_fail_closed(
    subscriber, monkeypatch
):
    subscriber._set_status(
        ready=True, healthy=True, fail_closed=False, phase="ready"
    )
    monkeypatch.setattr(
        subscriber,
        "_invalidate_ownership",
        MagicMock(side_effect=ConnectionError("redis unavailable")),
    )

    assert not subscriber._consume_frames(
        "tcp://engine:5557", [b"malformed"]
    )
    assert subscriber.status.ready is False
    assert subscriber.status.healthy is False
    assert subscriber.status.fail_closed is True
    assert "Redis invalidation failed" in subscriber.status.detail


def test_idle_redis_failure_drops_readiness(subscriber):
    subscriber._set_status(
        ready=True, healthy=True, fail_closed=False, phase="ready"
    )
    subscriber._next_redis_ping = 10.0
    subscriber._test_redis.ping.side_effect = ConnectionError("redis down")

    assert subscriber._redis_is_healthy(9.0)
    subscriber._test_redis.ping.assert_not_called()
    assert not subscriber._redis_is_healthy(10.0)
    assert subscriber.status.ready is False
    assert subscriber.status.healthy is False
    assert subscriber.status.fail_closed is True
    assert "Redis health check failed" in subscriber.status.detail


def test_transaction_failure_cannot_publish_undiscoverable_owner(
    fake_redis, monkeypatch
):
    sub = _subscriber_with_fake_redis(fake_redis)
    pipe = fake_redis.pipeline(transaction=True)
    monkeypatch.setattr(fake_redis, "pipeline", lambda **_kwargs: pipe)
    monkeypatch.setattr(
        pipe, "execute", MagicMock(side_effect=ConnectionError("exec failed"))
    )

    with pytest.raises(ConnectionError, match="exec failed"):
        sub._apply_batch(
            KVEventBatch(
                ts=0.0,
                events=[
                    BlockStored(
                        block_hashes=[777],
                        parent_block_hash=None,
                        token_ids=[1],
                        block_size=16,
                        lora_id=None,
                    )
                ],
            )
        )

    assert fake_redis.hget("m:kvblock:777", "pod-a") is None
    assert not fake_redis.sismember("m:podblocks:pod-a", "777")


def test_clear_transaction_failure_preserves_discoverability(
    fake_redis, monkeypatch
):
    fake_redis.hset("m:kvblock:777", "pod-a", 1)
    fake_redis.sadd("m:podblocks:pod-a", "777")
    pipe = fake_redis.pipeline(transaction=True)
    monkeypatch.setattr(fake_redis, "pipeline", lambda **_kwargs: pipe)
    monkeypatch.setattr(
        pipe, "execute", MagicMock(side_effect=ConnectionError("exec failed"))
    )

    with pytest.raises(ConnectionError, match="exec failed"):
        clear_pod_ownership(fake_redis, "m", "pod-a")

    assert fake_redis.hget("m:kvblock:777", "pod-a") is not None
    assert fake_redis.sismember("m:podblocks:pod-a", "777")


class StatefulPipeline:
    def __init__(self, client):
        self.client = client
        self.commands = []

    def hset(self, key, field, value):
        self.commands.append(("hset", key, field, value))

    def watch(self, _key):
        return None

    def smembers(self, key):
        return set(self.client.sets.get(key, set()))

    def multi(self):
        return None

    def reset(self):
        return None

    def sadd(self, key, value):
        self.commands.append(("sadd", key, value))

    def hdel(self, key, field):
        self.commands.append(("hdel", key, field))

    def srem(self, key, value):
        self.commands.append(("srem", key, value))

    def delete(self, key):
        self.commands.append(("delete", key))

    def execute(self):
        for command in self.commands:
            operation, key, *args = command
            if operation == "hset":
                field, value = args
                self.client.hashes.setdefault(key, {})[field] = value
            elif operation == "sadd":
                self.client.sets.setdefault(key, set()).add(args[0])
            elif operation == "hdel":
                self.client.hashes.setdefault(key, {}).pop(args[0], None)
            elif operation == "srem":
                self.client.sets.setdefault(key, set()).discard(args[0])
            elif operation == "delete":
                self.client.sets.pop(key, None)
        self.commands.clear()


class StatefulRedis:
    def __init__(self):
        self.hashes = {}
        self.sets = {}

    def pipeline(self, transaction=False):
        assert transaction is True
        return StatefulPipeline(self)

    def sscan_iter(self, key):
        return iter(tuple(self.sets.get(key, set())))


def test_store_then_clear_batch_leaves_no_stale_owner(subscriber):
    stateful = StatefulRedis()
    subscriber.redis = stateful
    decoded = subscriber._decode_batch(
        batch([stored((123,)), ["AllBlocksCleared"]])
    )

    subscriber._apply_batch(decoded)

    assert stateful.sets.get("model:podblocks:pod", set()) == set()
    assert "pod" not in stateful.hashes.get("model:kvblock:123", {})
