# tests/test_zmq_subscriber.py
# -*- coding: utf-8 -*-
"""Unit tests for KV event decode + Redis projection (sidecar.zmq_subscriber).

The ZMQ socket loop itself is not exercised (it needs a live publisher); instead
we test the pure DNS/endpoint resolution, the msgpack wire decode, and the Redis
projection logic (_handle_batch) against an in-memory fakeredis.
"""
import msgspec

from sidecar import zmq_subscriber as Z
from sidecar.zmq_subscriber import (
    KVSubscriber,
    KVEventBatch,
    BlockStored,
    BlockRemoved,
    AllBlocksCleared,
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
    sub._handle_batch(batch)

    assert fake_redis.hget("m:kvblock:111", "pod-a") is not None
    assert fake_redis.sismember("m:podblocks:pod-a", "111")
    assert fake_redis.hget("m:kvblocks", "222") == "m:kvblock:222"


def test_handle_block_removed_clears_ownership(fake_redis):
    sub = _subscriber_with_fake_redis(fake_redis)
    sub._handle_batch(KVEventBatch(ts=0.0, events=[
        BlockStored(block_hashes=[111], parent_block_hash=None,
                    token_ids=[1], block_size=16, lora_id=None),
    ]))
    sub._handle_batch(KVEventBatch(ts=0.0, events=[
        BlockRemoved(block_hashes=[111]),
    ]))
    assert fake_redis.hget("m:kvblock:111", "pod-a") is None
    assert not fake_redis.sismember("m:podblocks:pod-a", "111")


def test_handle_all_blocks_cleared(fake_redis):
    sub = _subscriber_with_fake_redis(fake_redis)
    sub._handle_batch(KVEventBatch(ts=0.0, events=[
        BlockStored(block_hashes=[1, 2, 3], parent_block_hash=None,
                    token_ids=[1], block_size=16, lora_id=None),
    ]))
    sub._handle_batch(KVEventBatch(ts=0.0, events=[AllBlocksCleared()]))
    assert fake_redis.hget("m:kvblock:1", "pod-a") is None
    assert fake_redis.scard("m:podblocks:pod-a") == 0
