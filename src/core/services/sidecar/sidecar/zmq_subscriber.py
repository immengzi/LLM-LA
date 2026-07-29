#!/usr/bin/env python3
"""Mirror inference-engine KV-cache events into the existing Redis schema."""

from __future__ import annotations

import hashlib
import socket
import threading
import time
from dataclasses import dataclass
from typing import Any, NewType, Optional, Union

import msgspec
import redis
import requests
import zmq

from .config import get_config
from .kv_redis import clear_pod_ownership, redis_key_prefix

_cfg = get_config()

BlockHash = NewType("BlockHash", int)


class EventBatch(msgspec.Struct, array_like=True, omit_defaults=True, gc=False):
    ts: float
    events: list[Any]
    attn_dp_rank: Optional[int] = None


class KVCacheEvent(msgspec.Struct, array_like=True, omit_defaults=True, gc=False, tag=True):
    pass


class BlockStored(KVCacheEvent):
    block_hashes: list[BlockHash]
    parent_block_hash: Optional[BlockHash]
    token_ids: list[int]
    block_size: int
    lora_id: Optional[int]
    medium: Optional[str] = None


class BlockRemoved(KVCacheEvent):
    block_hashes: list[BlockHash]
    medium: Optional[str] = None


class AllBlocksCleared(KVCacheEvent):
    pass


class KVEventBatch(EventBatch):
    events: list[Union[BlockStored, BlockRemoved, AllBlocksCleared]]


@dataclass(frozen=True)
class PublisherEndpoint:
    rank: int
    event_url: str
    replay_url: str


@dataclass(frozen=True)
class SubscriberStatus:
    ready: bool
    healthy: bool
    fail_closed: bool
    phase: str
    detail: str = ""
    cache_visibility: str = "none"


@dataclass(frozen=True)
class ReplayResult:
    available: bool
    complete: bool
    last_sequence: int | None


def _resolve_zmq_endpoints(
    leader_name: str,
    base_host: str,
    base_port: int,
    dp_size: int,
    dp_size_local: int,
    namespace: str = "vllm",
) -> tuple[list[str], list[tuple[int, str, int]]]:
    """Build the legacy vLLM DP fan-in endpoints without changing its DNS rules."""
    resolved: list[str] = [f"tcp://{base_host}:{base_port}"]
    pending: list[tuple[int, str, int]] = []
    if dp_size <= 1:
        return resolved, pending

    service_name = leader_name.rsplit("-", 1)[0] if "-" in leader_name else leader_name
    for rank in range(1, dp_size):
        worker_name = f"{leader_name}-{rank}"
        fqdn = f"{worker_name}.{service_name}.{namespace}.svc.cluster.local"
        port = base_port + rank * dp_size_local
        try:
            resolved.append(f"tcp://{socket.gethostbyname(fqdn)}:{port}")
        except socket.gaierror:
            pending.append((rank, fqdn, port))
    return resolved, pending


def _resolve_topic(engine: str, configured: str, pod_name: str, model: str) -> str:
    """Resolve an exact SGLang topic while retaining vLLM's legacy prefix."""
    value = (configured or "").strip()
    if engine != "sglang":
        return value or "kv@"
    if value and value != "kv@":
        return value
    return f"kv@{pod_name}@{model}"


def _decode_sequence(raw: bytes) -> int:
    if len(raw) != 8:
        raise ValueError(f"sequence frame must be 8 bytes, got {len(raw)}")
    return int.from_bytes(raw, "big", signed=False)


class DiscoveryMismatch(RuntimeError):
    pass


class StreamResyncRequired(RuntimeError):
    pass


class KVSubscriber:
    _RETRY_INTERVAL_S = 10.0
    _RETRY_MAX = 90
    _SGLANG_VERSION = "0.5.15"
    _REPLAY_END = b"\xff" * 8
    _BACKOFF_INITIAL_S = 0.25
    _BACKOFF_MAX_S = 5.0

    def __init__(self):
        self.engine = _cfg.INFERENCE_ENGINE
        self.inference_url = _cfg.INFERENCE_URL.rstrip("/")
        self.host = _cfg.INFERENCE_HOST
        self.port = _cfg.KV_EVENT_PORT
        self.replay_port = _cfg.KV_EVENT_REPLAY_PORT
        self.redis = redis.Redis(
            host=_cfg.REDIS_HOST,
            port=_cfg.REDIS_PORT,
            decode_responses=True,
            socket_connect_timeout=_cfg.KV_EVENT_REDIS_PING_TIMEOUT_S,
            socket_timeout=_cfg.KV_EVENT_REDIS_PING_TIMEOUT_S,
        )
        self.model = _cfg.MODEL_NAME_REDIS
        self.pod_name = _cfg.CONTAINER_NAME
        self.dp_size = _cfg.DP_SIZE
        self.dp_size_local = _cfg.DP_SIZE_LOCAL
        self.topic = _resolve_topic(
            self.engine,
            _cfg.KV_EVENT_TOPIC,
            self.pod_name,
            self.model,
        )
        self.expected_page_size = _cfg.KV_EVENT_EXPECTED_PAGE_SIZE
        self.discovery_enabled = _cfg.KV_EVENT_DISCOVERY_ENABLED
        self.discovery_timeout = _cfg.KV_EVENT_DISCOVERY_TIMEOUT_S
        self.redis_ping_interval = max(
            0.1, _cfg.KV_EVENT_REDIS_PING_INTERVAL_S
        )

        self._decoder = msgspec.msgpack.Decoder(type=KVEventBatch)
        self._last_sequence: dict[str, int] = {}
        self._last_payload_identity: dict[str, bytes] = {}
        self._epoch_zero_identity: dict[str, bytes] = {}
        self._bootstrap_watermarks: dict[str, int] = {}
        self._publishers: dict[str, PublisherEndpoint] = {}
        self._next_redis_ping = 0.0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._status_lock = threading.Lock()
        self._status = SubscriberStatus(
            ready=False,
            healthy=False,
            fail_closed=True,
            phase="stopped",
        )

    @property
    def status(self) -> SubscriberStatus:
        with self._status_lock:
            return self._status

    @property
    def ready(self) -> bool:
        return self.status.ready

    def _set_status(
        self,
        *,
        ready: bool,
        healthy: bool,
        fail_closed: bool,
        phase: str,
        detail: str = "",
        cache_visibility: str = "none",
    ) -> None:
        with self._status_lock:
            self._status = SubscriberStatus(
                ready=ready,
                healthy=healthy,
                fail_closed=fail_closed,
                phase=phase,
                detail=detail,
                cache_visibility=cache_visibility,
            )

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._set_status(
            ready=False,
            healthy=False,
            fail_closed=True,
            phase="starting",
        )
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=max(2.0, self.discovery_timeout + 1.0))
            self._thread = None
        try:
            self._invalidate_ownership("subscriber shutdown")
        except Exception as exc:
            print(f"[KV-SUB] shutdown ownership clear failed: {exc}")
        self._set_status(
            ready=False,
            healthy=False,
            fail_closed=True,
            phase="stopped",
        )

    def _invalidate_ownership(self, reason: str) -> None:
        clear_pod_ownership(self.redis, self.model, self.pod_name)
        self._last_sequence.clear()
        self._last_payload_identity.clear()
        self._epoch_zero_identity.clear()
        self._bootstrap_watermarks.clear()
        print(f"[KV-SUB] invalidated pod ownership: {reason}")

    def _wait_for_stop(self, delay: float) -> bool:
        return self._stop.wait(delay)

    def _discover_sglang(self) -> list[PublisherEndpoint]:
        response = requests.get(
            f"{self.inference_url}/server_info",
            timeout=self.discovery_timeout,
        )
        response.raise_for_status()
        info = response.json()
        descriptor = info.get("kv_events")
        if not isinstance(descriptor, dict):
            raise DiscoveryMismatch("server_info.kv_events is missing")

        expected = {
            "publisher": "zmq",
            "block_size": self.expected_page_size,
            "topic": self.topic,
            "endpoint_port_base": self.port,
            "dp_size": self.dp_size,
        }
        mismatches = [
            f"{key}={descriptor.get(key)!r} (expected {value!r})"
            for key, value in expected.items()
            if descriptor.get(key) != value
        ]
        version = str(info.get("version", ""))
        if version != self._SGLANG_VERSION:
            mismatches.append(
                f"version={version!r} (expected {self._SGLANG_VERSION!r})"
            )
        if mismatches:
            raise DiscoveryMismatch("; ".join(mismatches))

        return [
            PublisherEndpoint(
                rank=rank,
                event_url=f"tcp://{self.host}:{self.port + rank}",
                replay_url=f"tcp://{self.host}:{self.replay_port + rank}",
            )
            for rank in range(self.dp_size)
        ]

    def _initial_publishers(self) -> tuple[list[PublisherEndpoint], list[tuple[int, str, int]]]:
        if self.engine == "sglang":
            if self.discovery_enabled:
                return self._discover_sglang(), []
            return (
                [
                    PublisherEndpoint(
                        rank=rank,
                        event_url=f"tcp://{self.host}:{self.port + rank}",
                        replay_url=f"tcp://{self.host}:{self.replay_port + rank}",
                    )
                    for rank in range(self.dp_size)
                ],
                [],
            )

        resolved, pending = _resolve_zmq_endpoints(
            self.pod_name,
            self.host,
            self.port,
            self.dp_size,
            self.dp_size_local,
        )
        publishers = [
            PublisherEndpoint(
                rank=rank,
                event_url=url,
                replay_url=f"tcp://{self.host}:{self.replay_port}",
            )
            for rank, url in enumerate(resolved)
        ]
        return publishers, pending

    def _retry_startup(
        self,
    ) -> tuple[list[PublisherEndpoint], list[tuple[int, str, int]]] | None:
        delay = self._BACKOFF_INITIAL_S
        while not self._stop.is_set():
            self._set_status(
                ready=False,
                healthy=False,
                fail_closed=True,
                phase="invalidating",
                detail="clearing prior Redis ownership",
            )
            try:
                self._invalidate_ownership("subscriber startup")
                break
            except Exception as exc:
                self._set_status(
                    ready=False,
                    healthy=False,
                    fail_closed=True,
                    phase="invalidating",
                    detail=f"Redis invalidation failed: {exc}",
                )
                if self._wait_for_stop(delay):
                    return None
                delay = min(delay * 2, self._BACKOFF_MAX_S)
        else:
            return None

        delay = self._BACKOFF_INITIAL_S
        while not self._stop.is_set():
            self._set_status(
                ready=False,
                healthy=False,
                fail_closed=True,
                phase="discovering",
                detail="waiting for KV publisher discovery",
            )
            try:
                return self._initial_publishers()
            except Exception as exc:
                self._set_status(
                    ready=False,
                    healthy=False,
                    fail_closed=True,
                    phase="discovering",
                    detail=f"KV discovery failed: {exc}",
                )
                if self._wait_for_stop(delay):
                    return None
                delay = min(delay * 2, self._BACKOFF_MAX_S)
        return None

    def _loop(self) -> None:
        while not self._stop.is_set():
            startup = self._retry_startup()
            if startup is None:
                break
            publishers, pending = startup
            context = zmq.Context()
            sockets: list[Any] = []
            socket_endpoints: dict[Any, str] = {}
            poller = zmq.Poller()
            session_failed = False
            try:
                self._publishers.clear()
                self._set_status(
                    ready=False,
                    healthy=False,
                    fail_closed=True,
                    phase="connecting",
                )
                for publisher in publishers:
                    sub = context.socket(zmq.SUB)
                    sub.connect(publisher.event_url)
                    sub.setsockopt_string(zmq.SUBSCRIBE, self.topic)
                    poller.register(sub, zmq.POLLIN)
                    sockets.append(sub)
                    socket_endpoints[sub] = publisher.event_url
                    self._publishers[publisher.event_url] = publisher

                visibility = "full"
                if self.engine == "sglang":
                    self._set_status(
                        ready=False,
                        healthy=False,
                        fail_closed=True,
                        phase="bootstrapping",
                    )
                    for publisher in publishers:
                        result = self._bootstrap_endpoint(publisher.event_url)
                        if not result.available:
                            visibility = "live_only"
                        elif not result.complete and visibility == "full":
                            visibility = "truncated"

                # Redis may have disappeared while discovery/model loading was
                # in progress. Do not advertise readiness without a final
                # connectivity check after bootstrap.
                self.redis.ping()
                self._next_redis_ping = time.monotonic() + self.redis_ping_interval
                self._set_status(
                    ready=True,
                    healthy=True,
                    fail_closed=False,
                    phase="ready",
                    detail=(
                        ""
                        if visibility == "full"
                        else "replay unavailable or truncated; visibility is conservative"
                    ),
                    cache_visibility=visibility,
                )

                retry_attempt = 0
                retry_last = time.monotonic()
                while not self._stop.is_set():
                    events = dict(poller.poll(10))
                    for sub in list(events):
                        if not events[sub] & zmq.POLLIN:
                            continue
                        endpoint = socket_endpoints[sub]
                        if not self._consume_frames(endpoint, sub.recv_multipart()):
                            session_failed = True
                            break
                    if session_failed:
                        break
                    if not self._redis_is_healthy(time.monotonic()):
                        session_failed = True
                        break

                    if (
                        self.engine != "sglang"
                        and pending
                        and retry_attempt < self._RETRY_MAX
                        and time.monotonic() - retry_last >= self._RETRY_INTERVAL_S
                    ):
                        retry_last = time.monotonic()
                        retry_attempt += 1
                        still_pending = []
                        for rank, fqdn, port in pending:
                            try:
                                url = f"tcp://{socket.gethostbyname(fqdn)}:{port}"
                                sub = context.socket(zmq.SUB)
                                sub.connect(url)
                                sub.setsockopt_string(zmq.SUBSCRIBE, self.topic)
                                poller.register(sub, zmq.POLLIN)
                                sockets.append(sub)
                                socket_endpoints[sub] = url
                                publisher = PublisherEndpoint(
                                    rank=rank,
                                    event_url=url,
                                    replay_url=f"tcp://{self.host}:{self.replay_port}",
                                )
                                self._publishers[url] = publisher
                            except socket.gaierror:
                                still_pending.append((rank, fqdn, port))
                        pending = still_pending
            except Exception as exc:
                session_failed = True
                self._set_status(
                    ready=False,
                    healthy=False,
                    fail_closed=True,
                    phase="failed",
                    detail=f"KV subscriber session failed: {exc}",
                )
            finally:
                for sub in sockets:
                    try:
                        sub.close(0)
                    except Exception:
                        pass
                context.term()

            if session_failed and not self._stop.is_set():
                self._wait_for_stop(self._BACKOFF_INITIAL_S)

        self._set_status(
            ready=False,
            healthy=False,
            fail_closed=True,
            phase="stopped",
        )

    def _redis_is_healthy(self, now: float) -> bool:
        """Periodically verify Redis while the event stream is idle."""
        if now < self._next_redis_ping:
            return True
        try:
            self.redis.ping()
        except Exception as exc:
            self._set_status(
                ready=False,
                healthy=False,
                fail_closed=True,
                phase="failed",
                detail=f"Redis health check failed: {exc}",
            )
            return False
        self._next_redis_ping = now + self.redis_ping_interval
        return True

    def _consume_frames(self, endpoint: str, frames: list[bytes]) -> bool:
        """Process one live message, invalidating ownership on any corruption."""
        try:
            self._process_frames(endpoint, frames)
            return True
        except Exception as exc:
            self._set_status(
                ready=False,
                healthy=False,
                fail_closed=True,
                phase="invalidating",
                detail=f"event stream error: {exc}",
            )
            if isinstance(exc, StreamResyncRequired):
                return False
            try:
                self._invalidate_ownership(f"event stream error: {exc}")
                return False
            except Exception as redis_exc:
                self._set_status(
                    ready=False,
                    healthy=False,
                    fail_closed=True,
                    phase="invalidating",
                    detail=f"Redis invalidation failed: {redis_exc}",
                )
                print(f"[KV-SUB] cannot fail closed in Redis: {redis_exc}")
                return False

    def _process_frames(self, endpoint: str, frames: list[bytes]) -> None:
        if len(frames) != 3:
            raise ValueError(f"unexpected frame count: {len(frames)}")
        topic_raw, sequence_raw, payload = frames
        try:
            topic = topic_raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("topic is not UTF-8") from exc
        if self.engine == "sglang":
            topic_matches = topic == self.topic
        else:
            topic_matches = topic.startswith(self.topic)
        if not topic_matches:
            raise ValueError(f"unexpected topic {topic!r}")

        sequence = _decode_sequence(sequence_raw)
        batch = self._decode_batch(payload)
        payload_identity = self._batch_identity(batch)
        previous = self._last_sequence.get(endpoint)
        bootstrap_watermark = self._bootstrap_watermarks.get(endpoint)
        if bootstrap_watermark is not None:
            if sequence <= bootstrap_watermark:
                known_identity = (
                    self._epoch_zero_identity.get(endpoint)
                    if sequence == 0
                    else (
                        self._last_payload_identity.get(endpoint)
                        if sequence == previous
                        else None
                    )
                )
                if known_identity is not None and payload_identity != known_identity:
                    self._publisher_epoch_changed(endpoint, sequence)
                return
            self._bootstrap_watermarks.pop(endpoint, None)
        if previous is None:
            self._apply_batch(batch)
            self._last_sequence[endpoint] = sequence
            self._remember_payload(endpoint, sequence, payload_identity)
            return
        if sequence == previous + 1:
            self._apply_batch(batch)
            self._last_sequence[endpoint] = sequence
            self._remember_payload(endpoint, sequence, payload_identity)
            return
        if sequence == previous:
            if payload_identity != self._last_payload_identity.get(endpoint):
                self._publisher_epoch_changed(endpoint, sequence)
            return
        if sequence < previous:
            self._invalidate_ownership(
                f"publisher restart at {endpoint}: {previous} -> {sequence}"
            )
            raise StreamResyncRequired(
                f"publisher restart at {endpoint}: {previous} -> {sequence}"
            )

        if self._replay_gap(endpoint, previous + 1, sequence):
            current = self._last_sequence.get(endpoint)
            if current is not None and current >= sequence:
                return
            if current == sequence - 1:
                self._apply_batch(batch)
                self._last_sequence[endpoint] = sequence
                self._remember_payload(endpoint, sequence, payload_identity)
                return

        self._invalidate_ownership(
            f"unrecoverable sequence gap at {endpoint}: {previous} -> {sequence}"
        )
        raise StreamResyncRequired(
            f"unrecoverable sequence gap at {endpoint}: {previous} -> {sequence}"
        )

    def _remember_payload(
        self, endpoint: str, sequence: int, payload_identity: bytes
    ) -> None:
        self._last_payload_identity[endpoint] = payload_identity
        if sequence == 0:
            self._epoch_zero_identity[endpoint] = payload_identity

    @staticmethod
    def _batch_identity(batch: KVEventBatch) -> bytes:
        return hashlib.sha256(msgspec.msgpack.encode(batch)).digest()

    def _publisher_epoch_changed(self, endpoint: str, sequence: int) -> None:
        """Fail closed when a sequence number is reused for different content.

        ZMQ SUB does not expose a publisher process identity. Payload identity
        therefore provides a conservative epoch signal: exact retransmissions
        remain idempotent, while changed content at a reused sequence forces a
        full ownership clear and replay bootstrap.
        """
        previous = self._last_sequence.get(endpoint)
        self._invalidate_ownership(
            f"publisher epoch changed at {endpoint}: sequence {sequence} "
            f"(previous {previous})"
        )
        raise StreamResyncRequired(
            f"publisher epoch changed at {endpoint}: sequence {sequence}"
        )

    def _decode_batch(self, payload: bytes) -> KVEventBatch:
        try:
            batch = self._decoder.decode(payload)
        except Exception as exc:
            raise ValueError(f"malformed msgpack KVEventBatch: {exc}") from exc
        if batch.attn_dp_rank is not None and not (0 <= batch.attn_dp_rank < self.dp_size):
            raise ValueError(f"invalid attn_dp_rank {batch.attn_dp_rank}")
        return batch

    def _collect_replay(
        self, endpoint: str, start: int
    ) -> tuple[bool, list[tuple[int, KVEventBatch]]]:
        publisher = self._publishers.get(endpoint)
        if publisher is None or not publisher.replay_url:
            return False, []
        context = zmq.Context.instance()
        dealer = context.socket(zmq.DEALER)
        dealer.setsockopt(zmq.LINGER, 0)
        dealer.setsockopt(zmq.RCVTIMEO, max(1, int(self.discovery_timeout * 1000)))
        batches: list[tuple[int, KVEventBatch]] = []
        try:
            dealer.connect(publisher.replay_url)
            dealer.send_multipart([b"", start.to_bytes(8, "big")])
            while True:
                reply = dealer.recv_multipart()
                if len(reply) == 3 and reply[0] == b"":
                    _, sequence_raw, payload = reply
                elif len(reply) == 2:
                    sequence_raw, payload = reply
                else:
                    raise ValueError(f"invalid replay frame count: {len(reply)}")
                if sequence_raw == self._REPLAY_END:
                    return True, batches
                sequence = _decode_sequence(sequence_raw)
                batches.append((sequence, self._decode_batch(payload)))
        except zmq.Again:
            return False, []
        finally:
            dealer.close(0)

    def _bootstrap_endpoint(self, endpoint: str) -> ReplayResult:
        """Replay from sequence zero before live events become authoritative.

        A replay buffer beginning above zero is truncated. Its batches are not
        applied because earlier removes/clears may be absent; only its final
        sequence is retained so subsequent live events can be consumed safely.
        """
        available, batches = self._collect_replay(endpoint, 0)
        if not available:
            return ReplayResult(False, False, None)
        if not batches:
            return ReplayResult(True, True, None)

        sequences = [sequence for sequence, _batch in batches]
        contiguous = all(
            current == previous + 1
            for previous, current in zip(sequences, sequences[1:])
        )
        complete = sequences[0] == 0 and contiguous
        if complete:
            for sequence, event_batch in batches:
                self._apply_batch(event_batch)
                self._last_sequence[endpoint] = sequence
                identity = self._batch_identity(event_batch)
                self._remember_payload(endpoint, sequence, identity)
        else:
            # Ownership was already cleared. Do not apply an incomplete history.
            self._last_sequence[endpoint] = sequences[-1]
            self._last_payload_identity.pop(endpoint, None)
        self._bootstrap_watermarks[endpoint] = sequences[-1]
        return ReplayResult(True, complete, sequences[-1])

    def _replay_gap(self, endpoint: str, start: int, live_sequence: int) -> bool:
        try:
            available, batches = self._collect_replay(endpoint, start)
            if not available:
                return False
            expected = start
            for sequence, event_batch in batches:
                if sequence != expected:
                    return False
                self._apply_batch(event_batch)
                self._last_sequence[endpoint] = sequence
                identity = self._batch_identity(event_batch)
                self._remember_payload(endpoint, sequence, identity)
                expected += 1
            return expected > live_sequence - 1
        except Exception:
            return False

    def _apply_batch(self, event_batch: KVEventBatch) -> None:
        prefix = redis_key_prefix(self.model)
        kvblocks_key = f"{prefix}kvblocks"
        podblocks_key = f"{prefix}podblocks:{self.pod_name}"
        pipe = self.redis.pipeline(transaction=True)
        timestamp = int(time.time())

        for event in event_batch.events:
            if isinstance(event, BlockStored):
                if self.engine == "sglang" and event.medium not in (None, "GPU"):
                    continue
                if (
                    self.engine == "sglang"
                    and event.block_size != self.expected_page_size
                ):
                    raise ValueError(
                        f"event block_size={event.block_size}, "
                        f"expected {self.expected_page_size}"
                    )
                for block_hash in event.block_hashes:
                    value = str(block_hash)
                    block_key = f"{prefix}kvblock:{value}"
                    pipe.hset(block_key, self.pod_name, timestamp)
                    pipe.sadd(podblocks_key, value)
                    pipe.hset(kvblocks_key, value, block_key)
            elif isinstance(event, BlockRemoved):
                if self.engine == "sglang" and event.medium not in (None, "GPU"):
                    continue
                for block_hash in event.block_hashes:
                    value = str(block_hash)
                    pipe.hdel(f"{prefix}kvblock:{value}", self.pod_name)
                    pipe.srem(podblocks_key, value)
            elif isinstance(event, AllBlocksCleared):
                # A clear is an ordering barrier. Flush prior stores/removes so
                # the ownership scan observes them, clear synchronously, then
                # continue subsequent events in a fresh pipeline.
                pipe.execute()
                clear_pod_ownership(self.redis, self.model, self.pod_name)
                pipe = self.redis.pipeline(transaction=True)
            else:
                raise ValueError(f"unsupported KV event {type(event).__name__}")
        pipe.execute()
