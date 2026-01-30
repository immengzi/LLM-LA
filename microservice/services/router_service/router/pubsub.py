# router/pubsub.py
# -*- coding: utf-8 -*-
"""
ZeroMQ PUB publisher for async_pubsub transport.

Design goals:
- Router should never block request handling on slow subscribers.
- Best-effort delivery: PUB drops if no subscribers / HWM overflow.
- Simple topic prefix so clients can SUB-filter cheaply.
- Payload is JSON bytes: {"req_id": "...", "result": {...}, "run_id": "...", ...}

Config is wired via router/config.py:
- RESULTS_ZMQ_BIND   e.g. "tcp://0.0.0.0:5559"
- RESULTS_ZMQ_TOPIC  e.g. "results"
- RESULTS_ZMQ_HWM    e.g. 100000
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional
from threading import RLock

import zmq


class ResultPublisher:
    def __init__(self, *, bind: str, topic: str = "results", hwm: int = 100000):
        self._bind = str(bind)
        self._topic = str(topic or "results")
        self._hwm = int(hwm) if hwm is not None else 100000

        self._ctx: Optional[zmq.Context] = None
        self._sock: Optional[zmq.Socket] = None
        self._lock = RLock()
        self._started = False

    @property
    def bind(self) -> str:
        return self._bind

    @property
    def topic(self) -> str:
        return self._topic

    def start(self) -> None:
        with self._lock:
            if self._started:
                return

            ctx = zmq.Context.instance()
            sock = ctx.socket(zmq.PUB)

            # High-water mark: controls in-memory queueing for slow subscribers.
            # We want "best-effort": allow buffering but never block.
            try:
                sock.setsockopt(zmq.SNDHWM, int(self._hwm))
            except Exception:
                pass

            # LINGER=0 ensures close() doesn't block at shutdown.
            try:
                sock.setsockopt(zmq.LINGER, 0)
            except Exception:
                pass

            sock.bind(self._bind)

            self._ctx = ctx
            self._sock = sock
            self._started = True

    def stop(self) -> None:
        with self._lock:
            if not self._started:
                return
            try:
                if self._sock is not None:
                    self._sock.close(0)
            finally:
                self._sock = None
                self._ctx = None
                self._started = False

    def publish(self, payload: Dict[str, Any]) -> None:
        """
        Best-effort publish. Never raises to callers (unless truly unexpected).
        Message wire format:
          [topic_bytes, json_bytes]
        Where topic_bytes = f"{topic}.{run_id or 'default'}"
        """
        sock = None
        with self._lock:
            if not self._started or self._sock is None:
                return
            sock = self._sock

        try:
            run_id = payload.get("run_id")
            if not isinstance(run_id, str) or not run_id:
                run_id = "default"

            topic = f"{self._topic}.{run_id}".encode("utf-8", errors="strict")
            body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

            # DONTWAIT => never block router thread; drop if cannot enqueue.
            sock.send_multipart([topic, body], flags=zmq.DONTWAIT)
        except zmq.Again:
            # HWM hit; drop.
            return
        except Exception:
            # Best-effort: never crash request handling due to pubsub.
            return
