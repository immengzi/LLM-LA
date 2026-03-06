# client/pubsub_client.py
# -*- coding: utf-8 -*-
"""
ZMQ SUB client for async_pubsub transport.

Wire format (from router/pubsub.py):
  multipart: [topic_bytes, json_bytes]
  topic_bytes = f"{topic}.{run_id}"

We SUBSCRIBE to exactly that prefix, so we only receive messages for this run.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterator, Optional, Tuple

import zmq


@dataclass
class PubSubConfig:
    connect: str                 # e.g. "tcp://127.0.0.1:30081" or "tcp://router-service:5559"
    topic: str = "results"       # base topic (router adds .<run_id>)
    run_id: str = "default"      # used for subscription prefix
    rcv_hwm: int = 0             # 0 means "no explicit limit" (lib default); set if you want protection
    poll_timeout_ms: int = 250   # polling granularity
    idle_heartbeat_s: float = 5.0  # if >0, yield ("heartbeat", None) occasionally


class ResultSubscriber:
    def __init__(self, cfg: PubSubConfig):
        self.cfg = cfg
        self._ctx: Optional[zmq.Context] = None
        self._sock: Optional[zmq.Socket] = None
        self._poller: Optional[zmq.Poller] = None

        # We subscribe to topic.run_id (exact prefix)
        self._sub_prefix = f"{cfg.topic}.{cfg.run_id}".encode("utf-8", errors="strict")

    def start(self) -> None:
        if self._sock is not None:
            return

        ctx = zmq.Context.instance()
        sock = ctx.socket(zmq.SUB)

        # Best-effort, fast close
        try:
            sock.setsockopt(zmq.LINGER, 0)
        except Exception:
            pass

        # Optional receive HWM (0 => don't override)
        try:
            if int(self.cfg.rcv_hwm) > 0:
                sock.setsockopt(zmq.RCVHWM, int(self.cfg.rcv_hwm))
        except Exception:
            pass

        sock.connect(self.cfg.connect)

        # Subscribe only to our run prefix.
        sock.setsockopt(zmq.SUBSCRIBE, self._sub_prefix)

        poller = zmq.Poller()
        poller.register(sock, zmq.POLLIN)

        self._ctx = ctx
        self._sock = sock
        self._poller = poller

    def stop(self) -> None:
        try:
            if self._sock is not None:
                self._sock.close(0)
        finally:
            self._sock = None
            self._poller = None
            self._ctx = None

    def iter_results(
        self,
        *,
        deadline_s: Optional[float] = None,
    ) -> Iterator[Tuple[str, Optional[Dict[str, Any]]]]:
        """
        Yields:
          ("result", payload_dict) when a message arrives
          ("heartbeat", None) occasionally during long idle periods (optional)

        deadline_s:
          absolute time.time() deadline; if reached, stops iteration.
        """
        if self._sock is None or self._poller is None:
            self.start()

        assert self._sock is not None
        assert self._poller is not None

        last_hb = time.time()

        while True:
            now = time.time()
            if deadline_s is not None and now >= float(deadline_s):
                return

            timeout = int(self.cfg.poll_timeout_ms)
            socks = dict(self._poller.poll(timeout))

            if self._sock in socks and socks[self._sock] == zmq.POLLIN:
                try:
                    topic_b, body_b = self._sock.recv_multipart(flags=0)
                except Exception:
                    continue

                # Extra safety: ensure correct prefix (should already be filtered)
                if not topic_b.startswith(self._sub_prefix):
                    continue

                try:
                    payload = json.loads(body_b.decode("utf-8"))
                except Exception:
                    continue

                yield ("result", payload)
                continue

            # idle heartbeat
            if self.cfg.idle_heartbeat_s and self.cfg.idle_heartbeat_s > 0:
                if (now - last_hb) >= float(self.cfg.idle_heartbeat_s):
                    last_hb = now
                    yield ("heartbeat", None)
