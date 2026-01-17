# sidecar/result_poster.py
# -*- coding: utf-8 -*-
import queue
import threading
import time
import random
from typing import Dict, Any, Optional

import requests

from .config import get_config

_cfg = get_config()


class ResultPoster:
    """
    Async poster for sidecar -> router /result.

    Key property: vLLM workers never block on router /result.
    This prevents worker starvation and reduces TCP reset sensitivity.

    Semantics preserved:
      - same payload eventually sent to router /result
      - ordering is preserved (single worker poster thread) unless you increase workers
    """

    def __init__(self, maxsize: int = 100000):
        self._q: "queue.Queue[Dict[str, Any]]" = queue.Queue(maxsize=maxsize)
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._session: Optional[requests.Session] = None

    def start(self):
        if self._thread is not None:
            return
        self._stop.clear()
        self._session = requests.Session()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        print("[sidecar] ResultPoster started")

    def stop(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None
        if self._session is not None:
            try:
                self._session.close()
            except Exception:
                pass
            self._session = None
        print("[sidecar] ResultPoster stopped")

    def submit(self, payload: Dict[str, Any]) -> None:
        # No blocking: never stall vLLM workers.
        try:
            self._q.put_nowait(payload)
        except queue.Full:
            # If this happens, router can't accept results fast enough.
            # We keep logic unchanged (no backpressure), but we must avoid deadlock.
            # Dropping is noisy but safer than blocking compute threads.
            rid = payload.get("req_id", "?")
            print(f"[sidecar] ResultPoster queue FULL; dropping result for req_id={rid}")

    def _loop(self):
        session = self._session or requests.Session()
        url = f"{_cfg.ROUTER_URL}/result"

        while not self._stop.is_set():
            try:
                payload = self._q.get(timeout=0.5)
            except queue.Empty:
                continue

            attempt = 0
            while not self._stop.is_set():
                attempt += 1
                try:
                    r = session.post(url, json=payload, timeout=_cfg.ROUTER_RESULT_TIMEOUT_S)
                    if r.ok:
                        break
                except Exception:
                    pass

                # Retry with jittered exponential backoff (bounded)
                sleep_s = min(2.0, 0.05 * (2 ** min(attempt, 6)))
                sleep_s *= (0.8 + 0.4 * random.random())
                time.sleep(sleep_s)

            try:
                self._q.task_done()
            except Exception:
                pass
