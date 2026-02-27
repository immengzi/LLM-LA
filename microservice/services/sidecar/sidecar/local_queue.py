# -*- coding: utf-8 -*-
from queue import Queue, Empty
from threading import RLock
from typing import Tuple, Dict, Any, Optional

from .metrics import set_sidecar_queue_length


class LocalQueue:
    """
    Local queue next to vLLM. Holds (req_id, prompt, meta).
    Tracks pending (in queue) and inflight (being processed) so
    the sidecar can enforce a max concurrency per pod.

    Prom metric:
      sidecar_queue_length{endpoint} = pending + inflight   (logical)
    """

    def __init__(self, endpoint_id: str):
        self.endpoint_id = str(endpoint_id)
        self._q: "Queue[Tuple[str, str, Dict[str, Any]]]" = Queue()
        self._lock = RLock()
        self._inflight: int = 0

        # initialize gauge
        set_sidecar_queue_length(self.endpoint_id, 0)

    def _update_gauge_locked(self) -> None:
        pending = self._q.qsize()
        logical = pending + self._inflight
        set_sidecar_queue_length(self.endpoint_id, logical)

    def size(self) -> int:
        return self._q.qsize()

    def state(self) -> Tuple[int, int]:
        """
        Return (pending, inflight).
        """
        with self._lock:
            pending = self._q.qsize()
            inflight = self._inflight
        return pending, inflight

    def put(self, req_id: str, prompt: str, meta: Dict[str, Any]):
        self._q.put((req_id, prompt, meta))
        with self._lock:
            self._update_gauge_locked()

    def get_nowait(self) -> Optional[Tuple[str, str, Dict[str, Any]]]:
        try:
            item = self._q.get_nowait()
        except Empty:
            return None
        # mark as inflight
        with self._lock:
            self._inflight += 1
            self._update_gauge_locked()
        return item

    def task_done(self):
        try:
            self._q.task_done()
        except Exception:
            pass
        with self._lock:
            self._inflight = max(0, self._inflight - 1)
            self._update_gauge_locked()
