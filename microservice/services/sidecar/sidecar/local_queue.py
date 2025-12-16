# -*- coding: utf-8 -*-
from queue import Queue, Empty
from threading import RLock
from typing import Tuple, Dict, Any, Optional


class LocalQueue:
    """
    Local queue next to vLLM. Holds (req_id, prompt, meta).
    Tracks pending (in queue) and inflight (being processed) so
    the sidecar can enforce a max concurrency per pod.
    """

    def __init__(self):
        self._q: "Queue[Tuple[str, str, Dict[str, Any]]]" = Queue()
        self._lock = RLock()
        self._inflight: int = 0

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

    def get_nowait(self) -> Optional[Tuple[str, str, Dict[str, Any]]]:
        try:
            item = self._q.get_nowait()
        except Empty:
            return None
        # mark as inflight
        with self._lock:
            self._inflight += 1
        return item

    def task_done(self):
        try:
            self._q.task_done()
        except Exception:
            pass
        with self._lock:
            self._inflight = max(0, self._inflight - 1)
