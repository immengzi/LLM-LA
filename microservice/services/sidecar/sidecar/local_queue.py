# -*- coding: utf-8 -*-
from queue import Queue, Empty
from threading import RLock
from typing import Tuple, Dict, Any, Optional


class LocalQueue:
    """
    Local queue next to vLLM. Holds (req_id, prompt, meta).
    """

    def __init__(self):
        self._q: "Queue[Tuple[int, str, Dict[str, Any]]]" = Queue()
        self._lock = RLock()

    def size(self) -> int:
        return self._q.qsize()

    def put(self, req_id: int, prompt: str, meta: Dict[str, Any]):
        self._q.put((req_id, prompt, meta))

    def get_nowait(self) -> Optional[Tuple[int, str, Dict[str, Any]]]:
        try:
            return self._q.get_nowait()
        except Empty:
            return None

    def task_done(self):
        try:
            self._q.task_done()
        except Exception:
            pass

    def compute_want(self, target_local_queue: int) -> int:
        """
        If our local queue is below target, compute how many more items
        we can accept.
        """
        current = self.size()
        if current >= target_local_queue:
            return 0
        return max(0, target_local_queue - current)
