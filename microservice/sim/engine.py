# sim/engine.py
from __future__ import annotations
import heapq
from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Tuple


@dataclass(frozen=True)
class Event:
    t: float
    kind: str
    payload: Any
    eid: int


class Engine:
    def __init__(self):
        self.t: float = 0.0
        self._q: List[Tuple[float, int, Event]] = []
        self._next_eid: int = 1

    def schedule(self, t: float, kind: str, payload: Any):
        eid = self._next_eid
        self._next_eid += 1
        ev = Event(t=float(t), kind=str(kind), payload=payload, eid=eid)
        heapq.heappush(self._q, (ev.t, ev.eid, ev))

    def run(self, until: Optional[float], handler: Callable[[Event], None]):
        while self._q:
            t, _, ev = heapq.heappop(self._q)
            if until is not None and t > until:
                break
            self.t = t
            handler(ev)

    def empty(self) -> bool:
        return not self._q
