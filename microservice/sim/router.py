# sim/router.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Deque, List, Optional
from collections import deque
import random

from sim.engine import Engine
from sim.models import Endpoint, SimRequest
from sim.service_model import ServiceModel


@dataclass
class Router:
    mode: str  # "pull" | "push_rr" | "push_random" | "push_lq"
    endpoints: List[Endpoint]
    service: ServiceModel
    rng: random.Random

    q: Deque[SimRequest] = field(default_factory=deque)
    rr_i: int = 0

    def enqueue(self, req: SimRequest, now: float):
        req.t_arrival_router = now
        self.q.append(req)

    def _available_eps(self) -> List[Endpoint]:
        return [e for e in self.endpoints if e.inflight < e.max_inflight]

    def _choose_ep_push(self) -> Optional[Endpoint]:
        if not self.endpoints:
            return None
        if self.mode == "push_rr":
            ep = self.endpoints[self.rr_i % len(self.endpoints)]
            self.rr_i += 1
            return ep
        if self.mode == "push_random":
            return self.rng.choice(self.endpoints)
        # push_lq
        return min(self.endpoints, key=lambda e: e.inflight)

    def step(self, eng: Engine):
        now = eng.t
        if not self.q:
            return

        if self.mode == "pull":
            while self.q:
                avail = self._available_eps()
                if not avail:
                    return
                ep = min(avail, key=lambda e: e.inflight)
                self._dispatch_one(eng, now, ep, self.q.popleft())
            return

        while self.q:
            avail = self._available_eps()
            if not avail:
                return

            ep = self._choose_ep_push()
            if ep is None:
                return
            if ep.inflight >= ep.max_inflight:
                ep = min(avail, key=lambda e: e.inflight)

            self._dispatch_one(eng, now, ep, self.q.popleft())

    def _dispatch_one(self, eng: Engine, now: float, ep: Endpoint, req: SimRequest):
        req.t_dispatch_router = now
        req.endpoint = ep.endpoint_id

        n_decode = ep.inflight + 1
        t_prefill, t_decode = self.service.times(
            in_tokens=req.in_tokens,
            out_tokens=req.out_tokens,
            prefill_tps=ep.prefill_tps,
            decode_tps=ep.decode_tps,
            n_decode=n_decode,
            rng=self.rng,
        )
        req.t_prefill_s = t_prefill
        req.t_decode_s = t_decode

        ep.inflight += 1
        done_t = now + t_prefill + t_decode
        eng.schedule(done_t, "COMPLETE", (ep.endpoint_id, req))
