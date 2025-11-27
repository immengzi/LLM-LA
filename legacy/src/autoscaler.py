# -*- coding: utf-8 -*-
"""
autoscaler.py
Queue/backlog-driven autoscaler with two modes:
  - "virtual": we choose an active subset of endpoints (others may be draining).
  - "real": placeholder hooks for real K8s scale-out/in (not implemented yet).

Supports graceful scale-in with draining:
  - When desired shrinks, move endpoints to a draining set.
  - Draining endpoints receive no new work but remain until inflight==0.
"""

import time
from typing import List, Tuple, Dict, Set
from config import get_config


class QueueBacklogAutoscaler:
    """
    Decide desired server count from queue/backlog with hysteresis + debounce,
    and manage a draining set to avoid yanking endpoints mid-flight.

    step_and_select(all_eps, *, queue_len, inflight_by_ep) ->
        (active_eps, draining_eps, desired_servers, reason, changed)
    """

    def __init__(
        self,
        *,
        mode: str = "virtual",
        target_q_per_server: int = 10,
        min_servers: int = 1,
        max_servers: int = 10_000,
        hysteresis: float = 0.20,
        debounce_s: float = 1.0,
        router_mode_name: str = "router",
    ):
        self.cfg = get_config()
        self.mode = (mode or "virtual").lower().strip()
        self.target_q = max(1, int(target_q_per_server))
        self.min_s = max(1, int(min_servers))
        self.max_s = max(self.min_s, int(max_servers))
        self.hyst = max(0.0, float(hysteresis))
        self.debounce_s = max(0.0, float(debounce_s))
        self.router_mode_name = router_mode_name

        self._last_change_ts = 0.0
        self._cur_desired = None  # type: int | None

        # Draining / sticky membership
        self._active_set: Set[str] = set()
        self._draining: Set[str] = set()
        self._last_sig = None

    # ---------------- helpers ----------------

    def _desired_from_backlog(self, queue_len: int, total_eps: int) -> int:
        backlog = max(0, int(queue_len))
        if backlog <= 0:
            base = 1
        else:
            # ceil(queue / target_q_per_server)
            base = (backlog + self.target_q - 1) // self.target_q
        return max(self.min_s, min(self.max_s, base, int(total_eps)))

    def _apply_hysteresis(self, desired: int) -> int:
        if self._cur_desired is None:
            return desired
        cur = int(self._cur_desired)
        delta = abs(desired - cur) / float(max(1, cur))
        if delta < self.hyst:
            return cur
        return desired

    def _debounced(self, new_desired: int) -> Tuple[int, bool]:
        now = time.time()
        if self._cur_desired is None:
            self._cur_desired = new_desired
            self._last_change_ts = now
            return new_desired, True
        if new_desired != self._cur_desired:
            if (now - self._last_change_ts) >= self.debounce_s:
                self._cur_desired = new_desired
                self._last_change_ts = now
                return new_desired, True
            else:
                return self._cur_desired, False
        return self._cur_desired, False

    def _choose_scale_in_victims(
        self,
        want_remove: int,
        ordering: List[str],
        inflight_by_ep: Dict[str, int],
    ) -> List[str]:
        if want_remove <= 0:
            return []
        candidates = [ep for ep in ordering if ep in self._active_set]
        pos = {ep: i for i, ep in enumerate(ordering)}
        candidates.sort(key=lambda ep: (int(inflight_by_ep.get(ep, 0)), pos[ep]))
        return candidates[:want_remove]

    # ---------------- main API ----------------

    def step_and_select(
        self,
        all_eps: List[str],
        *,
        queue_len: int,
        inflight_by_ep: Dict[str, int],
    ) -> Tuple[List[str], List[str], int, str, bool]:
        """
        Returns:
          active_eps, draining_eps, desired_server_count, reason, changed
        """
        total = len(all_eps)
        desired_raw = self._desired_from_backlog(queue_len, total)
        desired_hys = self._apply_hysteresis(desired_raw)
        desired, changed_desired = self._debounced(desired_hys)

        ordering = list(all_eps)

        # Prune membership to currently known endpoints
        cur_set = set(all_eps)
        self._active_set &= cur_set
        self._draining &= cur_set

        # Seed active set on first run
        if not self._active_set and desired > 0:
            self._active_set = set(ordering[:desired])

        # Grow: promote from pool
        pool = [ep for ep in ordering if ep not in self._active_set | self._draining]
        need_add = max(0, desired - len(self._active_set))
        if need_add > 0:
            for ep in pool[:need_add]:
                self._active_set.add(ep)

        # Shrink: move to draining
        need_remove = max(0, len(self._active_set) - desired)
        if need_remove > 0:
            victims = self._choose_scale_in_victims(need_remove, ordering, inflight_by_ep)
            for ep in victims:
                if ep in self._active_set:
                    self._active_set.remove(ep)
                    self._draining.add(ep)

        # Reap drained (idle) endpoints
        for ep in list(self._draining):
            if int(inflight_by_ep.get(ep, 0)) == 0:
                self._draining.discard(ep)

        active_eps = [ep for ep in ordering if ep in self._active_set]
        draining_eps = [ep for ep in ordering if ep in self._draining]

        changed = changed_desired
        state_sig = (tuple(active_eps), tuple(draining_eps), desired)
        if self._last_sig != state_sig:
            changed = True
            self._last_sig = state_sig

        if self.mode == "real":
            # TODO: implement real K8s scale-out/in here
            pass

        reason = f"autoscale-enabled-{self.mode}"
        return active_eps, draining_eps, desired, reason, changed
