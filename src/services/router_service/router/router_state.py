# router/router_state.py
# -*- coding: utf-8 -*-

from __future__ import annotations

from collections import deque
from threading import RLock
from typing import Deque, Dict, Tuple, List, Any, Optional
import sys
import uuid
import asyncio
import time

from .config import get_config, get_model_registry
from .kv_aware import prefix_len
from .predictors import get_length_predictor
from .len_select import select_len_aware
from .models import JobItem, now_s
from .metrics import set_central_queue_length, inc_dispatch

_cfg = get_config()
_pred = get_length_predictor()

_DEFAULT_MODEL = _cfg.MODEL_NAME


def _log_req(msg: str, *, level: str = "summary") -> None:
    """
    Centralized logging for pull-routing decisions.
    Honors _cfg.REQ_LOG_MODE: off | summary | full
    """
    mode = str(_cfg.REQ_LOG_MODE).lower()

    if mode == "off":
        return

    if level == "summary":
        print(f"[PullRouter] {msg}")
        sys.stdout.flush()
    elif level == "full" and mode == "full":
        print(f"[PullRouter] {msg}")
        sys.stdout.flush()


# -------------------------------------------------------
# Lazy SLO-aware imports (only resolved when SLO_AWARE=true)
# -------------------------------------------------------
_slo_deps_loaded = False
_slo_registry = None
_latency_predictor = None
_batch_estimator = None
_queue_wait_estimator = None


def _ensure_slo_deps():
    """Lazy-load SLO dependencies on first use."""
    global _slo_deps_loaded, _slo_registry, _latency_predictor, _batch_estimator, _queue_wait_estimator
    if _slo_deps_loaded:
        return
    _slo_deps_loaded = True

    try:
        from .latency_predictor import get_latency_predictor
        from .slo_scoring import BatchSizeEstimator, QueueWaitEstimator

        _latency_predictor = get_latency_predictor()

        _batch_estimator = BatchSizeEstimator(
            mode=str(getattr(_cfg, "BATCH_SIZE_ESTIMATE", "fixed")),
            fixed_value=int(getattr(_cfg, "FIXED_BATCH_ESTIMATE", 8)),
        )

        _queue_wait_estimator = QueueWaitEstimator(
            mode=str(getattr(_cfg, "QUEUE_WAIT_MODEL", "none")),
        )
    except Exception as e:
        print(f"[PullRouter] WARNING: failed to load SLO deps: {e}")
        sys.stdout.flush()


def _get_slo_registry():
    """Get the SLO registry from api module (avoids circular import at module level)."""
    global _slo_registry
    if _slo_registry is not None:
        return _slo_registry
    try:
        from . import api as _api_mod
        _slo_registry = getattr(_api_mod, "_slo_registry", None)
    except Exception:
        pass
    return _slo_registry


class RouterState:
    """
    Central queue of pending jobs + KV + length-aware selection.

      - Result waiters are asyncio Futures
      - This avoids the router enqueue handler blocking in asyncio.to_thread(...)
        and eliminates massive router_wakeup_s artifacts under load.

    Additions (for async / decoupled flows):
      - Result retention (TTL) so /submit (async_pubsub) and push-dispatch decoupling
        don't leak _result_values forever if the client never waits.
      - Periodic cleanup loop to bound memory.
    """

    def __init__(self):
        self._lock = RLock()

        # Per-model queues. Key = model name (or _DEFAULT_MODEL for legacy).
        # queue entries: (req_id, prompt, t_enq_client_or_router, meta)
        self._queues: Dict[str, Deque[Tuple[str, str, float, dict]]] = {}
        # Legacy alias — kept so callers that read _queue still work
        self._queue: Deque[Tuple[str, str, float, dict]] = self._get_queue(_DEFAULT_MODEL)

        # Result tracking (async-native)
        # req_id -> asyncio.Future that will hold the result (or None placeholder if no loop)
        self._result_futs: Dict[str, Any] = {}
        # req_id -> stored result (for early-arriving /result before waiter exists)
        self._result_values: Dict[str, Any] = {}
        # req_id -> time when result was stored (for TTL cleanup)
        self._result_store_ts: Dict[str, float] = {}

        # Streaming chunk queues: req_id -> asyncio.Queue
        self._chunk_queues: Dict[str, asyncio.Queue] = {}

        # req_id -> endpoint that pulled it (for streaming identity)
        self._req_endpoint: Dict[str, str] = {}

        # background cleanup task (lazy-start)
        self._cleanup_task: Optional[asyncio.Task] = None

        # initialize gauge
        set_central_queue_length(0)

    def _get_queue(self, model: str) -> Deque[Tuple[str, str, float, dict]]:
        """Get or create the queue for a model (must be called under lock or at init)."""
        q = self._queues.get(model)
        if q is None:
            q = deque()
            self._queues[model] = q
        return q

    # -------------------------------------------------------
    # ID allocation
    # -------------------------------------------------------

    def next_req_id(self) -> str:
        with self._lock:
            return uuid.uuid4().hex

    # -------------------------------------------------------
    # Enqueue
    # -------------------------------------------------------

    def enqueue(self, prompt: str, t_enq_client: float | None, meta: dict, model: str = "") -> str:
        """
        Enqueue a new request in pull mode.

        t_enq_client: client-side enqueue timestamp (if provided),
        otherwise we stamp with router now().
        model: target model queue (empty = default MODEL_NAME).
        """
        with self._lock:
            rid = self.next_req_id()
            ts = float(t_enq_client) if t_enq_client else now_s()
            q = self._get_queue(model or _DEFAULT_MODEL)
            q.append((rid, prompt, ts, meta or {}))
            set_central_queue_length(self._total_size())
            return rid

    def update_meta(self, req_id: str, meta: dict) -> None:
        """
        In-place update of meta for a queued request.

        Used by the API layer to inject trace info after enqueue without
        changing queue order or timestamps.
        """
        with self._lock:
            for model_name, q in self._queues.items():
                if not q:
                    continue
                new_q: Deque[Tuple[str, str, float, dict]] = deque()
                updated = False
                while q:
                    rid, prompt, ts, old_meta = q.popleft()
                    if not updated and rid == req_id:
                        new_q.append((rid, prompt, ts, meta or {}))
                        updated = True
                    else:
                        new_q.append((rid, prompt, ts, old_meta))
                self._queues[model_name] = new_q
                if updated:
                    break

            set_central_queue_length(self._total_size())

    # -------------------------------------------------------
    # Pull (KV-aware + length-aware)
    # -------------------------------------------------------

    def pull_for_endpoint(self, endpoint: str, want: int, model: str = "") -> List[JobItem]:
        if want <= 0:
            return []

        with self._lock:
            q = self._get_queue(model or _DEFAULT_MODEL)
            if not q:
                set_central_queue_length(self._total_size())
                return []

            pool_factor = max(1, int(_cfg.POOL_FACTOR))
            max_scan = min(len(q), want * pool_factor)

            # 1) Build pool
            pool: List[Tuple[str, str, float, dict]] = []
            for _ in range(max_scan):
                rid, prompt, ts, meta = q.popleft()
                pool.append((rid, prompt, ts, meta))

            # queue length changed after draining pool
            set_central_queue_length(self._total_size())

            _log_req(
                f"endpoint={endpoint} want={want} pool_size={len(pool)} "
                f"queue_remaining={len(self._queue)}",
                level="full",
            )

            # ===================================================
            # Branch: SLO-aware vs legacy path
            # ===================================================
            slo_aware = bool(getattr(_cfg, "SLO_AWARE", False))

            if slo_aware and _cfg.SLO_AWARE:
                ordered, kv_hits_map = self._slo_aware_sort(pool, endpoint, want)
            else:
                ordered, kv_hits_map = self._legacy_sort(pool, endpoint)

            # ===================================================
            # Effective want: apply admission controls
            # ===================================================
            effective_want = want

            # Step 6: Fixed batch size cap
            fixed_cap = int(getattr(_cfg, "FIXED_BATCH_SIZE", 0))
            if fixed_cap > 0:
                effective_want = min(effective_want, fixed_cap)

            # Step 7: Dynamic admission throttling
            if slo_aware and bool(getattr(_cfg, "ADMISSION_THROTTLE", False)):
                effective_want = self._apply_admission_throttle(
                    effective_want, endpoint,
                )

            # 5) Choose
            kv_enabled = bool(_cfg.KV_AWARE)
            chosen_raw = ordered[:effective_want]
            chosen_ids = [rid for (rid, _p, _t, _m) in chosen_raw]
            chosen_kv_hits = [(rid, prefix_len(endpoint, rid)) for rid in chosen_ids] if kv_enabled else []

            _log_req(
                f"chosen endpoint={endpoint}: {chosen_ids} kv_hits={chosen_kv_hits}"
                + (f" effective_want={effective_want}" if effective_want != want else ""),
                level="summary",
            )

            # 5a) Attach trace info (if enabled)
            chosen: List[Tuple[str, str, float, dict]] = []
            dispatch_ts = now_s()
            if getattr(_cfg, "TRACE_ENABLED", False):
                qlen_at_dispatch = len(self._queue)

                for rid, prompt, ts, meta in chosen_raw:
                    m = dict(meta or {})
                    tr = dict(m.get("__trace__") or {})

                    tr.setdefault("t_enq_router_queue", ts)
                    tr.setdefault("endpoint", endpoint)

                    tr["t_dispatch_router"] = dispatch_ts
                    tr["router_queue_len_at_dispatch"] = qlen_at_dispatch

                    if kv_enabled:
                        tr["kv_hits_len"] = int(kv_hits_map.get(rid, 0))

                    # SLO trace enrichment
                    if slo_aware:
                        slo_reg = _get_slo_registry()
                        if slo_reg:
                            slo_entry = slo_reg.get(rid)
                            if slo_entry:
                                tr["slo_type"] = slo_entry.slo_type
                                tr["slo_slack"] = slo_entry.slack
                                tr["slo_binding"] = slo_entry.binding_constraint
                                tr["slo_predicted_output_len"] = slo_entry.predicted_output_len

                    m["__trace__"] = tr
                    chosen.append((rid, prompt, ts, m))
            else:
                chosen = chosen_raw

            # SLO dispatch tracking: update SLO registry with dispatch info
            if slo_aware:
                slo_reg = _get_slo_registry()
                if slo_reg:
                    for rid, _p, _t, _m in chosen:
                        slo_entry = slo_reg.get(rid)
                        if slo_entry:
                            slo_reg.update_dispatch(
                                rid,
                                endpoint=endpoint,
                                predicted_ttft=slo_entry.predicted_ttft,
                                predicted_tpot=slo_entry.predicted_tpot,
                                predicted_e2e=slo_entry.predicted_e2e,
                                slack=slo_entry.slack,
                                binding_constraint=slo_entry.binding_constraint,
                            )

                # Step 7: increment inflight for admission tracking
                if _batch_estimator is not None:
                    _batch_estimator.increment_inflight(endpoint, len(chosen))

            # Prom: outgoing dispatch (router -> sidecar) for each assigned item
            for _rid, _prompt, _ts, _meta in chosen:
                inc_dispatch(endpoint)
                self._req_endpoint[_rid] = endpoint

            # SLO metrics: observe slack at dispatch
            if slo_aware:
                try:
                    from .metrics import observe_slo_slack, inc_slo_predicted_miss
                    slo_reg = _get_slo_registry()
                    if slo_reg:
                        for rid, _p, _t, _m in chosen:
                            slo_entry = slo_reg.get(rid)
                            if slo_entry and slo_entry.slack is not None:
                                observe_slo_slack(slo_entry.slack)
                                if slo_entry.slack < 0:
                                    inc_slo_predicted_miss()
                except Exception:
                    pass

            # 6) Requeue leftovers
            leftovers = ordered[effective_want:]
            for rid, prompt, ts, meta in leftovers:
                q.appendleft((rid, prompt, ts, meta))

            # queue length changed after requeue
            set_central_queue_length(self._total_size())

            if leftovers:
                _log_req(
                    f"leftovers requeued: "
                    f"{[r for (r, _p, _t, _m) in leftovers]}",
                    level="full",
                )

            # 7) Build output
            items = [
                JobItem(req_id=rid, prompt=prompt, t_enq_client=ts, meta=meta)
                for (rid, prompt, ts, meta) in chosen
            ]
            return items

    # -------------------------------------------------------
    # Legacy sort (existing KV-first + length-aware path)
    # MUST remain byte-for-byte identical when SLO_AWARE=false
    # -------------------------------------------------------

    def _legacy_sort(
        self,
        pool: List[Tuple[str, str, float, dict]],
        endpoint: str,
    ) -> Tuple[List[Tuple[str, str, float, dict]], Dict[str, int]]:
        """Returns (ordered_list, kv_hits_map)."""
        kv_enabled = bool(_cfg.KV_AWARE)
        len_enabled = bool(_cfg.LEN_AWARE)
        len_policy = str(_cfg.LEN_POLICY or "")

        kv_pairs: List[Tuple[str, int]] = []
        pool_with_kv: List[Tuple[str, str, float, dict, int]] = []

        if kv_enabled:
            for rid, prompt, ts, meta in pool:
                kv_hits = prefix_len(endpoint, rid)
                kv_pairs.append((rid, kv_hits))
                pool_with_kv.append((rid, prompt, ts, meta, kv_hits))

            _log_req(
                f"KV raw endpoint={endpoint}: {kv_pairs}",
                level="full",
            )
        else:
            for rid, prompt, ts, meta in pool:
                pool_with_kv.append((rid, prompt, ts, meta, 0))

        kv_hits_map: Dict[str, int] = {}
        if kv_enabled:
            for rid, _prompt, _ts, _meta, kv_hits in pool_with_kv:
                kv_hits_map[rid] = int(kv_hits)

        kv_to_items: Dict[int, List[Tuple[str, str, float, dict]]] = {}
        for rid, prompt, ts, meta, kv_hits in pool_with_kv:
            kv_to_items.setdefault(int(kv_hits), []).append((rid, prompt, ts, meta))

        kv_levels = sorted(kv_to_items.keys(), reverse=True)

        if kv_enabled:
            _log_req(
                f"KV tiers endpoint={endpoint}: "
                f"{[(k, [r for (r, _p, _t, _m) in kv_to_items[k]]) for k in kv_levels]}",
                level="full",
            )

        ordered: List[Tuple[str, str, float, dict]] = []
        for kv_hits in kv_levels:
            tier = kv_to_items[kv_hits]
            tier.sort(key=lambda x: x[0])

            if len_enabled and len_policy:
                tier_refined = select_len_aware(tier, _pred, len_policy)
                tier = tier_refined

                _log_req(
                    f"Len refine within KV={kv_hits} policy='{len_policy}': "
                    f"{[r for (r, _p, _t, _m) in tier]}",
                    level="full",
                )

            ordered.extend(tier)

        if kv_enabled:
            _log_req(
                f"KV-first final order endpoint={endpoint}: "
                f"{[(r, prefix_len(endpoint, r)) for (r, _p, _t, _m) in ordered]}",
                level="full",
            )

        return ordered, kv_hits_map

    # -------------------------------------------------------
    # SLO-aware sort (slack-ascending with secondary KV/load sort)
    # -------------------------------------------------------

    def _slo_aware_sort(
        self,
        pool: List[Tuple[str, str, float, dict]],
        endpoint: str,
        want: int,
    ) -> Tuple[List[Tuple[str, str, float, dict]], Dict[str, int]]:
        """
        Slack-based ordering (Steps 4-5-8).

        1. Compute slack for each pool item.
        2. Sort ascending by slack (most urgent first).
        3. Within equal-slack bands (quantized to 100ms):
           - TTFT-bound + SLO_WITH_KV: prefer highest cache hit ratio.
           - TPOT-bound + SLO_WITH_KV: prefer least-loaded endpoint.
           - SLO_WITH_KV=false: pure slack order.
        4. Negative-slack bypass: skip KV scoring for predicted misses (Step 8).

        Returns (ordered_list, kv_hits_map).
        """
        _ensure_slo_deps()

        slo_reg = _get_slo_registry()
        kv_enabled = bool(_cfg.KV_AWARE)
        slo_with_kv = bool(getattr(_cfg, "SLO_WITH_KV", True))

        kv_hits_map: Dict[str, int] = {}

        # Compute KV hits
        if kv_enabled:
            for rid, _p, _t, _m in pool:
                kv_hits_map[rid] = prefix_len(endpoint, rid)

        # Compute slack for each item
        scored: List[Tuple[str, str, float, dict, float, Optional[str], int]] = []

        for rid, prompt, ts, meta in pool:
            slack = float("inf")
            binding: Optional[str] = None
            cached_blocks = kv_hits_map.get(rid, 0)

            if slo_reg and _latency_predictor:
                entry = slo_reg.get(rid)
                if entry and entry.slo_type:
                    from .slo_scoring import compute_slack as _compute_slack

                    batch_size = _batch_estimator.estimate(endpoint) if _batch_estimator else 8
                    queue_wait = _queue_wait_estimator.estimate(endpoint) if _queue_wait_estimator else 0.0

                    slack, binding = _compute_slack(
                        entry,
                        endpoint,
                        latency_predictor=_latency_predictor,
                        batch_size=batch_size,
                        cached_tokens=cached_blocks,
                        queue_wait_s=queue_wait,
                    )

                    # Store back into SLO entry for trace/metrics
                    entry.slack = slack
                    entry.binding_constraint = binding
                    if _latency_predictor:
                        entry.predicted_ttft = _latency_predictor.predict_ttft(
                            entry.input_tokens,
                            cached_blocks * 16,
                            batch_size,
                        )

            scored.append((rid, prompt, ts, meta, slack, binding, cached_blocks))

        # Sort: primary = slack ascending (most urgent first)
        # Secondary sort within 100ms bands:
        SLACK_BAND_MS = 100.0

        def _sort_key(item):
            rid, _p, _t, _m, slack, binding, cached = item
            # Quantize slack to 100ms bands for equal-slack grouping
            if slack == float("inf"):
                band = float("inf")
            elif slack == float("-inf"):
                band = float("-inf")
            else:
                band = round(slack * 1000 / SLACK_BAND_MS) * SLACK_BAND_MS

            # Step 8: negative-slack bypass -- don't reward KV, prefer least-loaded
            if slack < 0:
                secondary = 0
            elif slo_with_kv and kv_enabled:
                if binding == "tpot":
                    secondary = 0
                else:
                    secondary = -cached
            else:
                secondary = 0

            # Tertiary: req_id for determinism
            return (band, secondary, rid)

        scored.sort(key=_sort_key)

        _log_req(
            f"SLO-aware sort endpoint={endpoint}: "
            f"{[(r, f'slack={s:.3f}' if s != float('inf') else 'no-slo', b) for r, _p, _t, _m, s, b, _c in scored[:10]]}",
            level="full",
        )

        ordered = [(rid, prompt, ts, meta) for rid, prompt, ts, meta, _s, _b, _c in scored]
        return ordered, kv_hits_map

    # -------------------------------------------------------
    # Admission throttle (Step 7)
    # -------------------------------------------------------

    def _apply_admission_throttle(self, current_want: int, endpoint: str) -> int:
        """Dynamic admission throttling via binary search on predicted TPOT."""
        if _latency_predictor is None or _batch_estimator is None:
            return current_want

        slo_reg = _get_slo_registry()
        if not slo_reg:
            return current_want

        # Find the tightest TPOT budget among queued requests
        tpot_budget = None
        avg_accum = 2048  # reasonable default for accumulated length

        # Use entries from the SLO registry to find budget
        # (simplified: use FIXED_BATCH_ESTIMATE as proxy)
        try:
            from .admission import compute_max_safe_admit
            inflight = _batch_estimator.get_inflight(endpoint)

            # Find a TPOT budget from pending SLO entries (use first available)
            # In practice this should look at the pool's entries, but we simplify
            # to a global budget here.
            tpot_budget_s = None
            # Check recent entries for TPOT budget across all queues
            all_items = []
            for _q in self._queues.values():
                all_items.extend(list(_q)[:50])
            for rid, _p, _t, _m in all_items[:50]:
                entry = slo_reg.get(rid)
                if entry and entry.deadline_tpot_s is not None:
                    if tpot_budget_s is None or entry.deadline_tpot_s < tpot_budget_s:
                        tpot_budget_s = entry.deadline_tpot_s

            if tpot_budget_s is None:
                return current_want

            max_admit = compute_max_safe_admit(
                _latency_predictor,
                inflight,
                tpot_budget_s,
                avg_accum,
            )
            return min(current_want, max_admit)
        except Exception:
            return current_want

    # -------------------------------------------------------
    # Result wait/notify (ASYNC) + TTL retention
    # -------------------------------------------------------

    def _ensure_cleanup_task(self) -> None:
        """
        Start background TTL cleanup task once we are in an event loop.
        Safe to call multiple times.
        """
        if self._cleanup_task is not None and not self._cleanup_task.done():
            return

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return

        interval = float(getattr(_cfg, "POLL_CLEANUP_INTERVAL_S", 1.0))
        if interval <= 0:
            return

        self._cleanup_task = loop.create_task(self._cleanup_loop())

    async def _cleanup_loop(self) -> None:
        """
        Periodically remove stored results that have exceeded TTL.
        Only affects _result_values/_result_store_ts and None-placeholder futs.
        """
        interval = max(0.1, float(getattr(_cfg, "POLL_CLEANUP_INTERVAL_S", 1.0)))
        ttl = max(1.0, float(getattr(_cfg, "POLL_RESULT_TTL_S", 300.0)))

        while True:
            try:
                await asyncio.sleep(interval)
                now = time.time()
                to_del: List[str] = []

                with self._lock:
                    for rid, ts in list(self._result_store_ts.items()):
                        if (now - float(ts)) >= ttl:
                            to_del.append(rid)

                    for rid in to_del:
                        self._result_store_ts.pop(rid, None)
                        self._result_values.pop(rid, None)
                        self._req_endpoint.pop(rid, None)

                        # Drop placeholder fut=None (created when no loop existed)
                        fut = self._result_futs.get(rid)
                        if fut is None:
                            self._result_futs.pop(rid, None)

            except asyncio.CancelledError:
                raise
            except Exception:
                continue

    def register_waiter(self, req_id: str) -> None:
        """
        Ensure an asyncio Future exists for this req_id.
        If a result already arrived (early /result), resolve immediately.

        Also kicks off TTL cleanup loop (best-effort).
        """
        self._ensure_cleanup_task()

        with self._lock:
            if req_id in self._result_futs:
                return

            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None

            if loop is None:
                # placeholder (so we can later GC if no one ever waits)
                self._result_futs[req_id] = None
                return

            fut: asyncio.Future = loop.create_future()
            self._result_futs[req_id] = fut

            if req_id in self._result_values and not fut.done():
                fut.set_result(self._result_values[req_id])

    def store_result(self, req_id: str, result: Any) -> None:
        """
        Store result and resolve any waiting Future.

        Also records store timestamp for TTL cleanup.
        """
        fut: Optional[asyncio.Future] = None
        now = time.time()

        with self._lock:
            self._result_values[req_id] = result
            self._result_store_ts[req_id] = now
            maybe = self._result_futs.get(req_id)
            fut = maybe if isinstance(maybe, asyncio.Future) else None

        if fut is None:
            return
        if fut.done():
            return

        try:
            loop = fut.get_loop()
        except Exception:
            loop = None

        def _set():
            if not fut.done():
                fut.set_result(result)

        if loop is None:
            try:
                _set()
            except Exception:
                pass
            return

        try:
            loop.call_soon_threadsafe(_set)
        except Exception:
            try:
                _set()
            except Exception:
                pass

    async def wait_for_result_async(self, req_id: str, timeout_s: float) -> Optional[Any]:
        """
        Await the result for req_id up to timeout_s.

        Note:
          - Always drops the waiter Future on exit to avoid leaks.
          - Does NOT delete stored results on timeout (result may arrive later);
            TTL cleanup bounds retention.
        """
        self._ensure_cleanup_task()

        loop = asyncio.get_running_loop()

        with self._lock:
            existing = self._result_futs.get(req_id)
            if not isinstance(existing, asyncio.Future) or existing.get_loop() is not loop:
                fut = loop.create_future()
                self._result_futs[req_id] = fut
            else:
                fut = existing

            if req_id in self._result_values and not fut.done():
                fut.set_result(self._result_values[req_id])

        try:
            return await asyncio.wait_for(fut, timeout=float(timeout_s))
        except asyncio.TimeoutError:
            return None
        finally:
            with self._lock:
                self._result_futs.pop(req_id, None)

    # -------------------------------------------------------
    # Streaming chunk queues
    # -------------------------------------------------------

    def register_chunk_queue(self, req_id: str) -> asyncio.Queue:
        """Create an asyncio.Queue for streaming chunks keyed by req_id."""
        q: asyncio.Queue = asyncio.Queue()
        with self._lock:
            self._chunk_queues[req_id] = q
        return q

    def push_chunk(self, req_id: str, chunk: Dict[str, Any]) -> bool:
        """
        Push a chunk into the queue for req_id (thread-safe).
        Returns False if no queue is registered for this req_id.
        """
        with self._lock:
            q = self._chunk_queues.get(req_id)
        if q is None:
            return False

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is not None and loop.is_running():
            loop.call_soon_threadsafe(q.put_nowait, chunk)
        else:
            q.put_nowait(chunk)
        return True

    def remove_chunk_queue(self, req_id: str) -> None:
        """Remove the chunk queue for req_id (cleanup)."""
        with self._lock:
            self._chunk_queues.pop(req_id, None)
            self._req_endpoint.pop(req_id, None)

    def get_req_endpoint(self, req_id: str) -> Optional[str]:
        """Return the endpoint that pulled this req_id, or None."""
        with self._lock:
            return self._req_endpoint.get(req_id)

    def has_chunk_queue(self, req_id: str) -> bool:
        with self._lock:
            return req_id in self._chunk_queues

    # -------------------------------------------------------
    # Metrics
    # -------------------------------------------------------

    def _total_size(self) -> int:
        """Total items across all model queues (must be called under lock)."""
        return sum(len(q) for q in self._queues.values())

    def size(self, model: str = "") -> int:
        with self._lock:
            if model:
                q = self._queues.get(model)
                return len(q) if q else 0
            return self._total_size()


# global instance
router_state = RouterState()
