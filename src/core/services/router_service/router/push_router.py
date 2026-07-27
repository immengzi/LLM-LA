# router/push_router.py
# -*- coding: utf-8 -*-
import os
import re
import math
import random
import time
import asyncio
from collections import defaultdict
from typing import Dict, List, Optional
from threading import RLock

import httpx

from .config import get_config
from .k8s_discovery import discover_running_pods
from .metrics import inc_dispatch
from .kv_aware import get_request_blocks, prefix_len, record_routing

_cfg = get_config()

# One Prometheus exposition line: name{labels} value  (comments/blank skipped).
_PROM_LINE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([0-9eE.+-]+)\s*$")


def _pick_min_score(scores: Dict[str, Optional[float]]) -> Optional[str]:
    """Return the endpoint with the smallest numeric score.

    Endpoints whose score is ``None`` (probe failed / metric absent) are
    skipped. Returns ``None`` when no endpoint has a usable score.
    """
    best_ep: Optional[str] = None
    best: Optional[float] = None
    for ep, s in scores.items():
        if s is None:
            continue
        if best is None or s < best:
            best = s
            best_ep = ep
    return best_ep


def _parse_prom_sums(text: str, bases) -> Dict[str, Optional[float]]:
    """Sum each requested metric base name across all its label sets.

    Returns ``{base: total}``, or ``None`` for a base that never appears.
    """
    wanted = set(bases)
    out = {b: 0.0 for b in bases}
    seen = {b: False for b in bases}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line[0] == "#":
            continue
        m = _PROM_LINE.match(line)
        if not m:
            continue
        name, val = m.group(1), m.group(3)
        if name in wanted:
            try:
                out[name] += float(val)
                seen[name] = True
            except ValueError:
                pass
    return {b: (out[b] if seen[b] else None) for b in bases}


def _total_tokens_from_sums(sums: Dict[str, Optional[float]]) -> Optional[float]:
    """Total tokens processed = prompt_tokens_total + generation_tokens_total.

    Returns ``None`` only when neither counter is present; if just one is
    missing it is treated as 0 so a partially-instrumented pod still scores.
    """
    p = sums.get("vllm:prompt_tokens_total")
    g = sums.get("vllm:generation_tokens_total")
    if p is None and g is None:
        return None
    return (p or 0.0) + (g or 0.0)


def _avg_latency_from_sums(sums: Dict[str, Optional[float]]) -> Optional[float]:
    """Average e2e latency = sum/count. Idle pod (count 0) scores 0.0 (free)."""
    s = sums.get("vllm:e2e_request_latency_seconds_sum")
    c = sums.get("vllm:e2e_request_latency_seconds_count")
    if s is None or c is None:
        return None
    if c <= 0:
        return 0.0
    return s / c



def _pick_lower_load(a: str, sa: Optional[float], b: str, sb: Optional[float]) -> str:
    """Power-of-two-choices comparator: return the less-loaded of two endpoints.

    A ``None`` score means the load probe failed for that endpoint; it is
    treated as +inf so a reachable peer wins. If both fail, the first sampled
    endpoint is returned (a random pick).
    """
    if sa is None and sb is None:
        return a
    if sa is None:
        return b
    if sb is None:
        return a
    return a if sa <= sb else b


def _kv_cost(prefill_blocks: int, hits: int, load: float,
             overlap_credit: float, prefill_load_scale: float) -> float:
    """KV-aware routing cost for one worker.

    cost = prefill_load_scale * max(prefill_blocks - overlap_credit * hits, 0) + load

    ``prefill_blocks`` is the request's total block count, ``hits`` is the
    contiguous cached-prefix blocks already resident on the worker, and
    ``load`` is a decode-load proxy (logical inflight). Higher ``overlap_credit``
    rewards cache reuse (lower TTFT); larger ``prefill_load_scale`` weights
    prompt-side work over decode load. Lower cost = better worker.
    """
    adjusted = prefill_blocks - overlap_credit * hits
    if adjusted < 0:
        adjusted = 0.0
    return prefill_load_scale * adjusted + load


def _select_by_cost(costs: Dict[str, float], temperature: float,
                    rng: Optional["random.Random"] = None) -> Optional[str]:
    """Pick a worker from a cost map.

    temperature <= 0 -> deterministic argmin (ties: first inserted).
    temperature  > 0 -> softmax sampling over the negated, normalized costs
    (a ``ROUTER_TEMPERATURE`` knob that spreads load).
    """
    if not costs:
        return None
    items = list(costs.items())
    if temperature is None or temperature <= 0:
        best_ep, best = items[0]
        for ep, c in items[1:]:
            if c < best:
                best, best_ep = c, ep
        return best_ep
    # Softmax over -cost/temperature, shifted by the min cost for stability.
    lo = min(c for _, c in items)
    weights = [math.exp(-(c - lo) / temperature) for _, c in items]
    total = sum(weights)
    if total <= 0:
        return items[0][0]
    r = (rng or random).random() * total
    acc = 0.0
    for (ep, _), w in zip(items, weights):
        acc += w
        if r <= acc:
            return ep
    return items[-1][0]


def _log_req(msg: str, *, level: str = "summary") -> None:
    """
    Centralized logging for push-routing decisions.
    Uses _cfg.REQ_LOG_MODE directly.
    """
    mode = str(_cfg.REQ_LOG_MODE).lower()

    if mode == "off":
        return

    if level == "summary":
        print(f"[PushRouter] {msg}")
    elif level == "full" and mode == "full":
        print(f"[PushRouter] {msg}")


class PushRouter:
    """
    Push-mode endpoint selector + sidecar push client.

    IMPORTANT for decoupled dispatch:
      - route_and_push() can be called concurrently by many background workers.
      - so endpoint discovery / endpoint lists / logical inflight bookkeeping must be guarded.
    """

    def __init__(self, mode: str):
        self.mode = mode  # "push-rr", "push-random", "push-leastq"

        # shared mutable state guarded by _lock
        self._lock = RLock()
        self._eps: List[str] = []        # pod names
        self._urls: Dict[str, str] = {}  # pod_name -> sidecar base URL
        self._rr_idx: int = 0
        self._last_discovery = 0.0
        self._discovery_interval_s = float(getattr(_cfg, "KV_DISCOVERY_INTERVAL_S", 5.0))
        self._leastq_mode: str = getattr(_cfg, "PUSH_LEASTQ_MODE", "health")
        # local logical queue lengths: sent - completed
        self._logical_inflight = defaultdict(int)

        # ------------------------------------------------------------------
        # Long-lived httpx clients (important for decoupled dispatch)
        #
        # IMPORTANT: httpx.Timeout must include either a default timeout
        # or explicitly set all four: connect/read/write/pool.
        # ------------------------------------------------------------------
        t = float(getattr(_cfg, "PUSH_HTTP_TIMEOUT_S", 2.0))
        timeout = httpx.Timeout(connect=t, read=t, write=t, pool=t)

        limits_health = httpx.Limits(
            max_keepalive_connections=int(getattr(_cfg, "PUSH_MAX_KEEPALIVE", 200)),
            max_connections=int(getattr(_cfg, "PUSH_MAX_KEEPALIVE", 200)),
            keepalive_expiry=float(getattr(_cfg, "PUSH_KEEPALIVE_EXPIRY_S", 30.0)),
        )
        limits_push = httpx.Limits(
            max_keepalive_connections=int(getattr(_cfg, "PUSH_MAX_KEEPALIVE", 200)),
            max_connections=int(getattr(_cfg, "PUSH_MAX_KEEPALIVE", 200)),
            keepalive_expiry=float(getattr(_cfg, "PUSH_KEEPALIVE_EXPIRY_S", 30.0)),
        )

        self._health_client = httpx.AsyncClient(timeout=timeout, limits=limits_health)
        self._push_client = httpx.AsyncClient(timeout=timeout, limits=limits_push)

    async def aclose(self) -> None:
        """
        Close underlying httpx clients. Safe to call multiple times.
        """
        try:
            await self._health_client.aclose()
        except Exception:
            pass
        try:
            await self._push_client.aclose()
        except Exception:
            pass

    # ---------------------------------------------------------
    # Pod discovery
    # ---------------------------------------------------------

    def _discover_pods(self) -> Dict[str, str]:
        # Shared implementation (see k8s_discovery.discover_running_pods); kept as
        # a thin wrapper so both the sidecar push path and the sidecar-less
        # central-push registry discover pods identically.
        return discover_running_pods(log_prefix="[PushRouter]")

    def _refresh_endpoints_locked(self, *, force: bool = False) -> None:
        """
        Refresh endpoint list/URLs if stale, or always if force=True.
        Caller must hold self._lock.
        """
        now = time.time()
        if (not force) and self._eps and (now - self._last_discovery) < self._discovery_interval_s:
            return

        pods = self._discover_pods()
        eps = list(pods.keys())
        urls = {pod: f"http://{ip}:{_cfg.SIDECAR_PORT}" for pod, ip in pods.items()}

        self._eps = eps
        self._urls = urls
        self._last_discovery = now

        # Keep rr index sane if endpoint set changes
        if self._eps:
            self._rr_idx = self._rr_idx % len(self._eps)
        else:
            self._rr_idx = 0

        _log_req(f"discovered {len(self._eps)} pods: {self._eps}", level="summary")

    def _ensure_endpoints(self) -> None:
        with self._lock:
            self._refresh_endpoints_locked(force=False)

    # ---------------------------------------------------------
    # Endpoint selection
    # ---------------------------------------------------------

    def _pick_endpoint_rr(self) -> Optional[str]:
        with self._lock:
            if not self._eps:
                return None
            ep = self._eps[self._rr_idx % len(self._eps)]
            self._rr_idx = (self._rr_idx + 1) % len(self._eps)

        _log_req(f"RR pick → {ep}", level="full")
        return ep

    def _pick_endpoint_random(self) -> Optional[str]:
        with self._lock:
            if not self._eps:
                return None
            eps = list(self._eps)

        ep = random.choice(eps)
        _log_req(f"Random pick → {ep}", level="full")
        return ep

    async def _pick_endpoint_leastq_health(self) -> Optional[str]:
        with self._lock:
            if not self._eps:
                return None
            eps = list(self._eps)
            urls = dict(self._urls)

        async def fetch_score(ep: str):
            url = urls.get(ep)
            if not url:
                return ep, None
            try:
                r = await self._health_client.get(f"{url}/health")
                if r.status_code != 200:
                    return ep, None
                data = r.json()
                score = int(data.get("logical", data.get("queue_len", 0)))
                return ep, score
            except Exception:
                return ep, None

        results = await asyncio.gather(*(fetch_score(ep) for ep in eps), return_exceptions=False)

        best_ep = None
        best_score = None
        for ep, score in results:
            if score is None:
                continue
            if best_score is None or score < best_score:
                best_score = score
                best_ep = ep

        _log_req(f"LeastQ(health) pick → {best_ep} (score={best_score})", level="full")
        return best_ep

    def _pick_endpoint_leastq_local(self) -> Optional[str]:
        with self._lock:
            if not self._eps:
                return None
            eps = list(self._eps)
            inflight = dict(self._logical_inflight)

        best_ep = None
        best_score = None
        for ep in eps:
            score = int(inflight.get(ep, 0))
            if best_score is None or score < best_score:
                best_score = score
                best_ep = ep

        _log_req(f"LeastQ(local) pick → {best_ep} (score={best_score})", level="full")
        return best_ep

    async def _pick_endpoint_leastq(self) -> Optional[str]:
        if self._leastq_mode == "local":
            return self._pick_endpoint_leastq_local()
        return await self._pick_endpoint_leastq_health()

    async def _scrape_metric_sums(self, eps, urls, bases):
        """Scrape vLLM /metrics for each pod and sum the requested metric bases.

        The vLLM metrics endpoint is derived from the sidecar URL by swapping the
        port to VLLM_METRICS_PORT (default 8200). Results are cached for
        PUSH_METRIC_TTL_S (default 1s). Returns {ep: {base: total|None}}.
        """
        now = time.time()
        ttl = float(os.getenv("PUSH_METRIC_TTL_S", "1.0"))
        key = tuple(bases)
        with self._lock:
            if (
                getattr(self, "_metric_cache_key", None) == key
                and (now - getattr(self, "_metric_cache_ts", 0.0)) < ttl
            ):
                return getattr(self, "_metric_cache", {})

        port = int(os.getenv("VLLM_METRICS_PORT", "8200"))

        async def one(ep: str):
            url = urls.get(ep)
            if not url:
                return ep, {b: None for b in bases}
            metrics_url = f"{url.rsplit(':', 1)[0]}:{port}/metrics"
            try:
                r = await self._health_client.get(metrics_url)
                if r.status_code != 200:
                    return ep, {b: None for b in bases}
                return ep, _parse_prom_sums(r.text, bases)
            except Exception:
                return ep, {b: None for b in bases}

        results = await asyncio.gather(*(one(ep) for ep in eps))
        out = {ep: sums for ep, sums in results}
        with self._lock:
            self._metric_cache = out
            self._metric_cache_ts = now
            self._metric_cache_key = key
        return out

    async def _pick_endpoint_throughput(self) -> Optional[str]:
        """Route to the pod that has processed the fewest total tokens.

        Score = vllm:prompt_tokens_total + vllm:generation_tokens_total scraped
        from each pod's vLLM /metrics, favoring underloaded pods. Falls back to
        round-robin when no pod exposes the counters.
        """
        with self._lock:
            eps = list(self._eps)
            urls = dict(self._urls)
        if not eps:
            return None
        bases = ["vllm:prompt_tokens_total", "vllm:generation_tokens_total"]
        metrics = await self._scrape_metric_sums(eps, urls, bases)
        scores = {ep: _total_tokens_from_sums(sums) for ep, sums in metrics.items()}
        best = _pick_min_score(scores)
        if best is None:
            _log_req("Throughput: no token counters, falling back to RR", level="full")
            return self._pick_endpoint_rr()
        _log_req(f"Throughput pick → {best} (tokens={scores.get(best)})", level="full")
        return best

    async def _health_load(self, url: Optional[str]) -> Optional[int]:
        """Fetch a single sidecar's load score (logical/queue_len) from /health."""
        if not url:
            return None
        try:
            r = await self._health_client.get(f"{url}/health")
            if r.status_code != 200:
                return None
            data = r.json()
            return int(data.get("logical", data.get("queue_len", 0)))
        except Exception:
            return None

    async def _pick_endpoint_p2c(self) -> Optional[str]:
        """Power-of-two-choices: sample two pods, route to the less loaded one.

        Cheaper than full least-queue (probes only 2 pods, not the fleet) while
        avoiding the worst-case pile-ups of pure random. Load comes from local
        logical inflight when PUSH_LEASTQ_MODE=local, else each candidate's
        /health.
        """
        with self._lock:
            eps = list(self._eps)
            urls = dict(self._urls)
            inflight = dict(self._logical_inflight)

        if not eps:
            return None
        if len(eps) == 1:
            return eps[0]

        a, b = random.sample(eps, 2)
        if self._leastq_mode == "local":
            ep = _pick_lower_load(a, inflight.get(a, 0), b, inflight.get(b, 0))
        else:
            sa, sb = await asyncio.gather(
                self._health_load(urls.get(a)), self._health_load(urls.get(b))
            )
            ep = _pick_lower_load(a, sa, b, sb)

        _log_req(f"P2C pick → {ep} (candidates={a},{b})", level="full")
        return ep

    async def _fetch_health_loads(self, eps, urls) -> Dict[str, float]:
        """Fetch each sidecar's decode-load proxy (logical/queue_len) from /health.

        Missing/failed probes map to 0.0 (optimistic) so a pod is still eligible.
        """
        async def one(ep: str):
            url = urls.get(ep)
            if not url:
                return ep, 0.0
            try:
                r = await self._health_client.get(f"{url}/health")
                if r.status_code != 200:
                    return ep, 0.0
                data = r.json()
                return ep, float(data.get("logical", data.get("queue_len", 0)))
            except Exception:
                return ep, 0.0

        results = await asyncio.gather(*(one(ep) for ep in eps))
        return {ep: v for ep, v in results}

    async def _pick_endpoint_kv_cost(self, req_id: Optional[str]) -> Optional[str]:
        """KV-aware cost routing (`push-kv-cost`).

        Scores each pod by a single tunable cost that trades cached-prefix reuse
        against decode load: cost = prefill_load_scale * max(prefill_blocks -
        overlap_credit * cached_prefix, 0) + decode_load. Picks the min-cost pod
        (or softmax-samples when ROUTER_TEMPERATURE > 0). Degrades to load-based
        selection when KV awareness is off (prefill_blocks = 0).
        """
        with self._lock:
            eps = list(self._eps)
            urls = dict(self._urls)
            inflight = dict(self._logical_inflight)
        if not eps:
            return None
        if len(eps) == 1:
            return eps[0]

        if self._leastq_mode == "local":
            loads = {ep: float(inflight.get(ep, 0)) for ep in eps}
        else:
            loads = await self._fetch_health_loads(eps, urls)

        prefill_blocks = len(get_request_blocks(req_id)) if req_id else 0
        overlap_credit = float(getattr(_cfg, "ROUTER_KV_OVERLAP_CREDIT", 1.0))
        prefill_scale = float(getattr(_cfg, "ROUTER_PREFILL_LOAD_SCALE", 1.0))
        temperature = float(getattr(_cfg, "ROUTER_TEMPERATURE", 0.0))

        costs: Dict[str, float] = {}
        for ep in eps:
            hits = prefix_len(ep, req_id) if req_id else 0
            costs[ep] = _kv_cost(prefill_blocks, hits, loads.get(ep, 0.0),
                                 overlap_credit, prefill_scale)

        ep = _select_by_cost(costs, temperature)
        _log_req(f"KVCost pick → {ep} (cost={costs.get(ep) if ep else None})", level="full")
        return ep

    async def _fetch_health_field(self, eps, urls, field: str) -> Dict[str, Optional[float]]:
        """Fetch a numeric field from every sidecar's /health concurrently."""
        async def one(ep: str):
            url = urls.get(ep)
            if not url:
                return ep, None
            try:
                r = await self._health_client.get(f"{url}/health")
                if r.status_code != 200:
                    return ep, None
                v = r.json().get(field)
                return ep, (float(v) if v is not None else None)
            except Exception:
                return ep, None

        results = await asyncio.gather(*(one(ep) for ep in eps))
        return {ep: s for ep, s in results}

    async def _pick_endpoint_least_kv(self) -> Optional[str]:
        """Route to the pod with the lowest KV-cache occupancy.

        Uses the sidecar-reported ``kv_usage`` (vLLM ``kv_cache_usage_perc``,
        falling back to ``gpu_cache_usage_perc``) exposed on /health — so it
        requires ``sidecar.kvUsageReport.enabled``. Covers both the
        ``least-kv-cache`` and ``least-gpu-cache`` names (same underlying signal
        here). Falls back to round-robin if no pod reports kv_usage.
        """
        with self._lock:
            eps = list(self._eps)
            urls = dict(self._urls)
        if not eps:
            return None
        scores = await self._fetch_health_field(eps, urls, "kv_usage")
        best = _pick_min_score(scores)
        if best is None:
            _log_req("LeastKV: no kv_usage reported, falling back to RR", level="full")
            return self._pick_endpoint_rr()
        _log_req(f"LeastKV pick → {best} (kv_usage={scores.get(best)})", level="full")
        return best

    async def _pick_endpoint_least_latency(self) -> Optional[str]:
        """Route to the pod with the lowest average end-to-end request latency.

        Score = vllm:e2e_request_latency_seconds_sum / _count scraped from each
        pod's vLLM /metrics (a cumulative average). Idle pods score 0.0; falls
        back to round-robin when no pod exposes the metric.
        """
        with self._lock:
            eps = list(self._eps)
            urls = dict(self._urls)
        if not eps:
            return None
        bases = [
            "vllm:e2e_request_latency_seconds_sum",
            "vllm:e2e_request_latency_seconds_count",
        ]
        metrics = await self._scrape_metric_sums(eps, urls, bases)
        scores = {ep: _avg_latency_from_sums(sums) for ep, sums in metrics.items()}
        best = _pick_min_score(scores)
        if best is None:
            _log_req("LeastLatency: no metric, falling back to RR", level="full")
            return self._pick_endpoint_rr()
        _log_req(f"LeastLatency pick → {best} (avg_s={scores.get(best)})", level="full")
        return best


    async def _pick_endpoint(self, req_id: Optional[str] = None) -> Optional[str]:
        if self.mode == "push-rr":
            return self._pick_endpoint_rr()
        if self.mode == "push-random":
            return self._pick_endpoint_random()
        if self.mode == "push-leastq":
            return await self._pick_endpoint_leastq()
        if self.mode == "push-throughput":
            return await self._pick_endpoint_throughput()
        if self.mode == "push-p2c":
            return await self._pick_endpoint_p2c()
        if self.mode == "push-kv-cost":
            return await self._pick_endpoint_kv_cost(req_id)
        if self.mode == "push-least-kv":
            return await self._pick_endpoint_least_kv()
        if self.mode == "push-least-latency":
            return await self._pick_endpoint_least_latency()
        return self._pick_endpoint_rr()

    # ---------------------------------------------------------
    # Push operation (trace added)
    # ---------------------------------------------------------

    async def route_and_push(self, req_id: str, prompt: str, meta: dict) -> None:
        """
        Dispatch a request to a sidecar in PUSH mode.
        Injects trace info into meta["__trace__"] if TRACE_ENABLED.

        Concurrency notes:
          - called by background dispatch workers concurrently
          - protects shared endpoint lists and leastq-local counters
        """
        # Ensure we have an endpoint snapshot
        self._ensure_endpoints()

        # We'll retry once after forcing endpoint refresh (handles pod churn / stale IPs)
        last_err: Optional[Exception] = None
        for attempt in (0, 1):
            if attempt == 1:
                with self._lock:
                    self._refresh_endpoints_locked(force=True)

            with self._lock:
                if not self._eps:
                    raise RuntimeError("No endpoints available for push routing")

            ep = await self._pick_endpoint(req_id)
            if not ep:
                raise RuntimeError("Failed to pick endpoint")

            with self._lock:
                url = self._urls.get(ep)

            if not url:
                last_err = RuntimeError(f"No sidecar URL for endpoint {ep}")
                continue

            # Prom: outgoing dispatch (router -> sidecar)
            inc_dispatch(ep)

            # ---------------------------------------------------------
            # Logical queue length (for leastq-local)
            # ---------------------------------------------------------
            logical_before: Optional[int] = None
            if self.mode == "push-leastq" and self._leastq_mode == "local":
                with self._lock:
                    logical_before = int(self._logical_inflight.get(ep, 0))
                    self._logical_inflight[ep] = logical_before + 1

            dispatch_ts = time.time()

            # Capture routing decision per request (independent of TRACE) so the
            # /latency_log ring can be enriched at completion time.
            _blocks = get_request_blocks(req_id)
            # prefix_len() returns 0 when no blocks were registered (measurement
            # off), so this reports real hit counts whenever blocks exist —
            # including affinity-only / none runs with ROUTER_MEASURE_PREFIX on —
            # without coupling the measurement to the KV routing decision.
            record_routing(
                req_id,
                endpoint=ep,
                kv_hits_len=prefix_len(ep, req_id),
                total_blocks=len(_blocks),
                affinity_key=(meta or {}).get("__affinity_key__"),
                block_hashes=_blocks if getattr(_cfg, "ROUTER_LOG_BLOCK_HASHES", False) else None,
            )

            # ---------------------------------------------------------
            # Inject trace into meta["__trace__"]
            # ---------------------------------------------------------
            if getattr(_cfg, "TRACE_ENABLED", False):
                meta = dict(meta or {})
                tr = dict(meta.get("__trace__") or {})

                tr.setdefault("endpoint", ep)
                tr.setdefault("router_mode", self.mode)
                tr["t_dispatch_router"] = dispatch_ts

                if _cfg.KV_AWARE:
                    tr["kv_block_hashes"] = get_request_blocks(req_id)

                if logical_before is not None:
                    tr["router_logical_inflight_before"] = logical_before
                    tr["router_logical_inflight_after"] = logical_before + 1

                meta["__trace__"] = tr

            payload = {
                "req_id": req_id,
                "prompt": str(prompt),
                "meta": meta or {},
            }

            # Helps leastq-local debugging (optional field)
            if self._leastq_mode == "local":
                payload["endpoint"] = ep

            _log_req(f"push req_id={req_id} → {ep} ({url})", level="summary")

            try:
                r = await self._push_client.post(f"{url}/push", json=payload)
            except Exception as e:
                last_err = e
                _log_req(f"push failed for {ep}: {e}", level="full")
                if self.mode == "push-leastq" and self._leastq_mode == "local":
                    with self._lock:
                        if self._logical_inflight[ep] > 0:
                            self._logical_inflight[ep] -= 1
                continue

            if r.status_code != 200:
                last_err = RuntimeError(f"push to {ep} failed: {r.status_code} {r.text}")
                _log_req(f"push to {ep} failed: {r.status_code} {r.text}", level="full")
                if self.mode == "push-leastq" and self._leastq_mode == "local":
                    with self._lock:
                        if self._logical_inflight[ep] > 0:
                            self._logical_inflight[ep] -= 1
                continue

            # success
            return

        # if we got here, both attempts failed
        if last_err is not None:
            raise last_err
        raise RuntimeError("push failed")

    # ---------------------------------------------------------
    # Central-push delivery (router-driven; endpoint chosen by the scheduler)
    # ---------------------------------------------------------

    def endpoints_snapshot(self) -> List[str]:
        """Return the current discovered pod names (refreshing if stale).

        Used by the central-push dispatcher to iterate delivery targets.
        """
        self._ensure_endpoints()
        with self._lock:
            return list(self._eps)

    async def refresh_kv_usage_from_health(self, router_state) -> None:
        """GET each sidecar /health and store optional kv_usage on router_state.

        Used by central-push (sidecars do not /pull). Best-effort: failures skip.
        """
        self._ensure_endpoints()
        with self._lock:
            items = list(self._urls.items())
        if not items:
            return

        async def one(ep: str, url: str):
            try:
                r = await self._health_client.get(f"{url}/health")
                # 503 still can carry kv_usage when vLLM is unhealthy; parse body.
                data = r.json() if r.content else {}
                kv = data.get("kv_usage", None)
                if kv is not None:
                    router_state.record_kv_usage(ep, kv)
            except Exception:
                return

        await asyncio.gather(*(one(ep, url) for ep, url in items), return_exceptions=True)

    async def push_to_endpoint(self, endpoint: str, req_id: str, prompt: str, meta: dict) -> None:
        """Deliver a single pre-selected request to a specific sidecar via
        POST {url}/push. Unlike route_and_push(), the target endpoint is chosen
        by the central scheduler (pull_for_endpoint), so no _pick_endpoint().

        Raises on failure so the caller can requeue + decrement in-flight.
        """
        with self._lock:
            url = self._urls.get(endpoint)
        if not url:
            # Endpoint may be stale; force a refresh once and retry lookup.
            with self._lock:
                self._refresh_endpoints_locked(force=True)
                url = self._urls.get(endpoint)
        if not url:
            raise RuntimeError(f"No sidecar URL for endpoint {endpoint}")

        # Prom: outgoing dispatch (router -> sidecar)
        inc_dispatch(endpoint)

        dispatch_ts = time.time()

        # Capture routing decision (independent of TRACE) for /latency_log.
        _blocks = get_request_blocks(req_id)
        record_routing(
            req_id,
            endpoint=endpoint,
            kv_hits_len=prefix_len(endpoint, req_id),
            total_blocks=len(_blocks),
            affinity_key=(meta or {}).get("__affinity_key__"),
            block_hashes=_blocks if getattr(_cfg, "ROUTER_LOG_BLOCK_HASHES", False) else None,
        )

        if getattr(_cfg, "TRACE_ENABLED", False):
            meta = dict(meta or {})
            tr = dict(meta.get("__trace__") or {})
            tr.setdefault("endpoint", endpoint)
            tr.setdefault("router_mode", self.mode)
            tr["t_dispatch_router"] = dispatch_ts
            if _cfg.KV_AWARE:
                tr["kv_block_hashes"] = get_request_blocks(req_id)
            meta["__trace__"] = tr

        payload = {
            "req_id": req_id,
            "prompt": str(prompt),
            "meta": meta or {},
            # Helps the sidecar/back-channel attribute results to this endpoint.
            "endpoint": endpoint,
        }

        _log_req(f"central-push req_id={req_id} → {endpoint} ({url})", level="summary")

        r = await self._push_client.post(f"{url}/push", json=payload)
        if r.status_code != 200:
            raise RuntimeError(f"push to {endpoint} failed: {r.status_code} {r.text}")

    # ---------------------------------------------------------
    # Result notification (for local leastq mode)
    # ---------------------------------------------------------

    def notify_result(self, endpoint: Optional[str]) -> None:
        if not endpoint:
            return
        with self._lock:
            if endpoint not in self._eps:
                return
            current = self._logical_inflight.get(endpoint, 0)
            if current > 0:
                self._logical_inflight[endpoint] = current - 1
