
# Vanilla prefill/decode disaggregation proxy.
#
# Two-phase HTTP flow per request:
#   1) send the request to a PREFILL instance with max_tokens=1 so it only
#      computes+publishes the prompt KV (no real decoding);
#   2) send the original request to a DECODE instance, which reuses that KV
#      (pulled over the configured KV connector) and streams the output.
#
# Connector-agnostic: if the prefill response carries `kv_transfer_params`
# (NIXL-style connectors), they are forwarded to decode; otherwise decode
# relies on the shared KV store (Mooncake/LMCache). If prefill fails, the
# proxy falls back to decode-only (correct, just not disaggregated).
#
# Only depends on aiohttp (ships with the vLLM image).
import asyncio
import itertools
import json
import logging
import os
import sys
import time
import uuid
from collections import deque
from aiohttp import web, ClientSession, ClientTimeout

LOG_LEVEL = os.environ.get("LOG_LEVEL", "info").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [pd-proxy] %(message)s",
)
log = logging.getLogger("pd-proxy")

def _urls(env):
    raw = os.environ.get(env, "") or ""
    return [u.strip().rstrip("/") for u in raw.split(",") if u.strip()]

PREFILL = _urls("PREFILL_BASE")
DECODE = _urls("DECODE_BASE")
PORT = int(os.environ.get("PROXY_PORT", "8200"))
PD_PATHS = ("/v1/chat/completions", "/v1/completions")
DRAIN_MAX_WAIT = float(os.environ.get("PROXY_DRAIN_MAX_WAIT_SECONDS", "300"))
# Response headers worth relaying back to the client. Kept as a small
# allow-list on purpose: hop-by-hop headers (Content-Length, Content-Encoding,
# Connection) must not be copied because aiohttp re-encodes the stream itself.
_RELAY_HEADERS = (
    "X-Request-Id",
    "Retry-After",
    "RateLimit-Limit",
    "RateLimit-Remaining",
    "RateLimit-Reset",
    "X-RateLimit-Limit",
    "X-RateLimit-Remaining",
    "X-RateLimit-Reset",
)


class DrainTimeout(Exception):
    """A P/D request waited too long for the proxy to resume from a drain."""


_drain_state = {"paused": False}
_drain_cond = asyncio.Condition()


class _Metrics:
    """In-process Prometheus text-format metrics for the P/D proxy."""

    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.values = {
            "prefill_inflight": 0.0,
            "prefill_requests_total": 0.0,
            "prefill_prompt_tokens_total": 0.0,
            "decode_inflight": 0.0,
            "decode_requests_total": 0.0,
        }
        # Sliding window of prompt tokens from successful prefill responses.
        # The mean tracks recent workload instead of the lifetime average, so a
        # prompt-size mix change is reflected quickly and a proxy restart does
        # not leave the planner with a stale mean.
        self.prompt_tokens_window = deque(maxlen=200)

    async def inc(self, key: str, delta: float = 1.0) -> None:
        async with self.lock:
            self.values[key] += delta

    async def record_prefill(self, prompt_tokens: float) -> None:
        async with self.lock:
            self.values["prefill_requests_total"] += 1.0
            self.values["prefill_prompt_tokens_total"] += prompt_tokens
            self.prompt_tokens_window.append(prompt_tokens)

    async def record_decode(self) -> None:
        async with self.lock:
            self.values["decode_requests_total"] += 1.0

    async def snapshot(self) -> dict[str, float]:
        async with self.lock:
            values = dict(self.values)
            window = self.prompt_tokens_window
            values["prefill_mean_prompt_tokens"] = (
                sum(window) / len(window) if window else 0.0
            )
            return values

_metrics = _Metrics()

_prefill_rr = itertools.cycle(PREFILL) if PREFILL else None
_decode_rr = itertools.cycle(DECODE) if DECODE else None
_rr_lock = asyncio.Lock()

# Bases that recently failed (connection error or 5xx) are usually pods being
# terminated during a scale-down. Remember them briefly so round-robin picks
# and retries avoid them; the TTL is short so a recovered pod is not skipped
# forever and the dict stays bounded.
_FAILURE_TTL_SECONDS = 10.0
_MAX_FAILED_BASES = 64
_failures: dict[str, float] = {}
_failures_lock = asyncio.Lock()


async def _pick(rr, bases, skip=frozenset()):
    """Next round-robin base, avoiding known-failed bases when possible."""
    async with _rr_lock:
        for _ in bases:
            base = next(rr)
            if base not in skip:
                return base
        # Every candidate is failed/just tried: fall back to the plain next
        # pick (e.g. a single-URL Service that should still be retried).
        return next(rr)


async def _remember_failure(base: str) -> None:
    async with _failures_lock:
        now = time.monotonic()
        _failures[base] = now
        if len(_failures) > _MAX_FAILED_BASES:
            for b, ts in list(_failures.items()):
                if now - ts >= _FAILURE_TTL_SECONDS:
                    del _failures[b]
            if len(_failures) > _MAX_FAILED_BASES:
                oldest = min(_failures, key=_failures.get)
                del _failures[oldest]


async def _recent_failures() -> frozenset[str]:
    async with _failures_lock:
        now = time.monotonic()
        expired = [
            b for b, ts in _failures.items()
            if now - ts >= _FAILURE_TTL_SECONDS
        ]
        for b in expired:
            del _failures[b]
        return frozenset(_failures)


async def _cycle_candidates(rr, bases, skip=frozenset(), limit=None):
    """Yield distinct round-robin candidates, skipping `skip` (best effort).

    Advances the shared cycle under the lock and consumes at most one full
    revolution. The lock is released before yielding so a slow upstream call
    never blocks other requests' round-robin picks. If every candidate is
    skipped (e.g. a single-URL Service that just failed), yields the next pick
    anyway so a retry still happens.
    """
    seen = set()
    picked = []
    total = len(set(bases))
    async with _rr_lock:
        while len(seen) < total and (limit is None or len(picked) < limit):
            base = next(rr)
            if base in seen:
                continue
            seen.add(base)
            if base in skip:
                continue
            picked.append(base)
        if not picked and bases:
            picked.append(next(rr))
    for base in picked:
        yield base

async def _get_session(app):
    return app["session"]

async def health(request):
    return web.Response(text="ok")

async def metrics(request):
    values = await _metrics.snapshot()
    lines = [
        "# HELP pd_proxy_prefill_inflight Requests currently waiting on the prefill engine (phase 1).",
        "# TYPE pd_proxy_prefill_inflight gauge",
        f"pd_proxy_prefill_inflight {values['prefill_inflight']:.0f}",
        "# HELP pd_proxy_prefill_requests_total Requests that completed the prefill phase successfully.",
        "# TYPE pd_proxy_prefill_requests_total counter",
        f"pd_proxy_prefill_requests_total {values['prefill_requests_total']:.0f}",
        "# HELP pd_proxy_prefill_prompt_tokens_total Prompt tokens observed in successful prefill responses.",
        "# TYPE pd_proxy_prefill_prompt_tokens_total counter",
        f"pd_proxy_prefill_prompt_tokens_total {values['prefill_prompt_tokens_total']:.0f}",
        "# HELP pd_proxy_prefill_mean_prompt_tokens Sliding-window mean prompt tokens of successful prefill responses.",
        "# TYPE pd_proxy_prefill_mean_prompt_tokens gauge",
        f"pd_proxy_prefill_mean_prompt_tokens {values['prefill_mean_prompt_tokens']:.3f}",
        "# HELP pd_proxy_decode_inflight Requests currently streaming from the decode engine (phase 2).",
        "# TYPE pd_proxy_decode_inflight gauge",
        f"pd_proxy_decode_inflight {values['decode_inflight']:.0f}",
        "# HELP pd_proxy_decode_requests_total Requests that completed the decode phase.",
        "# TYPE pd_proxy_decode_requests_total counter",
        f"pd_proxy_decode_requests_total {values['decode_requests_total']:.0f}",
    ]
    return web.Response(
        text="\n".join(lines) + "\n",
        content_type="text/plain; version=0.0.4",
    )


async def status(request):
    """JSON snapshot used by the rebalancer for the drain handshake."""
    values = await _metrics.snapshot()
    async with _drain_cond:
        paused = _drain_state["paused"]
    return web.json_response(
        {
            "paused": paused,
            "prefill_inflight": values["prefill_inflight"],
            "decode_inflight": values["decode_inflight"],
            "prefill_requests_total": values["prefill_requests_total"],
            "decode_requests_total": values["decode_requests_total"],
            "prefill_mean_prompt_tokens": values["prefill_mean_prompt_tokens"],
        }
    )


async def drain(request):
    """Pause (or resume) new P/D requests while the rebalancer transitions."""
    try:
        payload = await request.json()
        enabled = bool(payload.get("enabled"))
    except Exception:  # noqa: BLE001
        return web.json_response(
            {"error": "drain payload must be {\"enabled\": bool}"}, status=400
        )
    async with _drain_cond:
        _drain_state["paused"] = enabled
        _drain_cond.notify_all()
    log.info("drain enabled=%s", enabled)
    return web.json_response({"paused": enabled})


async def _wait_for_drain():
    """Block a new P/D request until the drain window ends (or timeout)."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + DRAIN_MAX_WAIT
    async with _drain_cond:
        while _drain_state["paused"]:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise DrainTimeout()
            await asyncio.wait_for(_drain_cond.wait(), timeout=remaining)


async def _relay(request, upstream):
    """Stream an aiohttp upstream response back to the client verbatim."""
    resp = web.StreamResponse(status=upstream.status)
    resp.headers["Content-Type"] = upstream.headers.get(
        "Content-Type", "application/json"
    )
    # Copy the allow-list so tracing/rate-limit signals survive the relay
    # without leaking hop-by-hop headers that aiohttp manages.
    for name in _RELAY_HEADERS:
        value = upstream.headers.get(name)
        if value is not None:
            resp.headers[name] = value
    await resp.prepare(request)
    async for chunk in upstream.content.iter_any():
        await resp.write(chunk)
    await resp.write_eof()
    return resp

async def _passthrough(request, base):
    """Forward a non-P/D request (e.g. GET /v1/models) to a decode pod."""
    session = await _get_session(request.app)
    url = f"{base}{request.rel_url}"
    data = await request.read()
    headers = {
        k: v
        for k, v in request.headers.items()
        if k.lower() not in ("host", "content-length")
    }
    try:
        upstream = await session.request(
            request.method, url, data=data or None, headers=headers
        )
    except Exception as e:  # noqa: BLE001
        return web.json_response({"error": f"upstream error: {e}"}, status=502)
    return await _relay(request, upstream)

async def handle(request):
    path = request.path
    if request.method != "POST" or path not in PD_PATHS:
        failed = await _recent_failures()
        return await _passthrough(
            request, await _pick(_decode_rr, DECODE, skip=failed)
        )
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        failed = await _recent_failures()
        return await _passthrough(
            request, await _pick(_decode_rr, DECODE, skip=failed)
        )
    try:
        await _wait_for_drain()
    except DrainTimeout:
        return web.json_response(
            {"error": "pd-proxy is draining; retry later"}, status=503
        )

    req_id = request.headers.get("X-Request-Id") or f"pd-{uuid.uuid4().hex}"
    failed = await _recent_failures()
    prefill_base = await _pick(_prefill_rr, PREFILL, skip=failed)
    decode_base = await _pick(_decode_rr, DECODE, skip=failed)
    session = await _get_session(request.app)

    # --- Phase 1: prefill (max_tokens=1) ---
    prefill_body = dict(body)
    prefill_body["max_tokens"] = 1
    if "max_completion_tokens" in prefill_body:
        prefill_body["max_completion_tokens"] = 1
    # min_tokens > 1 contradicts max_tokens=1 (vLLM rejects with 400), which
    # would silently degrade the request to decode-only. Prefill must only
    # compute+publish KV, so drop the decode-side floor.
    prefill_body["min_tokens"] = 0
    prefill_body["stream"] = False
    prefill_body.pop("stream_options", None)
    kv_params = None

    async def _prefill_once(base):
        """Return (status, body) or (None, exception) so callers can retry."""
        try:
            async with session.post(
                f"{base}{path}",
                json=prefill_body,
                headers={"X-Request-Id": req_id},
            ) as r:
                text = await r.text()
                return r.status, text
        except Exception as e:  # noqa: BLE001
            return None, e

    async def _prefill_candidates():
        """At most two distinct bases: the picked one, then one more on retry."""
        yield prefill_base
        async for base in _cycle_candidates(
            _prefill_rr, PREFILL, skip=failed | {prefill_base}, limit=1
        ):
            yield base

    await _metrics.inc("prefill_inflight", 1.0)
    try:
        # Retry prefill once on connection errors / 5xx so a terminating
        # prefill pod does not silently degrade the request to decode-only.
        # The retry picks a different base and skips recently-failed ones, so
        # it does not just resend to the same pod that is draining. Client
        # errors (4xx) are not retried.
        prefill_status = None
        prefill_text = ""
        prefill_candidates = [b async for b in _prefill_candidates()]
        for attempt, base in enumerate(prefill_candidates, start=1):
            prefill_status, prefill_text = await _prefill_once(base)
            if isinstance(prefill_status, int) and prefill_status // 100 == 2:
                break
            if isinstance(prefill_status, int) and 400 <= prefill_status < 500:
                break
            # Connection error or 5xx: the pod may be terminating, so remember
            # it and let the retry (and later requests) avoid it.
            await _remember_failure(base)
            if attempt < len(prefill_candidates):
                log.warning(
                    "prefill attempt %s failed on %s id=%s status=%s; retrying once",
                    attempt, base, req_id, prefill_status,
                )
                await asyncio.sleep(0.2)
        if isinstance(prefill_status, int) and prefill_status // 100 == 2:
            try:
                data = json.loads(prefill_text)
                kv_params = data.get("kv_transfer_params")
                usage = data.get("usage") or {}
                await _metrics.record_prefill(
                    float(usage.get("prompt_tokens") or 0.0),
                )
            except Exception:  # noqa: BLE001
                kv_params = None
        else:
            log.warning(
                "prefill non-2xx status=%s id=%s body=%.200s (decode-only fallback)",
                prefill_status, req_id, prefill_text,
            )
    finally:
        await _metrics.inc("prefill_inflight", -1.0)

    # --- Phase 2: decode (original request), streamed back ---
    decode_body = dict(body)
    if kv_params is not None:
        decode_body["kv_transfer_params"] = kv_params

    async def _post_decode(base: str):
        try:
            upstream = await session.post(
                f"{base}{path}",
                json=decode_body,
                headers={"X-Request-Id": req_id},
            )
        except Exception as e:  # noqa: BLE001
            return None, e
        if upstream.status >= 500:
            upstream.close()
            return None, RuntimeError(f"decode HTTP {upstream.status}")
        return upstream, None

    async def _decode_candidates():
        """The picked base first, then other distinct bases, skipping bad ones."""
        yield decode_base
        async for base in _cycle_candidates(
            _decode_rr, DECODE, skip=failed | {decode_base}
        ):
            yield base

    # A decode pod can die mid-scale-down; retry on another instance so a
    # request is not lost just because the first pod was being terminated.
    # Each base is tried at most once and recently-failed bases are skipped,
    # so if several decode pods are draining at once the retry still lands on
    # a surviving instance instead of blindly hitting the next dying pod.
    # (A worker-side drain does not cover an already established connection
    # that gets cut, so the proxy retry is the safety net.)
    await _metrics.inc("decode_inflight", 1.0)
    try:
        upstream = None
        err = None
        attempt = 0
        async for base in _decode_candidates():
            attempt += 1
            upstream, err = await _post_decode(base)
            if upstream is not None:
                break
            log.warning(
                "decode attempt %s failed on %s id=%s err=%s",
                attempt, base, req_id, err,
            )
            await _remember_failure(base)
        if upstream is None:
            return web.json_response(
                {"error": f"decode error after retry: {err}"}, status=502
            )
        await _metrics.record_decode()
        return await _relay(request, upstream)
    finally:
        await _metrics.inc("decode_inflight", -1.0)

async def _on_startup(app):
    app["session"] = ClientSession(timeout=ClientTimeout(total=None))

async def _on_cleanup(app):
    await app["session"].close()

def main():
    if not PREFILL or not DECODE:
        log.error("PREFILL_BASE and DECODE_BASE must both be set")
        sys.exit(1)
    app = web.Application(client_max_size=1024 ** 3)
    app.on_startup.append(_on_startup)
    app.on_cleanup.append(_on_cleanup)
    app.router.add_get("/health", health)
    app.router.add_get("/metrics", metrics)
    app.router.add_get("/status", status)
    app.router.add_post("/drain", drain)
    app.router.add_route("*", "/{tail:.*}", handle)
    log.info("pd-proxy on :%d prefill=%s decode=%s", PORT, PREFILL, DECODE)
    web.run_app(app, host="0.0.0.0", port=PORT, access_log=None)

if __name__ == "__main__":
    main()
