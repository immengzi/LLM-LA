# Sidecar Service

The sidecar runs next to each inference-engine pod (vLLM by default, or the
pinned SGLang profile) and turns that pod into a well-behaved
worker in the cluster. It handles local queuing, talks to the router-service
and the local engine, and keeps track of the pod’s KV-cache blocks.

---

## Role in the System

- Receives work from the router and feeds it to the pod’s engine endpoint.
- Limits how many requests run on the pod at once.
- Reports completions back to the router so client calls can finish.
- Listens to KV events from the engine and records which KV blocks live on this pod.

This lets the router focus on global decisions while each sidecar manages its
own pod.

---

## Local Queue and Capacity

Each sidecar keeps a small in-memory queue holding `(req_id, prompt, meta)`.

- It tracks two counters: pending items in the queue and inflight items being
  processed by workers.
- The pull capacity is `BATCH_SIZE + PREFETCH` — the maximum of
  `pending + inflight` the sidecar will hold before it stops pulling.
- When the limit is reached, the sidecar temporarily stops asking the router
  for more work.

This ensures each pod only runs as many vLLM requests as it can handle.

---

## Getting Work from the Router

A pull helper checks the local queue state and, when there is spare capacity,
asks the router for more items with a single `/pull` call that includes:

- the pod identity (used as the endpoint name), and
- how many new requests it can accept.

The helper is triggered by events:

- after each completed request to immediately top up, and
- occasionally while idle to see if new work is available.

There is no tight polling loop; traffic is proportional to actual activity.

---

## KV-Memory Pull Gate

An **off-by-default** pull-side memory guard. When the pod's local vLLM GPU KV
cache fill fraction (`kv_usage`, from `vllm:kv_cache_usage_perc`) is high, the
sidecar shrinks or stops pulling so that long-decode / long-sequence work does
not drive the pod into KV preemption (recompute) or OOM.

After the static window computes how many items to request
(`want = (BATCH_SIZE + PREFETCH) − (pending + inflight)`), the gate scales it:

- `kv_usage ≥ KV_PULL_GATE_HIGH` (default `0.90`) → `want := 0` (stop pulling)
- `kv_usage ≤ KV_PULL_GATE_LOW` (default `0.70`) → `want` unchanged
- in between → linear taper of `want` toward 0

It only ever *reduces* `want`, so it composes with the static cap and the
SLO/TPOT AIMD backpressure controller (whichever is smallest wins). It needs
`KV_USAGE_REPORT=true` for samples; with no sample it **fails open** (never
blocks) and logs a one-time startup warning. The `[LOW, HIGH]` band gives
hysteresis so pulling does not thrash near the threshold.

Enable with `KV_PULL_GATE_ENABLED=true` (`KV_PULL_GATE_HIGH` / `KV_PULL_GATE_LOW`
tune the band; normalized to `0 ≤ LOW ≤ HIGH ≤ 1`). When enabled it exports
`sidecar_kv_pull_gate_scale` (the multiplier applied this tick, `1`=no throttle,
`0`=blocked) and `sidecar_kv_pull_gate_kv_usage` (the fill fraction it acted on);
these gauges are registered only when the gate is on, so a disabled sidecar's
`/metrics` is byte-for-byte unchanged.

This is a local **memory** guard, distinct from the router-side soft KV divert,
which instead reorders *which* requests a saturated pod keeps (see
[router.md](router.md) §5d). The same `kv_usage` sample the sidecar reports on
`/pull` and `/health` feeds both. Applies only to pull-mode sidecars; see the
[Routing Compatibility Matrix](router.md#routing-compatibility-matrix).

---

## Talking to vLLM and Returning Results

Worker threads repeatedly:

1. Take a request from the local queue.
2. Call the pod’s vLLM `/v1/chat/completions` endpoint using the stored
   prompt and meta.
3. Extract the generated text from the response.
4. Send the result back to the router via a `/result` call that includes the
   original `req_id`.

On any error, the sidecar logs the issue and still marks the request complete
in the local queue so the capacity accounting stays correct.

The number of workers is `BATCH_SIZE + PREFETCH`, giving that many
concurrent vLLM requests per pod.

---

## KV-Cache Awareness

A background subscriber connects to vLLM over ZMQ and receives KV-cache events
such as blocks being stored, removed, or fully cleared. For each event, it
updates Redis so that other components know:

- which block hashes belong to this pod, and
- which pods are associated with each block hash.

The router can then use this information to send requests to pods that already
hold matching KV blocks, enabling cache reuse.

---

## Sidecar HTTP API

The sidecar exposes a small internal API:

- `GET /health` — reports basic status, queue length, and inflight count.
- `POST /push` — optional way to inject work directly into the local queue
  (useful for testing or alternative routing modes).

---

## Startup and Shutdown

On startup the sidecar:

1. Loads configuration from environment variables.
2. Creates the local queue and binds it to the HTTP API.
3. Starts the pull helper (in pull mode).
4. Launches vLLM worker threads.
5. Starts the KV subscriber.
6. Runs the FastAPI server with Uvicorn.

On shutdown it stops workers, the pull helper, and the subscriber cleanly.

Together, this makes each vLLM pod a self-contained worker with clear capacity,
KV visibility, and a simple integration point for the central router-service.

# Tracing (Sidecar)

When `TRACE_ENABLED=true`, the sidecar records high-resolution timing for:

- arrival (pull or push mode)
- dequeue
- sending request to vLLM
- receiving response from vLLM
- sending result back to router

These fields are stored in:

    meta["__trace__"]

The sidecar forwards this object to the router’s `/result` endpoint, where the
router merges it with its own timestamps.
