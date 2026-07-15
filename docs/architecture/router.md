# KV-Aware Router Service

The router service is the **central coordinator** between:

- external clients (HTTP `/enqueue`), and
- model workers running behind sidecars.

It provides **synchronous request/response** to clients while internally
combining:

- KV-aware routing (prefix/block reuse),
- length-aware selection,
- pull-mode and push-mode dispatch.

---

## 1. What the Router Does

At a high level, the router:

1. Receives prompts from clients via `/enqueue`.
2. Assigns each request a unique `req_id`.
3. If KV-awareness is enabled:
   - computes KV block hashes for the prompt (inline via `prefix_hash.py` by
     default; the legacy external service is opt-in),
   - records which blocks the request will use.
4. Either:
   - **pull-mode**: puts the request in a central queue, or
   - **push-mode**: immediately pushes it to a chosen sidecar.
5. Waits for the sidecar to send back the result.
6. Returns the result to the client as the `/enqueue` response.

Clients only see a simple, blocking API; all routing and batching happens
inside the router.

---

## 2. Main Endpoints (Conceptual)

- `GET /health`
  Liveness check. The aggregated response nests queue length under `router.queue_len`; the per-component `GET /health/router` returns a top-level `queue_len`.

- `POST /enqueue` (client → router)
  Synchronous call:
  - input: `prompt` + optional `meta`, timestamps, etc.
  - output: `req_id` and the model `result` (text, finish reason, latency, …).

- `POST /pull` (sidecar → router; pull-mode)
  Sidecar asks for up to `want` jobs:
  - input: `endpoint` identity + capacity `want`
  - output: list of jobs (each with `req_id`, `prompt`, `meta`).

- `POST /result` (sidecar → router)
  Sidecar delivers model output:
  - router unblocks the waiting `/enqueue` call and returns the result
    to the client.

- `GET /latency_log?last=N` (observer → router)
  Returns the most recent completed requests from an in-memory ring buffer
  (server-side, so it survives a BooM/proxy hop). Each entry carries
  `rid`, `endpoint`, token counts, and latencies, plus the routing decision
  captured at dispatch: `kv_hits_len`, `total_blocks`, `matched_tokens`,
  `kv_hit`, and `affinity_key` (and the raw `block_hashes` list when
  `ROUTER_LOG_BLOCK_HASHES=true`). This is the source of truth for
  `router_logs.json` and the `logs.json` endpoint/KV enrichment described in
  [artifacts and analysis](../benchmarking/artifacts-and-analysis.md#router-request-log-collect_router_log).

---

## 3. Routing Modes

The router supports two main ways of distributing work to sidecars:

### Pull Mode (`ROUTER_MODE = "pull"`)

- All incoming requests go into a central queue.
- Each sidecar calls `/pull` when it has capacity (`want`).
- Router picks which requests to give to that sidecar based on:
  - KV-awareness (existing blocks),
  - length-awareness (predicted output length).

This is the default and is easy to reason about: workers pull work when ready.

### Push Modes (`"push-rr"`, `"push-random"`, `"push-leastq"`)

- Router uses Kubernetes discovery to find sidecars.
- For each request, it picks an endpoint and calls the sidecar’s `/push`.
- Different strategies:
  - **round-robin** – cycle over endpoints,
  - **random** – random endpoint,
  - **least-queue** – query sidecar health and choose the least loaded one.

Results still come back via `/result`; from the client’s viewpoint `/enqueue`
is the same.

### Central-Push Mode (`"central-push"`)

Central-push is a hybrid: **admit like pull, deliver like push.**

- Requests are admitted into the **same central queue** as pull mode, so KV
  awareness, conversation-key affinity, length awareness, SLO scheduling and
  pull-mode fairness all apply **unchanged**.
- But the **router**, not the sidecar, decides when and how much to dispatch.
  A background dispatcher runs periodically (`ROUTER_CENTRAL_PUSH_INTERVAL_S`)
  and is also *kicked* on every enqueue. On each pass it estimates each pod's
  free capacity as `ROUTER_CENTRAL_PUSH_CAP − in-flight`, asks the scheduler for
  exactly that many items (via the same code path `/pull` uses), and delivers
  them to the pod with `POST /push`.
- Sidecars run in **push mode** (no pull poller); they simply receive `/push`
  and post results via `/result`.

**Why central-push (vs. pull with capacity/fairness)?** Pull is worker-driven:
capacity balancing only happens *when a pod chooses to pull*, so a slow, warming
or wedged pod can under-pull and quietly distort the fleet-average signal, and
the router can only react to pulls it receives. Central-push moves the decision
to the router, which has a global, always-current view of in-flight counts. That
gives:

- **Deterministic, continuous rebalancing** – dispatch happens on a fixed tick
  and on every admission, independent of pod pull timing.
- **No pull round-trip / long-poll bookkeeping** on the hot path.
- **A single global control point** for capacity, which composes cleanly with
  fairness and SLO throttling (all still enforced inside the scheduler).

The trade-off is that the router must estimate capacity (`CAP − in-flight`)
rather than have each pod self-report via `want`; `ROUTER_CENTRAL_PUSH_CAP`
should therefore track the sidecar's `batchSize + prefetch`. The sidecar `/push`
handler also gates on vLLM readiness and local-queue capacity (returning `503`),
so a mis-estimate or a warming pod causes the item to be **requeued to the front
and retried on a later pass**, never dropped.

**KV / affinity compatibility.** Because central-push admits through the pull
path, block registration and owner lookup are identical to pull; the dispatcher
selects items with the same KV-aware / affinity-aware scheduler, so every KV
scenario (measure-only, KV-aware routing, soft/hard affinity) behaves exactly as
in pull mode.

**Rebalancing compatibility.** The capacity throttle and fairness throttle live
inside the scheduler, so they apply to central-push dispatch unchanged. The only
mode-specific detail is *stuck detection*: in pull mode a pod is "stuck" when it
stops pulling, but under central-push the router stamps the last-pull time every
tick, so stuck detection instead keys off the **last successful result** per pod
(`ROUTER_STUCK_PULL_SECONDS` / `ROUTER_AFFINITY_RELEASE_ON_STUCK` still apply).

**Streaming.** Streaming works unchanged: chunks are transported by `req_id`
(`/result_chunk`) and the sidecar posts a final `/result`, exactly as in pull
and push modes.

**Robustness.** In-flight release is idempotent and keyed by `req_id`: whichever
happens first — the `/result` callback or a client-timeout reconcile — performs
the single decrement, so a lost delivery (e.g. a pod dies after a `200`) can
never permanently consume a pod's capacity.

Results still come back via `/result`; from the client’s viewpoint `/enqueue`
is the same.

---

## 4. KV Awareness (Conceptual)

> For the four routing strategies (`none | prefix | affinity | both`) with
> figures, see [router-strategies.md](router-strategies.md).

KV-awareness is about **reusing model KV cache blocks** when possible.

The router keeps two maps:

- request → list of block hashes
- block hash → set of endpoints that own that block

Information comes from:

- **KV-block hasher** – computes block hashes for each new request; inline via
  `prefix_hash.py` by default (legacy external service is opt-in).
- **Block ownership** – by default (`KV_OWNER_SOURCE=lookup`) the router fetches a
  request's own block owners on demand at admit via a targeted Redis `HGETALL`
  (`owner_lookup.py`). The legacy **KV watcher** (`KV_OWNER_SOURCE=watcher`)
  instead periodically scans Redis (`scan_iter` over `{model}:kvblock:*`) into a
  shared owner map. Kubernetes is used only for pod discovery (which the watcher
  loop still performs), not for block ownership.

When a sidecar pulls work, the router prefers:

- requests whose prefix blocks are already on that sidecar’s endpoint,
- so that the worker can reuse KV state instead of recomputing from scratch.

If KV-awareness is disabled, all requests are treated the same.

---

## 5. Length Awareness (Conceptual)

Length-awareness tries to group or prioritize requests based on predicted
output length (using a simple predictor for now).

When a sidecar pulls `want` jobs, the router:

1. Looks at a small pool of candidates from the front of the queue.
2. Optionally reorders them by:
   - shorter first, or
   - longer first (configurable policy).

This helps shape batches and reduce tail latency without changing the external
API.

---

## 5b. Conversation Key Affinity (Conceptual)

Key affinity keeps all turns of one chat conversation on the same pod so the
engine's prefix cache is reused across turns. It is **off by default** and is
purely additive to KV- and length-awareness.

The router derives a stable per-conversation key from the conversation's opening
(`model + system prompt + first user message`) — which is identical on every
turn — and remembers which pod last served it. On a pull it either *prefers*
(soft mode) or *pins* (hard mode, time-bounded) that conversation to its pod.
It requires no client/BooM/sidecar changes.

See [router-strategies.md](router-strategies.md) for where affinity fits among
the four strategies, and [key-affinity.md](key-affinity.md) for the full
reference (modes, metrics, interactions, and source map).

---

## 5c. Pull-Mode Fairness (Conceptual)

Fairness is an **off-by-default, load-aware grant throttle** for pull mode. It
addresses pod-load imbalance without ever overriding KV/affinity decisions.

Because `/pull` returns immediately (no long-poll), the router cannot choose
between waiting pods; ordering also does not control balance (each puller still
takes its own `want`). The only affinity-safe lever is **how many items each
pull is granted**. When `ROUTER_FAIR_PULL` is on, on every pull the router:

- reads the fleet in-flight snapshot (the always-on `router_endpoint_inflight`
  counter) and computes `ceiling = ROUTER_FAIR_MARGIN x fleet-average`,
- grants an underloaded pod its full `want`, and trims a pod at/above the
  ceiling so it only fills the remaining gap (`ceiling - its_inflight`), never
  below `ROUTER_FAIR_FLOOR` movable items,
- trims **only the movable/unpinned tail** — self-pinned (affinity) items are
  always granted and moved to the front — so KV tiers and affinity pins are
  never overridden. Trimmed items requeue to the front for the next puller.

Interaction with strategies: full-strength under `none`/`prefix`/soft affinity;
under `hard`/`both` it only rebalances the unpinned overflow (imbalance caused
by hard pins is intentionally not overridden). A floor guarantees an overloaded
pod always makes progress, so a dead/non-pulling pod can never stall the queue.

Optional liveness: `ROUTER_STUCK_PULL_SECONDS` flags a pod that stopped pulling
while the queue is backed up (exposed as `router_endpoint_stuck` /
`router_endpoint_last_pull_seconds`); with `ROUTER_AFFINITY_RELEASE_ON_STUCK`,
a stuck pod's pins release to load balancing via the existing
unavailable-target path.

---

## 5d. Soft KV Divert (Conceptual)

Soft KV divert is an **off-by-default grant filter** for pull / central-push. When a
pod’s GPU KV usage is high **and another pod still has headroom**, the saturated
pod keeps only:

- affinity self-pins, and
- requests with local `prefix_len >= ROUTER_KV_SOFT_MIN_HITS`,

and leaves cold / low-hit work for healthier pullers. It does **not** dump
prefix-local conversations onto peers (that would be hard divert).

**Signal:** each sidecar optionally scrapes **local** vLLM `/metrics`
(`vllm:kv_cache_usage_perc`, fallback `vllm:gpu_cache_usage_perc`; `max` across
engines) and reports `kv_usage` on `/pull` and `/health`. Direct pod-local HTTP —
not cluster Prometheus. Sidecar switch: `KV_USAGE_REPORT` (default off).

**Activation:** `ROUTER_KV_SOFT_DIVERT=true`, self `kv_usage ≥ ROUTER_KV_PRESSURE_HIGH`
(with LOW hysteresis), and some peer with fresh `kv_usage < ROUTER_KV_PRESSURE_PEER_OK`.
If every peer is also high / samples are missing or stale → **no-op** (rebalance
cannot create capacity). Central-push refreshes samples by polling sidecar `/health`
(`ROUTER_KV_HEALTH_POLL_INTERVAL_S`).

Composes after fair-pull in `pull_for_endpoint`. Orthogonal to SLO dynamic pull
(sidecar AIMD on `want`).

---

## 6. Configuration (High-Level)

Most behavior is controlled via environment variables loaded into
`RouterConfig`, for example:

- `ROUTER_MODE` – `pull`, `push-rr`, `push-random`, `push-leastq`, `central-push`.
- `ROUTER_CENTRAL_PUSH_CAP` (default 8) – per-pod concurrency ceiling in
  central-push mode; the router dispatches `CAP − in-flight` items per pod. Set
  it to track the sidecar `batchSize + prefetch`.
- `ROUTER_CENTRAL_PUSH_INTERVAL_S` (default 0.05) – central-push periodic
  dispatch tick (dispatch is also triggered on every enqueue).
- `KV_AWARE` – enable/disable KV-aware routing.
- `LEN_AWARE`, `LEN_POLICY` – enable length awareness and choose policy.
- `AFFINITY_ENABLED`, `AFFINITY_MODE`, `AFFINITY_TTL_S`, `AFFINITY_HARD_TIMEOUT_S`
  – conversation key affinity (see [key-affinity.md](key-affinity.md)).
- `KV_HASH_SOURCE` – `inline` (default; hash in-process/in-container) or
  `external` (legacy standalone `vllm-cpu-hash` service).
- `KV_BLOCK_SIZE` – prefix block size (tokens) used to derive `matched_tokens`.
- `ROUTER_MEASURE_PREFIX` – compute per-request prefix-hit counts *for logging
  only*, even when KV routing is off (so `none`/`affinity` still report
  `kv_hits_len`/`total_blocks`/`kv_hit`). Decoupled from the routing decision.
- `ROUTER_LOG_BLOCK_HASHES` – also emit the raw `block_hashes` list per request.
- `ROUTER_FAIR_PULL` – enable the pull-mode fairness grant throttle (see 5c).
  `ROUTER_FAIR_MARGIN` (default 1.25) sets the ceiling as a multiple of the
  fleet-average in-flight; `ROUTER_FAIR_FLOOR` (default 1) is the min movable
  items an overloaded pod still gets. `ROUTER_STUCK_PULL_SECONDS` (0 = off) and
  `ROUTER_AFFINITY_RELEASE_ON_STUCK` are the optional stuck-pod controls. All
  default off; never override KV/affinity.
- `ROUTER_KV_SOFT_DIVERT` – soft GPU-KV pressure divert (see 5d). Defaults off.
  Thresholds: `ROUTER_KV_PRESSURE_HIGH` / `LOW` / `PEER_OK`,
  `ROUTER_KV_SOFT_MIN_HITS`, `ROUTER_KV_USAGE_STALE_S`,
  `ROUTER_KV_HEALTH_POLL_INTERVAL_S`. Sidecar must set `KV_USAGE_REPORT=true`.
- `ROUTER_LOG_REQUEST_BODY` – also store each request's full body (messages +
  sampling params) under `request_body` in the `/latency_log` ring. Opt-in, off
  by default; the body rides the bounded ring so it evicts automatically.
  `ROUTER_LOG_REQUEST_BODY_MAX_BYTES` (default 16384, `0` = unlimited) caps each
  body, truncating oversized ones to a `{"_truncated", "bytes", "preview"}` marker.
- `HASH_SERVICE_URL` – `/compute_hashes` endpoint; only used by the Go gateway
  (in-container hasher) and in `external` mode (unused by the Python router inline).

The Go gateway and the Python router are kept at parity: both honor
`ROUTER_STRATEGY`, `ROUTER_MEASURE_PREFIX`, `ROUTER_LOG_BLOCK_HASHES`,
`ROUTER_LOG_REQUEST_BODY`, `KV_BLOCK_SIZE`, the pull-mode fairness knobs
(`ROUTER_FAIR_PULL`, `ROUTER_FAIR_MARGIN`, `ROUTER_FAIR_FLOOR`,
`ROUTER_STUCK_PULL_SECONDS`, `ROUTER_AFFINITY_RELEASE_ON_STUCK`), soft KV divert
(`ROUTER_KV_SOFT_DIVERT` and related thresholds), and the
central-push knobs (`ROUTER_MODE=central-push`, `ROUTER_CENTRAL_PUSH_CAP`,
`ROUTER_CENTRAL_PUSH_INTERVAL_S`), and both enrich `/latency_log` with the same
prefix/KV fields.
- `REDIS_HOST`, `REDIS_PORT`, `MODEL_NAME` – KV watcher’s view of Redis keys.
- `NAMESPACE`, `LABEL_SELECTOR`, `SIDECAR_PORT` – how to find sidecars in K8s.
- `RESULT_TIMEOUT_S` – how long `/enqueue` will wait for a result.
- `REQ_LOG_MODE` – `off`, `summary`, or `full` per-request logging.

At startup, the router prints the effective config and starts:

- the KV watcher thread, and
- the push router (if in push mode).

---

## 7. Runtime View

Putting it all together:

1. Client sends `/enqueue` → router.
2. Router records metadata and computes the request's KV block hashes.
3. Router dispatches the job (queued or pushed).
4. Sidecar processes it and posts `/result`.
5. Router wakes the blocked `/enqueue` call and returns the model output.

The router is therefore the **single point** that understands:

- which requests exist,
- which workers are best for them (KV + length),
- how to expose a simple synchronous API to clients.

# Tracing (Router Service)

When `TRACE_ENABLED=true`, the router and sidecar attach timing fields to the
final response. All timestamps appear under:

    result.trace

## Router-provided timestamps

- t_enq_router_queue — request entered router queue
- t_dispatch_router — router dispatched the request to a sidecar
- t_router_result_recv — router received result from sidecar
- t_enqueue_response — router responded to the client

## Sidecar-provided timestamps (best-effort)

- t_arrive_sidecar_pull or t_arrive_sidecar_push
- t_dequeue_sidecar
- t_vllm_send
- t_vllm_recv
- t_post_result_sidecar

## /result callback

If `TRACE_ENABLED=true` and the sidecar sends a trace object, the router merges
it and appends `t_router_result_recv` before resolving the waiting client.
