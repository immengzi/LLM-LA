# Dynamic P/D Rebalancing with Per-Card Warm Standby

> Goal: each card hosts both a Prefill and a Decode vLLM engine in the same pod
> on the same NPU card, with exactly one engine awake at a time. Requests are
> routed pool-wide to any awake engine; a P/D rebalance is a per-card
> sleep/wake flip that completes in seconds without recreating pods or
> reloading model weights.

## 1. Topology

```text
One card = one pod (vllm-<model>-pd)
┌─────────────────────────────────────────────────┐
│ vllm-prefill :8200 (KV producer)                │
│   - owns the configured accelerator resource    │
│ vllm-decode  :8201 (KV consumer)                │
│   - device-share + privileged + hostIPC         │
│   - exactly one engine awake (mutual exclusion) │
└─────────────────────────────────────────────────┘
```

- `replicas` = number of dual-engine pods (defaults to `maxTotalReplicas`);
  tensor parallelism 1 and 2 are both supported on a single node. With TP=1
  each pod owns one card and one engine; with TP=2 each pod owns a two-card
  pair and each engine inside the pod uses that pair (only one engine is
  awake at a time).
- Startup gate: the decode container waits for the prefill `/health`, puts the
  prefill to sleep, and only then starts its own engine, so each pod boots with
  a single awake engine. The rebalancer then converges to the target `(P,D)`.
- Pod labels (written only by the rebalancer):
  `pd-prefill-awake: "true"|"false"` and `pd-decode-awake: "true"|"false"`.
- Services:
  - `vllm-<model>-prefill`: selects pods with `pd-prefill-awake=true`,
    port 8200 -> targetPort 8200;
  - `vllm-<model>-decode`: selects pods with `pd-decode-awake=true`,
    port 8200 -> targetPort 8201.
- The proxy stays pool-level: `PREFILL_BASE`/`DECODE_BASE` point to the two
  Services and requests are routed to any awake engine of the required role. The
  decode phase pulls KV from the shared store (Mooncake/LMCache) by hash; it is
  never routed to the co-located engine on the same card.
- A planned flip is hidden from clients per card: the rebalancer takes the
  card's engine out of its role Service (label patch) and waits until that
  engine's own in-flight gauges reach zero before sleeping it, so every other
  card keeps serving and in-flight requests finish normally. The proxy only
  queues for the model-wide drain that the *blocking* KV warm-up gate requests:
  it queues new P/D requests at the request entry and again at each phase
  boundary, so a request admitted during that window waits for the new topology
  instead of racing the endpoint switch. Planned unavailability is
  reported as `503` with `Retry-After`; `502` is reserved for real upstream
  protocol errors. Requests that fail on an unplanned endpoint error are retried
  as a whole (prefill + decode) for at most `PROXY_RETRY_DEADLINE_SECONDS`
  (default 60s), always before any byte is relayed to the client, and
  `X-Request-Id` is deduplicated while a request is in flight.

## 2. Key design points

### 2.1 Routing isolation

- Sleeping engines are removed from Service endpoints through their labels;
  the engine `/health` endpoint keeps returning 200 while sleeping, so the
  label is the reliable isolation mechanism.
- Requests never trigger a wake: phase 2 goes to any awake decode pod through
  the Service. Sleep/wake is only performed by the rebalancer during a rebalance.

### 2.2 Card-level flip (rebalancer executor)

- A target `(P,D)` keeps pods that already match a needed role and minimizes the
  number of flips. One transition trades replicas between the roles, so `P + D`
  is preserved: the planner moves up to `max_step_replicas` replicas per
  decision (default 1), clamped by the role floors and the fixed budget, and the
  executor applies the whole target atomically.
- Per-card flip sequence: patch labels to idle
  (`pd-*-awake: "false"`, the card leaves its role Service) -> wait until the
  active engine's own `vllm:num_requests_running/waiting` is zero and stays zero
  for `quiesceSettleSeconds` (`PD_REBALANCER_QUIESCE_SETTLE_SECONDS`, default 3s)
  -> `POST /sleep` -> wait for `is_sleeping=true` -> `POST /wake_up` on the
  co-located peer after the `wakeAfterSleepSeconds` grace (retrying transient
  HTTP errors) -> wait for `/health` -> patch labels to the new role. Each flip
  logs `quiesce=/sleep=/wake=/total=` timings.
- The cards of one transition flip **concurrently** (`_apply_flip_plan`): every
  pod owns its own NPUs and ports, so a multi-replica move costs roughly one
  flip (`PD_REBALANCER_FLIP_PARALLELISM=<n>` caps the batch; `1` = sequential,
  for troubleshooting a wake resource spike). The batch is always joined before
  returning, so the rollback path sees every card in its final state.
- Mutual exclusion is a hard constraint: the current engine must be asleep
  (NPU released) before the peer is woken; waking the peer too early can
  OOM-kill EngineCore.
- Labels are patched after every engine step, not only at the end, so the
  Services always route to reality: once the old role is asleep the card is
  reported idle, and only after the peer reports healthy is the new role
  published.
- If waking the peer fails, the card is left idle and recorded in the
  rebalancer's `needs_recreate` set: the executor never wakes a peer on a
  card whose engine may have crashed mid-wake (that is the crash-cascade
  path), and rollback skips flagged cards. The only safe recovery is pod
  recreation.
- On failure or stale transition, per-card previous roles are recorded and
  restored; wake failures additionally honor `PD_REBALANCER_WAKE_RETRIES`
  (default 3) and `PD_REBALANCER_WAKE_BACKOFF_SECONDS` (default 5) before a
  card is flagged.
- `Deployment /scale` is never touched in warm-standby mode.

### 2.3 Pre-warm pool and SLO-driven scaling

A pre-warm pool card keeps **both** engines asleep — `pd-prefill-awake=false`
AND `pd-decode-awake=false`, so no engine on that card is in wakeup state.
Every dual-engine card has both engines loaded, so any role can be served
within a wake (seconds) instead of a cold pod start (minutes). Steady state
keeps exactly `target.prefill + target.decode` engines awake across the serving
cards; every other card is fully asleep and holds a reserved card slot.

- **Structure**: a single pre-warm pool with no role preference and
  no minimum reservation — pool depth is simply
  `replicas - target.prefill - target.decode`, consumed by scale-up and
  regenerated by scale-down. (A per-role dual-pool design and a `poolCards`
  minimum reservation were considered and rejected; revisit only if roles ever
  run different weights or a guaranteed standing reserve is required.)

- **Scale-up**: wake a role engine on a fully-asleep pool card (the card
  leaves the pool) and patch its labels into the Service. Waking never
  interrupts in-flight traffic, so the transition never drains the proxy.
- **Scale-down**: patch the excess engines out of the Service, quiesce them
  (wait for their own in-flight gauges) and sleep them; those cards return to
  the pool with both engines asleep. This is per card, so the remaining cards
  keep serving — there is no model-wide pause.
- **When the proxy still drains**: only the *blocking* KV warm-up gate
  (`kvWarmup=1` with `kvWarmupBlocking=1`) needs an isolated probe window; it
  pauses new requests for the budgeted warm-up. With the default background
  gate (`kvWarmupBlocking=0`) every transition is drain-free.
- **Pool depth**: `GET /v1/targets/<model>` reports
  `pool: {cards}` — the number of cards whose BOTH engines are asleep. A card
  with one engine awake (even if its peer is sleeping) is serving, not pooled.
- **Bootstrap**: with `prefillDecode.poolBootSleep=true` each pod sleeps BOTH
  engines after boot (labels `pd-*-awake: "false"`), so the first reconcile is
  a drain-free wake of exactly the target roles, leaving
  `replicas - target.prefill - target.decode` cards in the pool. The default
  (`false`) keeps the decode-awake boot and lets the rebalancer
  converge to the same steady state.

Wake/sleep operations are idempotent: the executor checks `/is_sleeping` before
`POST /wake_up` / `POST /sleep`, so a label/runtime divergence (e.g. after a
pod restart) is repaired with a label patch instead of an engine error.

### 2.4 Budget / capacity

- Card count `replicas >= maxTotalReplicas`; awake `P + D <= replicas`
  (steady state `== replicas`).
- Cost: CPU memory holds two weight copies (sleep level 1 keeps the sleeping
  engine's weights in CPU memory); the NPU runs one engine at a time.

### 2.5 Configuration

```yaml
prefillDecode:
  replicas: 4            # dual-engine pods (cards); null -> maxTotalReplicas
  prefill:
    port: 8200
    tensorParallelSize: 2
    batchSize: 16
    # Multi-process port plan (see 2.6) - required in warm-standby mode
    hcclSocketPortRange: "63000-63050"        # HCCL_NPU_SOCKET_PORT_RANGE
    hcclHostSocketPortRange: "62000-62050"    # HCCL_HOST_SOCKET_PORT_RANGE
    hixlListenPort: 16700                     # HIXL / NPU-adapter listen port
  decode:
    port: 8201
    tensorParallelSize: 2
    batchSize: 16
    hcclSocketPortRange: "65000-65050"
    hcclHostSocketPortRange: "64000-64050"
    hixlListenPort: 16800
  dynamicRebalance:
    mode: warmstandby    # scale (cold /scale) | warmstandby (per-card sleep/wake)
    minPrefillReplicas: 1
    minDecodeReplicas: 1
    maxTotalReplicas: 4
    sleepLevel: 1
  poolBootSleep: false   # optional: boot both engines asleep (pre-warm pool)
```

The rebalancer retry knobs are chart values too: `pdRebalancer.sleepRetries`
(default 5) and `pdRebalancer.sleepBackoffSeconds` (default 2) for `/sleep`;
`pdRebalancer.wakeRetries` (default 3) and
`pdRebalancer.wakeBackoffSeconds` (default 5) for `/wake_up`; and
`pdRebalancer.wakeAfterSleepSeconds` (default 2) as a short grace between a
confirmed sleep and the peer wake.

The KV warm-up gate (`pdRebalancer.kvWarmup`, default 0 in the chart) pays the
one-off cold start of each *engine pair* before user traffic sees it: a fresh
pair needs ~3.4 s for its first transfer on each leg, and after a flip that cost
otherwise lands on the first request. Two modes:

* `pdRebalancer.kvWarmupBlocking=1` (delivery setting) runs a light probe
  (unique ~270-token prompt, `max_tokens=1`) inside the proxy **drain window** and
  holds traffic until it finishes or `kvWarmupBlockingBudgetSeconds` (default 20)
  expires - past the budget the traffic is released and the probe finishes in the
  background, while a failure *within* the budget still rolls the flip back.
* `kvWarmupBlocking=0` runs it in a background thread after the drain is
  released: the flip stays as short as possible, but the first request may race
  the probe.

### 2.6 Multi-process port plan (two engines on one card)

A warm-standby pod runs a prefill **and** a decode engine on the same NPUs, so
every port family has to be split between the two containers. Three independent
layers matter, and only the third controls the port that used to break role
flips:

1. `HCCL_NPU_SOCKET_PORT_RANGE` / `HCCL_HOST_SOCKET_PORT_RANGE` - HCCL's own
   device-side and host-side sockets. The two roles' ranges must be disjoint and
   must avoid CANN's reserved ports 16666-16667 (libhcomm rejects a range that
   covers them, and a second process per card otherwise has to run without
   HCCL's multi-process support).
2. `ASCEND_GLOBAL_RESOURCE_CONFIG` = `{"comm_resource_config.listen_port": N}` -
   the **HIXL/ADXL (NPU network adapter) listen port**. The HCCL ranges do *not*
   cover it, and the HIXL rank table carries no port fields either: without a
   per-role value both engines on the card fall back to the same reserved
   default (16666) and whichever wakes second cannot bind. The reference
   deployment uses 16700 (prefill) / 16800 (decode).

The chart validates the plan at render time (`_helpers.tpl`): both roles must set
all three keys; ranges must be `<start>-<end>` with start <= end, disjoint
between the roles, and must not cover 16666-16667; `hixlListenPort` must be an
integer in 1024-65520, differ between the roles, and stay outside its own role's
ranges. The chart ships these values **empty** and renders a `fail` with
guidance, because the concrete numbers are environment specific (they must miss
the node's other services and `HCCL_IF_BASE_PORT`'s 16-port block).

Measured on the reference deployment (8x910B3, CANN 9.1.0, vllm-ascend v0.23.0):
role flips stay at 13-14 s while the first request after a flip is ~1.5 s.

### 2.7 Sleep-mode requirements

The engines run with vLLM sleep mode (`--enable-sleep-mode`). The following are
required for the supported engine image:

1. Remove `PYTORCH_NPU_ALLOC_CONF=expandable_segments:True` (the CaMem
   allocator rejects it).
2. Remove `--compilation-config` with `FULL_DECODE_ONLY` (conflicts with
   sleep-mode ACL graph capture).
3. Set `VLLM_ASCEND_ENABLE_NZ=0`.
4. Set `VLLM_SERVER_DEV_MODE=1` (exposes `/sleep` and `/wake_up`) and
   `VLLM_WORKER_MULTIPROC_METHOD=spawn`.
5. Keep the KV transport connections short-lived:
   `ASCEND_USE_SHORT_CONNECTION=1` (chart value
   `vllm.ascendUseShortConnection`, default "1"). With long-lived HIXL comms a
   sleeping engine that has served external KV keeps roughly 50 GiB of physical
   pages pinned and `wake_up` cannot remap them.
6. Pin Mooncake store PUTs to the writing engine's own segment:
   `mooncake.preferredSegment=true` (rendered into `mooncake.json`). Otherwise a
   PUT can land on a co-located sleeping engine and the later pull fails.
7. Install the CaMem/Mooncake sleep-wake overlay: either build the engine image
   with the companion patch (preferred for a release), or enable
   `vllm.sleepOverlay` and inject the patched sources at deploy time. The chart
   is file-agnostic - it renders whatever `vllm.sleepOverlay.files` describes -
   and the patch package (`overlay.json` + `make-overlay-command.py`) produces
   both the values fragment and the `--set-file` flags. Patch sources are never
   committed into this repository, and enabling the overlay with an incomplete
   entry fails the render with guidance.

The chart injects these automatically in warm-standby mode.

## 3. Component changes

| Component | Change |
|---|---|
| `41-vllm-pd-disagg.yaml` | warmstandby: single dual-engine Deployment (startup gate, device-share, sleep env) + per-role Services selected by awake labels |
| `42-vllm-pd-rebalancer.yaml` | models JSON carries `mode/deployment/replicas/ports/sleepLevel`; RBAC restricted to the dual-engine Deployment and pod label patches |
| `files/pd_rebalancer.py` | per-card awake counting, flip executor with drain-free pool scale-up and idempotent wake/sleep, `previousRoles` rollback, pool status, mode dispatch (scale/warmstandby) |
| `files/pd_proxy.py` | drain-first queueing at the request entry and at both phase boundaries; `503` + `Retry-After` for planned transitions; bounded whole-request retry for unplanned endpoint failures; `X-Request-Id` in-flight dedup; transition metrics |
| `files/pd_planner.py` | target = awake counts with `max_step_replicas` per decision (default 1, clamped by floors + budget); decode metrics scraped from the decode port |

## 4. Validation plan

- Unit tests: awake counting, minimal-flip planning, sleep-before-wake ordering,
  wake retry/needs_recreate, KV warm-up gating, rollback and budget validation
  (`tests/pd_rebalancer/`); proxy transition semantics (`test_pd_proxy.py`);
  Helm render assertions (`tests/helm/test_pd_warm_standby.py`, requires `helm`
  on PATH).
- Request correctness: fixed `X-Request-Id` end-to-end through proxy, prefill and
  decode; no lost or hung requests during a planned flip; decode-side KV hit
  evidence; no decode-only fallback.
- Observability: the proxy exposes `pd_proxy_retry_total`,
  `pd_proxy_drain_wait_seconds_total`/`_count`, `pd_proxy_reject_503/502/409_total`
  and phase-duration counters; the rebalancer logs `sleep`/`wake`/`total`
  timings per flip.
- Performance: transition duration of warmstandby flips vs cold `Deployment
  /scale` transitions under identical workloads.

## 5. Known boundaries

- Prompts shorter than the KV connector block granularity are not published to
  the shared store and therefore not reused; prompts covering at least one
  full block are published and pulled by the decode engine (connector design,
  topology independent).
- Validated on vllm-ascend v0.23.0 with short-lived connections: single-node
  TP=2 warm-standby flips P1,D2 <-> P2,D1 with concurrent requests (8 rounds,
  16 flips, 40/40 requests served, no engine restarts) and sleep/wake memory
  checks (freed ~55.9 GiB, restore 223/223, re-register 72/72). The earlier
  TP=1/DP=1, TP=1/DP=2 and TP=2/DP=2 matrices were validated on the v0.18 image
  and have not been re-run on v0.23.0.
- Long-lived KV transport connections are not supported: a sleeping engine that
  has served external KV retains ~50 GiB of pinned pages and the peer wake
  fails. Keep `ASCEND_USE_SHORT_CONNECTION=1`.
- `hostNetwork=true` is not supported in warm-standby mode (port and loopback
  semantics conflict with two engines per pod).
- First cold start loads the model twice (prefill, then decode); steady-state
  flips complete in seconds.
- device-share must be enabled on the node, and containers need privileged mode
  plus full driver mounts.
- Each engine must bind the physical card allocated by the device plugin (for
  example via the allocation annotation); hardcoded device indices are invalid
  when the plugin assigns arbitrary card numbers.
