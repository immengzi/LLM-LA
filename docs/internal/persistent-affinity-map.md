# Persistent Affinity Map (Redis-backed conversation → pod)

> Extends the router key-affinity feature
> ([../architecture/key-affinity.md](../architecture/key-affinity.md)) with an
> optional durable backing store so the `conversation_key → pod` map survives
> router restarts and full redeploys. **Off by default; feature-off behavior is
> byte-for-byte identical to the in-memory-only map.**

---

## 1. Why

The affinity map (`conversation_key → endpoint`) that keeps every turn of a chat
on the same vLLM pod lives **only in the router process memory**
(`router/affinity.py::AffinityMap`). Two consequences:

- A router restart / rollout / OOM wipes the map, so in-flight conversations
  lose their pin and re-scatter across pods on their next turn (the engine
  prefix cache is still warm, but the *routing hint* is gone until re-pinned).
- A multi-writer topology cannot share pins.

Persisting the map in Redis (which the router already runs for KV state) makes
the pins durable across restarts and redeploys.

## 2. What is persisted

The **affinity key** — `sha256("model:…|system:…|user:<first user msg>")[:16]`,
i.e. `meta["__affinity_key__"]` — mapped to the **endpoint (pod name)** that
last served it. This is *not* the KV block/prefix hash (`prefix_hash.py`); it is
the conversation stickiness key derived in `affinity.py::derive_affinity_key`,
reused unchanged.

### Key schema

```
{AFFINITY_REDIS_KEY_PREFIX}:{cluster}:{model}:{affinity_key}  ->  <pod name>
```

- `cluster` = `CLUSTER` env, falling back to the k8s namespace when empty, so
  keys are namespaced per deployment out of the box.
- `model` = `MODEL_NAME`.
- Reuses the existing `redis` package, `redis://{REDIS_HOST}:{REDIS_PORT}` URL,
  and colon-namespaced convention used by `kv_watcher.py`. No second Redis
  server and no parallel client abstraction (the store uses the sync client
  since the pull path is synchronous; the KV watcher keeps its async client —
  same Redis service).

## 3. How it works

```
admission (api._inject_affinity)
  derive key ──► prefetch(key): in-memory miss ─► Redis GET ─► populate memory   (1 GET/request)

pull_for_endpoint (hot path)
  AffinityMap.lookup(key)  ── in-memory ONLY (never hits Redis)
  hard-mode filter: pinned to an *unavailable* pod ─► fall back to normal LB
  dispatch ─► AffinityMap.claim(key, pod)
                 ├─ in-memory set (immediate)
                 └─ enqueue write-through ─► background writer ─► pipelined SET   (off hot path)

startup (api._startup)
  warm_affinity_from_store(): SCAN {namespace}:* ─► bulk-load memory
```

Design guarantees:

- **Hot path stays in-memory.** `lookup()` (called per pool item, per pull) is
  memory-only. The only per-request Redis read is a single `GET` at admission
  on a memory miss (rate = request rate, ~sub-ms, local Redis). Measured
  write-through overhead on `claim()` is ~3 µs (a non-blocking
  `queue.put_nowait`); the actual `SET` runs on a background writer thread and
  is log-and-continue on error.
- **Concurrent writes** for the same key are safe: claims are queued and applied
  in order by a single writer; last-write-wins matches
  claim-follows-latest-puller semantics. `SET` is atomic.
- **Pod availability = per-pod readiness.** A persisted mapping can point at a
  pod that is still loading weights (post-redeploy), was scaled down, or gone.
  The router tracks each endpoint's last pull time (`_seen_endpoints`); a sidecar
  `/pull` is **health-gated on vLLM `/health`**, so a pull means the pod was
  serviceable ≤5s ago — the table doubles as a **per-pod READY heartbeat**. A
  mapping to an endpoint not seen within `AFFINITY_ENDPOINT_STALE_S` (never
  pulled → still loading / never ready, or gone) is treated as a miss → fall back
  to normal LB → the dispatch re-`claim`s (and persists) the new pod. This check
  is **gated on persistence being enabled**, so the legacy path is unchanged. See
  §8 for the single-signal readiness design and its load-bearing invariant.
- **In-memory cache bound.** `AFFINITY_CACHE_MAX` caps memory; evicted keys stay
  durable in Redis and are brought back via `prefetch`.

## 4. Configuration

Client config (`src/configs/*.yaml`, under `helm:`) → chart values
(`values.router.*`) → router env:

| helm config key | values.router.* | env | default | purpose |
|---|---|---|---|---|
| `router_affinity_persist_enabled` | `affinityPersistEnabled` | `AFFINITY_PERSIST_ENABLED` | `false` | master switch |
| `router_affinity_redis_ttl_seconds` | `affinityRedisTtlSeconds` | `AFFINITY_REDIS_TTL_SECONDS` | `0` | per-key TTL; `0` = no expiry |
| `router_affinity_redis_key_prefix` | `affinityRedisKeyPrefix` | `AFFINITY_REDIS_KEY_PREFIX` | `affinity` | key namespace prefix |
| `router_affinity_cache_max` | `affinityCacheMax` | `AFFINITY_CACHE_MAX` | `100000` | in-memory cache bound (`0` = unbounded) |
| `router_affinity_cache_refresh_s` | `affinityCacheRefreshS` | `AFFINITY_CACHE_REFRESH_S` | `0` | periodic re-warm (`0` = startup only) |
| `router_affinity_endpoint_stale_s` | `affinityEndpointStaleS` | `AFFINITY_ENDPOINT_STALE_S` | `1800` | pod "available"/READY-heartbeat window (see §8) |
| `router_affinity_cluster` | `affinityCluster` | `CLUSTER` | `""` | key-namespace cluster (empty → k8s namespace) |

Persistence requires affinity itself to be on (`router_strategy: affinity`/`both`
or `router_affinity_enabled: true`).

### TTL trade-off (`AFFINITY_REDIS_TTL_SECONDS`)

- `0` (no expiry): pins survive arbitrarily long deploy gaps, but stale keys for
  scaled-down pods accumulate (bounded in practice by the pod-availability
  fallback + last-write-wins overwrite on the next turn).
- finite (e.g. `86400`): bounds stale keys, but drops mappings for conversations
  idle longer than the TTL (they simply re-pin on their next turn).

## 5. Durability across redeploy (Redis persistence)

The in-router persistence is only as durable as Redis itself. The Redis manifest
(`templates/10-redis.yaml`) gains, when `redis.persistence.enabled=true`:

- `--appendonly yes` + `--appendfsync everysec` (AOF) with `--save 60 1` kept as
  an RDB backstop, `--dir /data`;
- a `PersistentVolumeClaim` (`redis-data`) mounted at `/data`;
- `strategy: Recreate` (an RWO PVC cannot be mounted by two pods at once).

Values:

```yaml
redis:
  appendfsync: everysec       # everysec | always | no
  persistence:
    enabled: true
    size: 5Gi
    accessMode: ReadWriteOnce
    storageClass: ""          # "" = cluster default
```

With this, deleting/recreating the Redis pod (or a full redeploy) preserves the
map: the new Redis loads its AOF from the PVC, and the router `warm()`s it at
startup.

## 6. Metrics

Existing affinity metrics apply (`router_affinity_hits_total`,
`router_affinity_map_size`, …). `router_affinity_map_size` reflects the warmed +
live in-memory size. Store errors are logged (`[AffinityStore] …`) and never
fatal.

## 7. Source map

| Concern | Location |
|---|---|
| Durable store (writer, warm, get) | `router/affinity_store.py` |
| Map integration (claim/lookup/prefetch/warm/close, cache bound) | `router/affinity.py::AffinityMap` |
| Config knobs | `router/config.py`; `src/config.py`; `src/sweep_methods.py` |
| Wiring (admission prefetch, startup warm, shutdown close, availability) | `router/api.py`, `router/router_state.py` |
| Redis persistence manifest | `src/vllm-kv-stack/templates/10-redis.yaml`, `values.yaml` (`redis:`) |
| Tests | `tests/test_affinity_persist.py` |
| Readiness anchor / invariant | §8 below; `_seen_endpoints` in `router/router_state.py`; sidecar health gates in `sidecar/router_client.py`, `go/internal/sidecar/pull_worker.go` |

## 8. Readiness anchor & the load-bearing invariant

Warmed cross-pod mappings must only be honored once their target pod can
actually serve. We do **not** use a router-`warm()`-anchored grace timer, a
`phase==Running` discovery/existence set, a separate `ready_ts` map, or a
background vLLM `/health` poller. Instead we use a **single signal** that is
already in the routing path: the sidecar pull.

**A health-gated pull IS the per-pod READY event.** Both sidecars only issue a
`/pull` when vLLM `/health` returned 200 within the last ~5s (vLLM returns 200
only after weights are loaded):

- Python: `check_vllm_health()` (`sidecar/router_client.py`), poll loop pulls
  only when healthy, `pull_if_capacity()` re-checks `_vllm_healthy`.
- Go: `CheckVLLMHealth()` (`go/internal/sidecar/pull_worker.go`), `pollLoop`
  pulls only when healthy, `PullIfCapacity()` re-checks `vllmHealthy`.

So **"pulled" ⟹ "was serviceable ≤5s ago"** — a true READY signal, not "container
Running". An idle-but-ready pod still pulls every poll tick (empty local queue →
`reserved < pull_cap` → it hits `/pull` with `want>0`, gets 0 back), so it is a
**continuous READY heartbeat** refreshed sub-second even under zero traffic.

`_seen_endpoints[endpoint]`, stamped at the top of `pull_for_endpoint`, is
therefore already a valid per-pod ready timestamp — the last time the pod was
both vLLM-healthy and able to take work through its sidecar (a *stronger* signal
than a bare `/health` probe, which would over-report a pod whose sidecar is
stuck). It repopulates naturally from post-restart pulls; nothing is seeded at
`warm()` time.

### Predicate (`_endpoint_available`)

```
persist off                              -> True   (byte-identical legacy path)
_seen_endpoints[target] missing          -> False  (never ready / still loading -> LB)
now - _seen_endpoints[target] > STALE_S  -> False  (gone / scaled down -> LB)
otherwise                                -> True   (ready & serving -> honor pin)
```

- A warmed cross-pod mapping becomes valid **the instant its target pod is ready**
  (first health-gated pull, ~one sub-second poll tick after vLLM goes healthy),
  correctly surviving the full ~10-min cold start — timing only advances once the
  pod can actually serve. It is **not** honored before any pull.
- Never-ready/still-loading and gone/scaled-down pods both fall back to LB.
- `_seen_endpoints` is the **sole** signal. `STALE_S` (Change 1: 120 → 1800) is
  the ready-heartbeat staleness bound; because ready pods heartbeat sub-second it
  only matters as headroom against a ready pod going briefly silent. Its sole
  cost is that a genuinely dead pod's mappings linger a bit longer before
  fallback (harmless: miss → LB). It is **not** reused by any other timer and
  stays independent of the readiness logic.
- Off the hot path: the only write is `_seen_endpoints[endpoint] = time.time()`
  at the top of `pull_for_endpoint`; the check is a dict read. No health/k8s
  calls in `claim()` or pull dispatch, and no new deps.

### Invariant (load-bearing — protect it)

The entire correctness of this readiness anchor rests on **"a sidecar `/pull` is
health-gated on vLLM `/health`."** If someone later changes the sidecar to pull
before vLLM is ready — e.g. a warmup/pre-fetch pull, or removing/relaxing the
health gate — this affinity readiness check **silently breaks**: warmed pins
would be honored for a not-yet-ready pod.

**Any change to sidecar health-gating REQUIRES re-evaluating
`_endpoint_available`.** Back-reference comments are planted at the four gate
sites (`sidecar/router_client.py` poll loop + `pull_if_capacity`;
`go/internal/sidecar/pull_worker.go` `pollLoop` + `PullIfCapacity`) and at the
stamp/check sites in `router_state.py`, all pointing here.

### Why not the alternatives

- **Grace timer (`AFFINITY_WARM_GRACE_S`, router-`warm()` anchored):** the only
  startup-anchored event is router boot, independent of vLLM. With ~10-min vLLM
  cold start, any sane grace expires long before pods are Ready, so it is a no-op
  exactly when persistence is meant to help. A correct timer would need a
  per-pod first-Ready anchor sized above cold start — a fragile magic number.
- **Discovery/existence set (`phase==Running`):** would honor pins to pods whose
  container is up but whose weights are still loading (not serviceable), and adds
  a second signal to maintain. `phase==Running` is weaker than "can serve".
- **Separate `ready_ts` map + `/health` poller:** duplicates a signal already in
  the path, adds HTTP/k8s calls, and is *less* correct (vLLM-healthy but
  sidecar-stuck would be wrongly counted ready).

The sub-second "ready but not yet pulled" window is harmless (miss → LB →
recovers on the next poll tick) and not worth a weaker, more expensive signal.

## 9. Live validation (BZ cluster)

Validated live on **real Claude Code traffic** against the BZ deployment. Five
points were verified end-to-end; two items are explicitly **deferred** (below).

**Environment**
- Cluster **BZ**, namespace `vllm`, Helm release `vllm`.
- Router image `reg.local:32000/kv-router:persist-affinity-9004c35`
  (digest `sha256:89be8a05…f89fd`), pinned = rebased branch HEAD.
- `AFFINITY_PERSIST_ENABLED=true`, `AFFINITY_MODE=hard`,
  `AFFINITY_ENDPOINT_STALE_S=1800`, `ROUTER_STRATEGY=affinity`.
- Redis namespace `affinity:vllm:served-model` (`{prefix}:{cluster→ns}:{model}`).
- **Redis was the EPHEMERAL variant this round** (`appendonly no`, no PVC) — see
  "Deferred" below; this does not affect the router-logic tests, which never
  restart the Redis pod.

**(1) Write path — requests produce Redis mappings.** Live Claude Code turns
created keys under `affinity:vllm:served-model:<key>` mapping each conversation
to a pod (confirmed via `redis-cli --scan` + `GET`).

**(2) Stable pod-name values.** Every value was a **stable pod name**
(`vllm-minimax-m2-0` / `vllm-minimax-m2-1`), never a pod IP or a random id — so
mappings remain meaningful across a redeploy (LWS pod names are stable).

**(3) Stickiness / no key drift.** Via the router's `/latency_log`, every
multi-occurrence affinity key routed to a **single** pod across its turns. Keys
stayed **stable as `prompt_tokens` grew ~11k → ~65k** over a conversation — no
mid-session key drift was observed on the Claude Code traffic tested. (Drift
would appear as one conversation's system-prompt fingerprint changing between
turns; it did not. See the invariant/derivation notes for the theoretical drift
sources — volatile system-prompt state.)

**(4) Startup warm reload.** After a `rollout restart` of the router, the new
pod logged:
```
[PullRouter] affinity map warmed from store: 12 mappings
```
`N=12` matched the Redis mapping count exactly — the in-memory map was rebuilt
from Redis (`AffinityMap.warm()` → `RedisAffinityStore.warm()` SCAN), so pins
survived the router restart rather than being lost to an empty in-memory map.

**(5) Request-time read path (in-memory miss → single Redis GET → route).**
Proven with `redis-cli MONITOR`. Two **fresh** keys (never in memory) were
pre-seeded into Redis only, pinned to **different** pods, then a request that
hashes to each key was sent:

| key (derived by the router's own `derive_affinity_key`) | pre-seeded pod | result |
|---|---|---|
| `52045bb6fc0d5c46` | `vllm-minimax-m2-0` | GET fired, routed to m2-0 |
| `b32b7daaf82e6b12` | `vllm-minimax-m2-1` | GET fired, routed to m2-1 |

MONITOR captured the exact request-time reads:
```
"GET" "affinity:vllm:served-model:52045bb6fc0d5c46"     → then routed to m2-0
"GET" "affinity:vllm:served-model:b32b7daaf82e6b12"     → then routed to m2-1
```
`/latency_log` confirmed each `affinity_key → endpoint` matched its pre-seeded
pod (two different pods ⇒ ~25% coincidence, ruled out alongside the MONITOR
proof). Corroborating metrics: `router_affinity_map_size` **12 → 14** (both
fresh keys entered memory only via the request-time GET — no warm/refresh runs,
`AFFINITY_CACHE_REFRESH_S=0`) and `router_affinity_hits_total` **0 → 2**. Code
path exercised: `api.py::_inject_affinity` → `router_state.affinity_prefetch` →
`AffinityMap.prefetch` (`affinity.py:132`) → `RedisAffinityStore.get`
(`affinity_store.py:154`). Test keys were deleted afterward; the namespace
returned to its prior 12 mappings.

**Deferred follow-ups (not yet validated):**
- **Redis on-disk durability** (AOF + PVC / hostPath). Requires a bindable
  StorageClass per cluster (BZ has only non-default `local-path`), so it is
  wired per-cluster via the cluster-switch/values rather than defaulted. Needed
  only to survive a **Redis pod** restart; all of (1)–(5) hold with ephemeral
  Redis because they never restart Redis.
- **L4 vLLM cold-start continuity** — the end-to-end acceptance test that a
  warmed cross-pod pin degrades to LB while the target pod loads weights, then
  resumes on the pod's first health-gated pull. The mechanism is validated by
  design (§8) and by (5); the full live cold-start exercise is pending.

> Reminder — load-bearing invariant (§8): the readiness anchor depends on the
> sidecar pull being **health-gated on vLLM `/health`**. Any change to sidecar
> health-gating (a warmup pull, or relaxing/removing the gate) **requires
> re-evaluating `_endpoint_available`**, or warmed pins could be honored for a
> not-yet-ready pod.

---

## Appendix — investigation: writer-queue & staleness semantics

### [A] Background writer queue-full behavior

- **Bounded.** `RedisAffinityStore.__init__` creates
  `queue.Queue(maxsize=max(1, writer_queue_max))` with `writer_queue_max=100000`
  by default (`affinity_store.py:91`, default at `:81`). `build_affinity_store`
  does not override it, so the effective bound is **100000** entries.
- **Drop on full, never blocks, never grows unbounded.** `put()` uses
  `put_nowait` (`affinity_store.py:109`) and swallows `queue.Full`
  (`:110`) — the upsert is **dropped**. This keeps `claim()` off the hot path
  (no blocking, no synchronous Redis).
- **Partially observable.** On a drop, an internal integer `self._dropped` is
  incremented (`:111`) and a **rate-limited log** line is emitted on the 1st,
  1001st, 2001st… drop (`:112–113`, `% 1000 == 1`). This is **not** a Prometheus
  metric — it is invisible on `/metrics`, only in router stdout. There is **no**
  signal at all when the queue merely *backs up* (high depth but not yet full).
  → Fix 2 target: expose a dropped-writes Prometheus counter (keep the log);
  the drop policy itself is already correct (bounded + drop self-heals via
  idempotent re-claim on the next request), so no blocking/retry is added.

### [C] Staleness timestamp semantics

- **PER-POD liveness (semantics 1), not per-mapping.** `pull_for_endpoint`
  records `self._seen_endpoints[endpoint] = time.time()` at the top of *every*
  pull, keyed by **endpoint (pod name)**, for **any** request regardless of
  `affinity_key` (`router_state.py:242–243`). `_endpoint_available` compares
  `time.time() - _seen_endpoints[endpoint] <= AFFINITY_ENDPOINT_STALE_S`
  (`:955–959`) against the item's *target* pod (`:1040`). So it means "has this
  pod pulled **anything** within STALE_S", not "when did this pod last pull for
  this key".
  → Consequence: because pulls are health-gated, this per-pod "pulled recently"
  window doubles as a per-pod READY heartbeat (see §8). Ready pods heartbeat
  sub-second, so the default is set to **1800s** (Change 1) purely as headroom
  against a briefly-silent ready pod; the per-mapping/think-time-gap concern does
  **not** apply here.

- **`warm()` does NOT restore `_seen_endpoints`.** `warm_affinity_from_store` →
  `AffinityMap.warm()` repopulates only the `key → endpoint` map
  (`router_state.py:974–987`). `_seen_endpoints` is initialized empty
  (`:165`) and is never seeded from Redis. So immediately after a
  restart/redeploy, `_seen_endpoints.get(target)` is `None` for every pod, and
  `_endpoint_available` returns `False` (`:956`) → **every warmed *cross-pod*
  mapping is judged unavailable and falls back to LB** until that target pod is
  first observed pulling post-startup.
  - Bounded by "until each target pod's first post-startup pull" (usually
    seconds), **not** by the full STALE_S window; and self-pins
    (`target == pulling endpoint`) are unaffected because `_affinity_filter_hard`
    short-circuits on `target == endpoint` before the availability check
    (`:1040`).
  - This "gap" is the intended behavior: a warmed cross-pod mapping is honored
    only **once its target pod is actually ready** (its first post-restart
    health-gated pull), not before. See §8 — this is the single-signal readiness
    anchor, which is why no `warm()`-time seeding or grace period is used.

---

## Appendix 2 — investigation trail (readiness anchor)

The readiness-anchor design in §8 is the outcome of an investigation that
considered and **rejected** several alternatives. The full reasoning now lives in
§8 ("Why not the alternatives"); the short history:

1. A **router-`warm()`-anchored grace timer** (`AFFINITY_WARM_GRACE_S`) was
   proposed first. Rejected: the only startup-anchored event is router boot,
   independent of vLLM; with ~10-min vLLM cold start any sane grace expires long
   before pods are Ready, making it a no-op exactly when persistence should help.
2. A **`phase==Running` discovery/existence set** (via KVWatcher's pod list) was
   proposed next. Rejected: `Running` (container up, weights still loading) is
   weaker than "can serve", and it adds a second signal to maintain.
3. Final decision: recognize that a **health-gated sidecar pull already IS the
   per-pod READY event**, so `_seen_endpoints` is itself the ready timestamp. No
   `ready_ts` map, poller, grace timer, or discovery set is needed. Change 2
   documents this (no rename) and plants the invariant back-references. See §8.
