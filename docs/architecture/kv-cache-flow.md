# KV Cache Flow

> For a high-level overview of the routing strategies, see
> [router-strategies.md](router-strategies.md).
>
> For a production incident where the prefix-cache hit rate collapsed under peak
> load (GPU KV saturation + a full/lossy Mooncake remote tier), and the local-cache-first
> fix, see [internal/kv-cache-hit-rate-collapse.md](../internal/kv-cache-hit-rate-collapse.md).

## Table of Contents

1. [Overview](#overview)
2. [Components Involved](#components-involved)
3. [Plane 1: KV Block Event Capture](#plane-1-kv-block-event-capture)
4. [Plane 2: State Propagation to Router](#plane-2-state-propagation-to-router)
5. [Plane 3: Prefix Hash Registration](#plane-3-prefix-hash-registration)
6. [Plane 4: Scheduling Decision](#plane-4-scheduling-decision)
7. [Redis Key Schema](#redis-key-schema)
8. [Wire Formats](#wire-formats)
9. [Scoring Example](#scoring-example)
10. [Hash Correctness Dependency](#hash-correctness-dependency)
11. [Known Limitations](#known-limitations)

---

## Overview

The KV cache flow spans three processes — vLLM, the sidecar, and the router. The goal is to route each incoming request to the pod most likely to already have its prompt prefix cached in GPU memory, avoiding redundant KV recomputation.

There are two independent halves: (1) **ownership** — which pod holds which KV blocks — is learned from vLLM KV events via the sidecar and Redis; (2) **request identity** — the block hashes of an incoming prompt — is computed by the router. By default both routers compute request hashes with the same `router/prefix_hash.py` (the Python router in-process; the Go gateway via the identical code inside its own container). A legacy `KV_HASH_SOURCE=external` mode instead calls the standalone `vllm-cpu-hash` service. See [prefix-hash.md](prefix-hash.md) for the hashing details.

```
                    ┌─────────────────────────────────────────────────┐
                    │  vLLM pod                                       │
                    │                                                 │
                    │  GPU allocates / evicts KV blocks               │
                    │       │                                         │
                    │       │ ZMQ PUB  port 5557  topic="kv@"         │
                    │       ▼                                         │
                    │  [BlockStored | BlockRemoved | AllBlocksCleared]│
                    └──────────────────┬──────────────────────────────┘
                                       │ ZMQ SUB (daemon thread)
                                       ▼
                    ┌──────────────────────────────────────────────────┐
                    │  Sidecar  KVSubscriber                           │
                    │                                                  │
                    │  decode msgpack KVEventBatch                     │
                    │  pipeline.execute() → Redis                      │
                    └──────────────────┬───────────────────────────────┘
                                       │ HSET / SADD
                                       ▼
                              ┌─────────────────┐
                              │      Redis      │
                              │                 │
                              │ {MODEL}:kvblock:{H}    │
                              │ {MODEL}:podblocks:{pod}│
                              │ {MODEL}:kvblocks       │
                              └────────┬────────┘
                                       │ async scan_iter
                                       ▼
                    ┌──────────────────────────────────────────────────┐
                    │  Router  KVWatcher                               │
                    │                                                  │
                    │  _BLOCK_OWNERS  { H: {pod-a, pod-b}, ... }       │
                    │  _REQ_BLOCKS    { req_id: [H1,H2,H3], ... }      │
                    └──────────────────┬───────────────────────────────┘
                                       │ prefix_len scoring at pull time
                                       ▼
                    ┌──────────────────────────────────────────────────┐
                    │  pull_for_endpoint                               │
                    │                                                  │
                    │  tier kv=N  → [ req-A ]                          │
                    │  tier kv=M  → [ req-B, req-C ]                   │
                    │  tier kv=0  → [ req-D ]                          │
                    └──────────────────────────────────────────────────┘
```

There is also a lateral flow at admit time: when a request is enqueued, the router computes the block hashes for that prompt and stores them in `_REQ_BLOCKS`, the basis for `prefix_len` scoring later. By default (`KV_HASH_SOURCE=inline`) both routers hash with the same `prefix_hash.py` — the Python router in-process, the Go gateway via the identical code shipped inside its container on `127.0.0.1`. (Legacy opt-in `KV_HASH_SOURCE=external` calls the standalone `vllm-cpu-hash` pod instead.)

```
  Python router (inline):  enqueue ──► prefix_hash.py (in-process) ──────► _REQ_BLOCKS[req_id]
  Go gateway   (inline):   enqueue ──► prefix_hash.py @127.0.0.1:9095 ───► _REQ_BLOCKS[req_id]
  either       (external): enqueue ──► vllm-cpu-hash /compute_hashes ────► _REQ_BLOCKS[req_id]
```

---

## Components Involved

| Component | Role in KV flow |
|-----------|----------------|
| `vLLM` | Emits `BlockStored`, `BlockRemoved`, `AllBlocksCleared` events over ZMQ |
| `KVSubscriber` (sidecar) | ZMQ SUB thread; decodes events; writes to Redis |
| `Redis` | Shared state store for block ownership |
| `owner_lookup.py` (router) | Default owner source: targeted per-request Redis `HGETALL` at admit; fills `_REQ_OWNERS` (`KV_OWNER_SOURCE=lookup`) |
| `KVWatcher` (router) | Legacy owner source + pod discovery: scans Redis into `_BLOCK_OWNERS` (`KV_OWNER_SOURCE=watcher`) |
| `prefix_hash.py` (shared) | Computes request block hashes; runs in-process (Python router) and in-container on `127.0.0.1` (Go gateway) in the default `inline` mode |
| `vllm-cpu-hash` pod (legacy) | Standalone HTTP hasher, used only when `KV_HASH_SOURCE=external`; auto-deployed in that mode |
| `kv_aware.py` (router) | Stores `_REQ_BLOCKS`, `_BLOCK_OWNERS`; implements `prefix_len` |
| `api.py` / `router_state.py` (router) | `_maybe_register_kv_blocks` at admit; `pull_for_endpoint` at dispatch |

---

## Plane 1: KV Block Event Capture

### ZMQ wire format

vLLM publishes three-frame ZMQ messages:

```
frame 0: topic bytes  (e.g. b"kv@")
frame 1: seq bytes    (8-byte big-endian sequence number)
frame 2: payload      (msgpack-encoded KVEventBatch)
```

The sidecar subscribes with `setsockopt_string(zmq.SUBSCRIBE, "kv@")`, which matches all topics with that prefix.

### Message schema

`KVEventBatch` is decoded with `msgspec.msgpack.Decoder`. All structs use `array_like=True` (positional fields, not named keys) to minimize wire size.

```python
class KVEventBatch(EventBatch):
    ts: float
    events: list[BlockStored | BlockRemoved | AllBlocksCleared]

class BlockStored(KVCacheEvent):
    block_hashes:      list[int]
    parent_block_hash: int | None
    token_ids:         list[int]
    block_size:        int
    lora_id:           int | None

class BlockRemoved(KVCacheEvent):
    block_hashes: list[int]

class AllBlocksCleared(KVCacheEvent):
    pass
```

### Redis writes per event type

Each batch may contain multiple events. The subscriber processes all events and executes a single Redis pipeline per batch.

```
BlockStored(block_hashes=[H1, H2]):
    HSET {MODEL}:kvblock:H1   pod_name → unix_timestamp
    SADD {MODEL}:podblocks:{pod_name}   H1
    HSET {MODEL}:kvblocks   H1 → {MODEL}:kvblock:H1
    HSET {MODEL}:kvblock:H2   pod_name → unix_timestamp
    SADD {MODEL}:podblocks:{pod_name}   H2
    HSET {MODEL}:kvblocks   H2 → {MODEL}:kvblock:H2

BlockRemoved(block_hashes=[H1]):
    HDEL {MODEL}:kvblock:H1   pod_name
    SREM {MODEL}:podblocks:{pod_name}   H1

AllBlocksCleared:
    HGETALL {MODEL}:podblocks:{pod_name}  → list of all owned hashes
    for each hash H:
        HDEL {MODEL}:kvblock:H   pod_name
    DEL {MODEL}:podblocks:{pod_name}
```

`{MODEL}` is `MODEL_NAME_REDIS` from the sidecar config, which must match `MODEL_NAME` in the router config.

### Thread model

`KVSubscriber` runs as a single daemon thread started in `sidecar/main.py`. It is completely independent of the `VLLMWorker` threads and the HTTP server thread. There is no shared state between them; the subscriber's only output is Redis writes.

```
sidecar process
  ├── thread: uvicorn HTTP server
  ├── thread: KVSubscriber._loop()       ← ZMQ recv + Redis write
  ├── thread: VLLMWorker[0]._loop()
  ├── thread: VLLMWorker[1]._loop()
  │   ...
  └── thread: ResultPoster._loop()
```

---

## Plane 2: State Propagation to Router

### Owner source: `lookup` (default) vs `watcher` (legacy)

Block ownership can reach the router two ways, selected by `KV_OWNER_SOURCE`
(Helm `router.ownerSource`, config `router_owner_source`):

- **`lookup` (default).** At admit time the router issues a targeted, pipelined
  Redis `HGETALL` for the request's *own* block hashes (`owner_lookup.py`, capped
  by `KV_LOOKUP_MAX_BLOCKS` / `router.lookupMaxBlocks`, default 512) and stores
  the result in a per-request owner map (`_REQ_OWNERS`). `prefix_len` scores
  against this fresh, request-scoped view, so `prefix`/`both` routing and the
  `kv_hit` metric are truthful. The per-request state is dropped at completion.
- **`watcher` (legacy).** The background `KVWatcher` blind-scans Redis into a
  shared `_BLOCK_OWNERS` map. Retained for backward compatibility; it can starve
  under load (bounded by `KV_WATCH_MAX_KEYS`) and under-count `kv_hit`. Used only
  when `KV_OWNER_SOURCE=watcher`, and as the fallback map when a per-request
  lookup returned nothing.

The `KVWatcher` and pod-discovery loops below still run in both modes (pod
discovery is always needed to translate Redis pod names to endpoint URLs).

### KVWatcher

`KVWatcher` runs in a daemon **thread** in the router process (via `asyncio.run()`), not as an asyncio task on the main loop. It wakes every `KV_WATCH_INTERVAL_S` seconds and calls `_scan_once()`.

```python
# kv_watcher.py (simplified)
async def _scan_once():
    async for key in redis.scan_iter(f"{model}:kvblock:*", count=KV_WATCH_MAX_KEYS):
        block_hash = int(key.split(":")[-1])         # Redis stores the hash as a decimal string
        pod_owners = await redis.hgetall(key)        # { pod_name: timestamp }
        # owners are pod names — the same identifier the sidecar reports as its /pull endpoint
        register_block_owners(block_hash, list(pod_owners.keys()))
```

The `{model}:kvblocks` index is written by the sidecar but the watcher does not read it today — it always scans `{model}:kvblock:*`.

### In-memory state

`kv_aware.py` maintains two module-level dicts:

```python
_BLOCK_OWNERS: dict[int, set[str]]
# block_hash → set of pod names (endpoints) that own it
# e.g. { 12345: {"vllm-qwen-abc12", "vllm-qwen-def34"} }

_REQ_BLOCKS: dict[str, list[int]]
# req_id → ordered list of block hashes for that request's prompt prefix
# e.g. { "a3f9...": [H1, H2, H3, H4] }
```

`_BLOCK_OWNERS` is written exclusively by `KVWatcher` (watcher mode). In the default `lookup` mode the router instead fills a per-request `_REQ_OWNERS` map (`block_hash → set of pods`) from the targeted `owner_lookup.py` fetch, and `prefix_len` prefers it over `_BLOCK_OWNERS`. `_REQ_BLOCKS` (and `_REQ_OWNERS`) are written at admit time and dropped after result delivery (`drop_request`).

### Pod discovery

`KVWatcher` also runs a pod discovery loop every `KV_DISCOVERY_INTERVAL_S` seconds. It queries the Kubernetes API for pods matching `LABEL_SELECTOR` in `NAMESPACE` and builds a `pod_name → endpoint_url` map. This map is what translates Redis pod names (plain container names) to routable endpoint addresses.

If a pod is not yet in the discovery map when its blocks appear in Redis, those blocks are skipped for that scan cycle and will be picked up once discovery refreshes.

---

## Plane 3: Prefix Hash Registration

When a request is enqueued, `_maybe_register_kv_blocks` (in `router/api.py`) computes its block hashes before it is scheduled. In the **Python router** this is inline — no network call:

```
enqueue { messages | prompt, tools }
       │
       ▼
_maybe_register_kv_blocks(req_id, prompt, messages, tools)   # api.py
       │
       │  prefix_hash.compute_request_block_hashes_int(...)   # inline, in-process
       │    - render chat template (add_generation_prompt=True)
       │    - canonicalize tools (KV_CANONICALISE_TOOLS)
       │    - chained sha256(cbor2) block hashes, low-64-bit ints
       │
       ▼
register_request_blocks(req_id, [H1, H2, H3])
  _REQ_BLOCKS[req_id] = [H1, H2, H3]
```

The **Go gateway** does the equivalent step by POSTing `{ messages | prompt, tools }` to its in-container hasher at `127.0.0.1:9095` (which runs the same `prefix_hash.py`) and registering the returned hashes. In the legacy `external` mode it POSTs a flat `{ "prompt": "..." }` to the standalone `vllm-cpu-hash` service instead.

Both are fail-open: if hashing fails (inline exception, or a hasher timeout/error), the request proceeds with `_REQ_BLOCKS[req_id]` absent or empty, `prefix_len` returns 0 for all endpoints, and the request lands in `tier kv=0`. KV routing degrades gracefully to length-aware ordering.

---

## Plane 4: Scheduling Decision

### Entry point

When a sidecar calls `POST /pull { endpoint, want }`, the router calls `pull_for_endpoint(endpoint, want)`.

### Pool sampling

The router does not score the entire deque. It drains up to `want × POOL_FACTOR` items from the head of the deque into a local pool. Leftovers are returned to the head after selection. `POOL_FACTOR` (default 4) controls the trade-off between scheduling quality and the number of items temporarily dequeued per pull.

```
deque: [ req-A, req-B, req-C, req-D, req-E, req-F, req-G, req-H, ... ]
                                                                   ↑
want=2, POOL_FACTOR=4 → drain first 8 into pool
```

### prefix_len

For each candidate in the pool, the router calls `prefix_len(endpoint, req_id)`:

```python
def prefix_len(endpoint: str, req_id: str) -> int:
    blocks = _REQ_BLOCKS.get(req_id, [])
    count = 0
    for h in blocks:
        if endpoint not in _BLOCK_OWNERS.get(h, set()):
            break          # stop at first miss — prefix must be contiguous
        count += 1
    return count
```

The stop-at-first-miss rule is critical. vLLM's prefix caching requires a contiguous prefix. Owning H1, H2, H4 but not H3 provides no benefit beyond H2, because the KV block for H3 was never computed and the computation cannot skip ahead to H4.

### Tiering and sorting

Candidates are grouped by `kv_hits` descending. Within each tier, if `LEN_AWARE=true`, candidates are sorted by predicted output length according to `LEN_POLICY` (`short_first` or `long_first`). `SimpleLengthPredictor` uses prompt character count as a proxy for output length.

```
pool after scoring:

  req-A  kv_hits=3
  req-B  kv_hits=1
  req-C  kv_hits=3
  req-D  kv_hits=0
  req-E  kv_hits=2
  req-F  kv_hits=0
  req-G  kv_hits=1
  req-H  kv_hits=2

after tiering + short_first within tier:

  tier 3: [ req-A(short), req-C(long) ]
  tier 2: [ req-H(short), req-E(long) ]
  tier 1: [ req-B(short), req-G(long) ]
  tier 0: [ req-D(short), req-F(long) ]

dispatched (want=2): req-A, req-C
requeued at head:    req-H, req-E, req-B, req-G, req-D, req-F
```

---

## Redis Key Schema

| Key pattern | Type | Contents |
|-------------|------|----------|
| `{MODEL}:kvblock:{block_hash}` | HASH | `{ pod_name: unix_timestamp }` |
| `{MODEL}:podblocks:{pod_name}` | SET | `{ block_hash, block_hash, ... }` |
| `{MODEL}:kvblocks` | HASH | `{ block_hash: key_name }` (global index) |

`{MODEL}:kvblocks` is written by the sidecar as a global index, intended as a future scan optimization. The watcher does **not** read it today — it scans `{MODEL}:kvblock:*` each interval, with `KV_WATCH_MAX_KEYS` capping how many keys are consumed per cycle.

Write ownership: sidecars write, router only reads.

---

## Wire Formats

### ZMQ message (vLLM → sidecar)

```
┌──────────────────────────────────────────────────────┐
│ frame 0  │  b"kv@"                                    │  topic
├──────────────────────────────────────────────────────┤
│ frame 1  │  \x00\x00\x00\x00\x00\x00\x00\x01        │  seq (big-endian uint64)
├──────────────────────────────────────────────────────┤
│ frame 2  │  msgpack(KVEventBatch)                     │  payload
└──────────────────────────────────────────────────────┘
```

### HTTP: gateway → KV hasher (`/compute_hashes`)

Default (`inline`): the Go gateway calls its in-container hasher at
`127.0.0.1:9095` (same `prefix_hash.py` as the Python router), sending structured
inputs. The Python router skips HTTP entirely and hashes in-process.

```
POST /compute_hashes
Content-Type: application/json

{ "messages": [...], "tools": [...] }   # inline; or { "prompt": "..." }

200 OK
{ "block_hashes": [<int>, <int>, ...] }
```

Legacy (`external`): the same call goes to the standalone `vllm-cpu-hash`
service, with the Go gateway sending only `{ "prompt": "..." }`.

### HTTP: sidecar → router `/pull`

```
POST /pull
{ "endpoint": "pod-a", "want": 4 }

200 OK
{
  "items": [
    { "req_id": "...", "prompt": "...", "t_enq_client": 1712000000.0, "meta": {} },
    ...
  ]
}
```

### HTTP: sidecar → router `/result`

```
POST /result
{
  "req_id": "...",
  "result": {
    "output": "...",
    "finish_reason": "stop",
    "latency_s": 1.23,
    "usage": { "prompt_tokens": 42, "completion_tokens": 128, "total_tokens": 170 }
  }
}
```

---

## Scoring Example

Three pods, four requests, all pulling to `pod-a`.

```
_BLOCK_OWNERS:
    H1 → { pod-a, pod-b }
    H2 → { pod-a }
    H3 → { pod-b }          ← pod-a does NOT own H3
    H4 → { pod-a }
    H5 → { pod-c }
    H6 → { pod-c }

_REQ_BLOCKS:
    req-X → [H1, H2, H3, H4]
    req-Y → [H1, H2]
    req-Z → [H5, H6]
    req-W → [H1, H2, H3]

prefix_len("pod-a", req-X):
    H1 → owned    count=1
    H2 → owned    count=2
    H3 → missing  STOP
    result: 2     (H4 is owned but unreachable; prefix broken at H3)

prefix_len("pod-a", req-Y):
    H1 → owned    count=1
    H2 → owned    count=2
    result: 2     (full match)

prefix_len("pod-a", req-Z):
    H5 → missing  STOP
    result: 0

prefix_len("pod-a", req-W):
    H1 → owned    count=1
    H2 → owned    count=2
    H3 → missing  STOP
    result: 2

Tiering (short_first within tier, want=2):

    tier kv=2:  [ req-Y(2 blocks), req-W(3 blocks), req-X(4 blocks) ]
    tier kv=0:  [ req-Z ]

Dispatched to pod-a: req-Y, req-W
Requeued at head:    req-X, req-Z
```

Note that `req-Z` would be a full match for `pod-c`. If `pod-c` pulls next with `want=1`, `req-Z` would land in `tier kv=2` for that endpoint and be dispatched first.

---

## Hash Correctness Dependency

The routing benefit depends entirely on the hashes in `_REQ_BLOCKS` matching the hashes in `_BLOCK_OWNERS`. These come from two independent sources:

```
_BLOCK_OWNERS hashes:  produced by vLLM internally, observed via BlockStored events
_REQ_BLOCKS hashes:    produced by the inline prefix_hash.py (default) or the legacy external service
```

For a match to be valid, the request-hash producer must replicate vLLM's block hashing exactly: same **tokenizer**, same **block size** (`KV_BLOCK_SIZE` vs vLLM `--block-size`), same **`PYTHONHASHSEED`** (so `NONE_HASH` matches), same chained `sha256(cbor2)` algorithm, and the same logical request shape after normalization/tool canonicalization. If they diverge, the system produces silent routing errors in both directions.

| Mismatch type | Effect |
|---------------|--------|
| False positive | Router routes to a pod that does not have the prefix cached; vLLM recomputes the full prefix |
| False negative | Router misses a cache-warm pod; routes to a cold pod instead |

Neither error is detectable at the routing layer. There is no validation or verification of hash agreement in this codebase. This is the single most critical correctness dependency in the KV flow.

---

## Known Limitations

**Stale ownership window (watcher mode only).** With `KV_OWNER_SOURCE=watcher`, the KVWatcher scans Redis every `KV_WATCH_INTERVAL_S` (default 1s). Between scans, blocks evicted from vLLM are not reflected in `_BLOCK_OWNERS`, so the router may route to a pod that no longer has the blocks (silent cache miss). The default `lookup` mode avoids this by reading fresh ownership per request at admit; only the sub-millisecond gap between the lookup and dispatch remains.

**No token-level granularity.** `prefix_len` counts blocks, not tokens. Whether a block represents 16 or 64 tokens is not tracked. Tier comparisons across requests with different block sizes or token densities may not accurately reflect proportional cache reuse benefit.

**Session affinity is now explicit (optional).** The KV tier mechanism naturally tends to route follow-up turns to the pod that computed previous turns (their block hashes match), but for a stronger guarantee there is now an explicit conversation key-affinity feature (off by default). See [key-affinity.md](key-affinity.md).

**Fail-open on hashing failure.** If request hashing fails — a tokenizer error in the in-container/in-process hasher, or a down/slow legacy service in `external` mode — `_REQ_BLOCKS` is not populated and those requests route without KV hints. This is correct behavior but means the KV cache benefit disappears silently under that degradation.

**Mode divergence.** `external` (legacy) uses a different hashing implementation than `inline`; the two are not guaranteed to agree. Pick one mode per cluster.
