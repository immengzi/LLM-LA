# KV Cache Flow

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

The KV cache flow spans three processes — vLLM, the sidecar, and the router — and one external service. The goal is to route each incoming request to the pod most likely to already have its prompt prefix cached in GPU memory, avoiding redundant KV recomputation.

```
                    ┌─────────────────────────────────────────────────┐
                    │  vLLM pod                                        │
                    │                                                  │
                    │  GPU allocates / evicts KV blocks                │
                    │       │                                          │
                    │       │ ZMQ PUB  port 5557  topic="kv@"         │
                    │       ▼                                          │
                    │  [BlockStored | BlockRemoved | AllBlocksCleared] │
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
                              │      Redis       │
                              │                  │
                              │  kvblock:{H}     │
                              │  podblocks:{pod} │
                              │  kvblocks        │
                              └────────┬─────────┘
                                       │ async scan_iter
                                       ▼
                    ┌──────────────────────────────────────────────────┐
                    │  Router  KVWatcher                               │
                    │                                                  │
                    │  _BLOCK_OWNERS  { H: {pod-a, pod-b}, ... }      │
                    │  _REQ_BLOCKS    { req_id: [H1,H2,H3], ... }     │
                    └──────────────────┬───────────────────────────────┘
                                       │ prefix_len scoring at pull time
                                       ▼
                    ┌──────────────────────────────────────────────────┐
                    │  pull_for_endpoint                               │
                    │                                                  │
                    │  tier kv=N  → [ req-A ]                         │
                    │  tier kv=M  → [ req-B, req-C ]                  │
                    │  tier kv=0  → [ req-D ]                         │
                    └──────────────────────────────────────────────────┘
```

There is also a lateral flow at admit time: when a request arrives at `/enqueue`, the router calls the `prefix-hash-service` to compute the block hashes for that prompt. These are stored in `_REQ_BLOCKS` and are the basis for `prefix_len` scoring later.

```
  client ──► /enqueue ──► prefix-hash-service ──► _REQ_BLOCKS[req_id]
```

---

## Components Involved

| Component | Role in KV flow |
|-----------|----------------|
| `vLLM` | Emits `BlockStored`, `BlockRemoved`, `AllBlocksCleared` events over ZMQ |
| `KVSubscriber` (sidecar) | ZMQ SUB thread; decodes events; writes to Redis |
| `Redis` | Shared state store for block ownership |
| `KVWatcher` (router) | Scans Redis; maintains `_BLOCK_OWNERS` in-memory |
| `prefix-hash-service` | Computes block hashes for incoming prompts |
| `kv_aware.py` (router) | Stores `_REQ_BLOCKS`, `_BLOCK_OWNERS`; implements `prefix_len` |
| `router_state.py` (router) | Calls `_maybe_register_kv_blocks` at admit; calls `pull_for_endpoint` |

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

### KVWatcher

`KVWatcher` is an asyncio background task running in the router process. It wakes every `KV_WATCH_INTERVAL_S` seconds and calls `_scan_once()`.

```python
# kv_watcher.py (simplified)
async def _scan_once():
    async for key in redis.scan_iter(f"{model}:kvblock:*", count=KV_WATCH_MAX_KEYS):
        block_hash = int(key.split(":")[-1])
        pod_owners = await redis.hgetall(key)        # { pod_name: timestamp }
        ep_owners  = [pod_to_endpoint[p]             # translate pod → endpoint URL
                      for p in pod_owners if p in pod_to_endpoint]
        register_block_owners(block_hash, ep_owners)
```

### In-memory state

`kv_aware.py` maintains two module-level dicts:

```python
_BLOCK_OWNERS: dict[int, set[str]]
# block_hash → set of endpoint addresses that own it
# e.g. { 12345: {"http://10.0.0.1:8200", "http://10.0.0.2:8200"} }

_REQ_BLOCKS: dict[str, list[int]]
# req_id → ordered list of block hashes for that request's prompt prefix
# e.g. { "a3f9...": [H1, H2, H3, H4] }
```

`_BLOCK_OWNERS` is written exclusively by `KVWatcher`. `_REQ_BLOCKS` is written at admit time and cleaned up after result delivery.

### Pod discovery

`KVWatcher` also runs a pod discovery loop every `KV_DISCOVERY_INTERVAL_S` seconds. It queries the Kubernetes API for pods matching `LABEL_SELECTOR` in `NAMESPACE` and builds a `pod_name → endpoint_url` map. This map is what translates Redis pod names (plain container names) to routable endpoint addresses.

If a pod is not yet in the discovery map when its blocks appear in Redis, those blocks are skipped for that scan cycle and will be picked up once discovery refreshes.

---

## Plane 3: Prefix Hash Registration

When a request arrives at `/enqueue`, `_maybe_register_kv_blocks` is called before the request enters the deque:

```
POST /enqueue { prompt }
       │
       ▼
_maybe_register_kv_blocks(req_id, prompt)
       │
       │  POST http://prefix-hash-service:9095/compute_hashes
       │       { "prompt": "..." }
       │
       ◄── { "block_hashes": [H1, H2, H3] }
       │
       ▼
register_request_blocks(req_id, [H1, H2, H3])
  _REQ_BLOCKS[req_id] = [H1, H2, H3]
       │
       ▼
request appended to deque
```

The call has a hard timeout of `HASH_TIMEOUT_S` (default 2 seconds). On any failure — timeout, connection error, non-200 response — the exception is caught and the request proceeds with `_REQ_BLOCKS[req_id]` absent or empty. `prefix_len` will return 0 for all endpoints for this request, placing it in `tier kv=0`. This is the fail-open behavior: KV routing degrades gracefully to length-aware ordering.

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

`kvblocks` exists as a scan optimization. Rather than issuing `SCAN kvblock:*` on every watcher interval, the watcher can read the index from a single HGETALL. `KV_WATCH_MAX_KEYS` caps how many entries are consumed per cycle.

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

### HTTP: router → prefix-hash-service

```
POST /compute_hashes
Content-Type: application/json

{ "prompt": "<raw prompt string>" }

200 OK
{ "block_hashes": [<int>, <int>, ...] }
```

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
_REQ_BLOCKS hashes:    produced by prefix-hash-service from the raw prompt string
```

For a match to be valid, `prefix-hash-service` must replicate vLLM's internal block hashing exactly: same tokenizer, same block size, same rolling hash algorithm. If they diverge, the system produces silent routing errors in both directions.

| Mismatch type | Effect |
|---------------|--------|
| False positive | Router routes to a pod that does not have the prefix cached; vLLM recomputes the full prefix |
| False negative | Router misses a cache-warm pod; routes to a cold pod instead |

Neither error is detectable at the routing layer. There is no validation or verification of hash agreement in this codebase. This is the single most critical correctness dependency in the KV flow.

---

## Known Limitations

**Stale ownership window.** The KVWatcher scans Redis every `KV_WATCH_INTERVAL_S` (default 1s). Between scans, blocks evicted from vLLM are not reflected in `_BLOCK_OWNERS`. The router may route to a pod that no longer has the blocks, resulting in a silent cache miss.

**No token-level granularity.** `prefix_len` counts blocks, not tokens. Whether a block represents 16 or 64 tokens is not tracked. Tier comparisons across requests with different block sizes or token densities may not accurately reflect proportional cache reuse benefit.

**No session affinity.** Multi-turn requests in the same conversation are not correlated. The KV tier mechanism will naturally tend to route follow-up requests to the pod that computed previous turns (because those block hashes will match), but this is incidental. There is no explicit session tracking or sticky routing guarantee.

**Fail-open on hash service failure.** If `prefix-hash-service` is down or slow, `_REQ_BLOCKS` is never populated and all requests are routed without KV hints. This is correct behavior but means the KV cache benefit disappears silently under hash service degradation.
