# LLM Inference Routing Algorithm Comparison

## Pull-Based Prefix-KV vs AIBrix Prefix-Cache vs AIBrix Preble

---

## Background

### Pull-Based Method

The system maintains a single centralized deque. Workers are not assigned work — they declare capacity by issuing a `/pull` request carrying their endpoint identity and the number of free slots (`want`). The router responds by scanning the front `want × POOL_FACTOR` items from the deque, scoring each against the requesting endpoint, and returning the top `want`.

Scoring is two-dimensional. The primary key is KV prefix hit count: for each candidate request, the router walks its pre-computed block hash chain and counts how many leading blocks the requesting worker currently owns in Redis. This count is populated by real vLLM block events — sidecars subscribe to vLLM's ZMQ stream and write `BlockStored`/`BlockRemoved` events to Redis, so the index always reflects actual GPU memory state. The secondary key is predicted output length, applied within each KV tier to avoid head-of-line blocking.

Admission control is implicit. A worker that is at `BATCH_SIZE` capacity stops pulling. The queue grows at the router, not at the workers. The result is that routing decisions are always made relative to a specific, available worker with a known, accurate KV state.

---

### AIBrix Native Prefix-Cache

The gateway tokenizes each incoming request on the critical path, then walks a chain-hash prefix index to find which pods have seen the longest matching prefix. The index (`PrefixHashTable`) is a gateway-local LRU populated by routing decisions: when the gateway routes request R to pod P, it records the prefix hashes as owned by P. There is no eviction signal from the GPU — the index only learns from routing choices, not from actual memory events.

Before prefix matching, the gateway applies a load-imbalance pre-filter: if the spread between the maximum and minimum in-flight request counts across pods exceeds a threshold (default 8), the candidate set is restricted to the least-loaded pods. After prefix matching, pods whose running count exceeds `mean + stddev × factor` are excluded. The highest match-percentage pod among the remaining candidates is selected. If no match exists, the least-request-count pod is chosen as fallback.

The core algorithmic weakness is that the index can only grow stale in one direction — it records blocks as cached but cannot observe evictions. Under memory pressure a pod may have long since evicted a prefix, yet the gateway continues to route toward it believing it will get a cache hit.

---

### AIBrix Prefix-Cache-Preble

Preble replaces the match-percentage selection criterion with a GPU cost model. After prefix matching, instead of picking the highest-percentage pod, it estimates the total execution cost of routing to each candidate. Cost has two components: prefill cost, computed as a polynomial function of the number of batched tokens (calibrated separately for A6000 and V100 hardware), and decode cost, estimated as median throughput per token times the expected decode length. The pod with minimum total cost is selected.

The cost model also feeds a sliding-window histogram that tracks per-node token counts, hit rates, and decode lengths over time. This histogram is used to estimate the current decode backlog on each pod and feeds back into the cost estimate, making the selection sensitive to both the prefix hit and the current load shape rather than just the raw request count.

Preble inherits the same index limitations as the native variant — the prefix index is still populated by routing decisions, not real block events. However, because it weights the value of a cache hit by the actual compute cost saved (rather than treating all hits equally), it makes better decisions on heterogeneous hardware and under mixed short/long request traffic.

---

## Full Algorithmic Comparison

### Core Scheduling Model

| Dimension | Pull-based | AIBrix native | AIBrix Preble |
|---|---|---|---|
| Dispatch trigger | Worker declares capacity | Request arrives at gateway | Request arrives at gateway |
| Routing decision relative to | Specific available worker | All ready pods | All ready pods |
| Admission control | Implicit — full workers stop pulling | Explicit load-imbalance gate | Explicit load-imbalance gate |
| Thundering herd risk | None | Yes — concurrent decisions can pile onto same pod | Yes — same as native |
| Queue model | Centralized deque, grows at router | None — immediate dispatch | None — immediate dispatch |
| Dispatch latency | Idle-poll gap (up to `PULL_INTERVAL_S × BATCH_SIZE`) | Near-zero | Near-zero |

---

### KV Cache Index Accuracy

| Dimension | Pull-based | AIBrix native | AIBrix Preble |
|---|---|---|---|
| Index populated by | Real vLLM block events (ZMQ → Redis) | Routing decisions only | Routing decisions only |
| Eviction awareness | Yes — `BlockRemoved` propagates to index | No | No |
| Stale hit risk | Low (bounded by Redis poll interval ~1 s) | High under memory pressure | High under memory pressure |
| Block size alignment | Character-hash (may not align to vLLM token blocks) | 4 tokens/block | 4 tokens/block |
| **Winner** | **Pull-based** | — | — |

---

### Selection Algorithm

| Dimension | Pull-based | AIBrix native | AIBrix Preble |
|---|---|---|---|
| Primary selection criterion | KV hit count (absolute block count) | KV match percentage | Minimum estimated GPU cost |
| Secondary selection criterion | Output length within KV tier | None | Current decode backlog (histogram) |
| Overload exclusion | Worker never exceeds `BATCH_SIZE` | `mean + stddev × factor` gate | `mean + stddev × factor` gate |
| Hardware heterogeneity awareness | Not modeled | Not modeled | Yes — separate cost polynomials per GPU class |
| Length awareness | Yes — `short_first` or `long_first` policy | No | Partially — decode cost term penalizes long backlogs |
| Scan scope | Front `want × POOL_FACTOR` items in deque | All ready pods | All ready pods |
| Fallback | Unchosen items returned to deque front | Least-request-count pod | Least-cost pod (no prefix match) |

---

## Behavior Under Different Workloads

### Stateless Burst — many independent short requests, no shared prefix

| Metric | Pull-based | AIBrix native | AIBrix Preble |
|---|---|---|---|
| Dispatch latency | Hurt by idle-poll gap | Near-zero | Near-zero |
| Load distribution | Even by construction | Least-request fallback | Cost-balanced fallback |
| KV scoring contribution | None | None | None |
| **Winner** | **AIBrix native / Preble (tied)** — latency advantage, KV irrelevant |

---

### Long Shared Prefix — RAG, system prompt reuse

| Metric | Pull-based | AIBrix native | AIBrix Preble |
|---|---|---|---|
| Cache hit accuracy | High — eviction-aware | Overestimates under memory pressure | Overestimates under memory pressure |
| Routing to correct pod | Worker with real hits wins naturally | May route to pod that has evicted the blocks | May route to pod that has evicted the blocks |
| Memory pressure handling | Pod stops pulling — no new work stacks on evicting pod | Gateway continues routing to pod it believes has blocks | Same weakness, partially offset by cost term |
| **Winner** | **Pull-based** | | |

---

### Agentic Multi-Stage Pipeline — sequential dependent stages, same session

| Metric | Pull-based | AIBrix native | AIBrix Preble |
|---|---|---|---|
| Affinity mechanism | Emergent — worker accumulates blocks and wins future scoring | Declarative — index must stay accurate across stages | Declarative — same, with cost weighting |
| Cross-stage coordination needed | None | Accurate index required at each stage | Accurate index required at each stage |
| Resilience to eviction between stages | Worker that evicted scores lower and yields | Gateway may misroute stage N if index is stale | Same weakness |
| **Winner** | **Pull-based** — affinity is structural, not dependent on index accuracy | | |

---

### Mixed Length — short and long requests concurrently

| Metric | Pull-based | AIBrix native | AIBrix Preble |
|---|---|---|---|
| Head-of-line blocking | Mitigated by length-aware secondary sort | Unmitigated | Partially mitigated via decode cost term |
| Short request starvation risk | Low | High if long requests dominate | Reduced — cost model discourages routing shorts into burdened pods |
| **Winner** | **Pull-based** (scheduling); Preble comparable | | |

---

### Memory Pressure / High Eviction Rate

| Metric | Pull-based | AIBrix native | AIBrix Preble |
|---|---|---|---|
| Routing correctness under eviction | High — index reflects real state | Low — index cannot observe evictions | Low — same index limitation |
| Worker behavior | Slows pulling, naturally throttles | Continues dispatching at full rate | Continues dispatching at full rate |
| **Winner** | **Pull-based** | | |

---

### Heterogeneous GPU Hardware

| Metric | Pull-based | AIBrix native | AIBrix Preble |
|---|---|---|---|
| Hardware-aware routing | No | No | Yes — cost polynomials calibrated per GPU class |
| Avoids over-routing to slow pods | Only via implicit throughput feedback | Only via request-count heuristic | Yes — slower pods have higher cost and receive proportionally less work |
| **Winner** | **AIBrix Preble** | | |

---

### Low-Latency Interactive — single-turn, no prefix reuse

| Metric | Pull-based | AIBrix native | AIBrix Preble |
|---|---|---|---|
| Time-to-first-token | Hurt by pull idle-poll gap | Minimal | Minimal |
| KV benefit to offset latency cost | None | None | None |
| **Winner** | **AIBrix native / Preble (tied)** | | |

---

## Pros and Cons

### Pull-based

**Pros**

- KV index is always ground-truth accurate — reflects real GPU memory state via eviction events
- Admission control and dispatch are unified with no thundering-herd path
- Session and agentic affinity is emergent and requires no cross-worker coordination
- Length-aware secondary sort reduces head-of-line blocking
- Worker self-limiting naturally handles memory pressure without gateway-side tuning

**Cons**

- Idle-poll latency penalizes workloads with no prefix reuse
- Block hashing is character-based and may not align to vLLM token-block boundaries
- No hardware-cost model — all pods are treated as equivalent
- No aggregate cache-hit-rate metric; quality visible only in per-request traces

---

### AIBrix Native Prefix-Cache

**Pros**

- Zero dispatch latency — requests are routed immediately on arrival
- Explicit load threshold is tunable and observable
- Aggregate routing quality metric (`routing_decisions_total` by match band) gives direct cache-hit visibility
- Simple and low operational overhead — no Redis, no sidecar event pipeline

**Cons**

- KV index is inferred from routing decisions with no eviction awareness — hit scores are optimistic upper bounds
- Push model can pile concurrent decisions onto the same pod before counters update
- No length-aware scheduling lever
- No hardware modeling — homogeneous pod assumption

---

### AIBrix Preble

**Pros**

- GPU cost model makes better decisions on heterogeneous hardware and under mixed traffic
- Decode backlog term in histogram partially addresses head-of-line blocking
- Cost weighting means a high hit percentage on a burdened pod can be correctly passed over
- More theoretically grounded than percentage-match selection

**Cons**

- Inherits the same stale KV index as native — eviction-unaware
- Cost polynomials require per-GPU profiling to calibrate; currently only available for Mistral-7B on A6000/V100
- More complex to reason about and tune than the other two methods
- Still vulnerable to thundering-herd on high-hit pods

---

## Overall Winner by Workload

| Workload | Winner |
|---|---|
| Stateless short-request burst | AIBrix native or Preble |
| Long shared-prefix (RAG, system prompt) | Pull-based |
| Agentic multi-stage pipelines | Pull-based |
| Mixed short and long concurrent | Pull-based (Preble comparable) |
| Memory pressure / high eviction | Pull-based |
| Heterogeneous GPU hardware | AIBrix Preble |
| Low-latency interactive, no reuse | AIBrix native or Preble |
