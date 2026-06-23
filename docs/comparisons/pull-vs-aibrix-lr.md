# Algorithmic Comparison: AIBrix Least-Request vs Pull-Based Scheduling

---

## Part 1 — Core Scheduling, Without KV or Length Ordering

This section treats both methods purely as load balancers: ignore KV cache awareness, ignore
length-aware reordering, and focus only on how each method assigns requests to pods.

### Algorithmic Model

**AIBrix** is a greedy minimum-load dispatcher. At arrival, it reads a cached
`running_requests` count per pod, finds the minimum, and pushes the request immediately.
The decision is made once and is final.

**Pull-based** is a deferred capacity-auction. Requests enter a central queue. Pods pull work
only when they have free slots (`BATCH_SIZE - pending - inflight`). The router assigns from
the queue front at pull time.

---

### Comparison Table — Core Properties

| Dimension | AIBrix | Pull-based | Winner |
|---|---|---|---|
| Decision point | Arrival (push) | Pull time (deferred) | Pull — more accurate signal |
| Load signal | Cached running-request count | Exact local pending+inflight | Pull — zero staleness |
| Backpressure | None | Natural (capacity-gated pull) | Pull |
| Overload behavior | vLLM internal queues grow silently | Central queue absorbs surplus | Pull |
| Per-request routing overhead | O(N pods), local read | O(want × pool_factor), under lock | AIBrix — lower overhead |
| Observability | Per-pod metric scrape | Single central queue length | Pull — single choke point |
| Single point of failure | Gateway-local only | Central queue | AIBrix |
| Starvation risk | None | Possible under pool re-queuing | AIBrix |

---

### Decision Point and Signal Quality

AIBrix's signal is `RealtimeNumRequestsRunning`, scraped from vLLM's metrics endpoint and
stored in a gateway-local cache. This introduces scrape lag — under rapid load change,
assignments can overshoot a pod that just became saturated. There is also a multi-replica
problem: if two gateway instances run, their caches diverge and both may simultaneously push
to the same minimum-count pod.

Pull-based reads `LocalQueue.state()` which reflects exact counts at the moment of pull.
There is no scrape pipeline. However, the signal is local to the sidecar — it does not see
vLLM's internal queue depth if vLLM itself is backlogged. In practice this gap is small
because the sidecar controls how much enters vLLM in the first place.

---

### Backpressure and Overload

This is the largest functional difference between the two.

AIBrix pushes unconditionally. If a pod is busy and the metric cache has not updated, the
gateway continues assigning to it. vLLM accepts these requests and queues them internally.
This queue is invisible to the router — from the gateway's perspective the pod looks busy
but not overloaded. Under a traffic spike, all pods saturate simultaneously and latency
degrades abruptly.

Pull-based imposes a hard ceiling. A pod that is at `BATCH_SIZE` capacity issues no pull
request. Excess traffic accumulates in the central queue where it is visible, measurable, and
sheds naturally when pods free up. Latency increases gradually as queue depth grows rather
than collapsing through vLLM-internal queue explosion.

---

### Behavior Under Different Workloads (Part 1)

| Workload | AIBrix behavior | Pull-based behavior | Better |
|---|---|---|---|
| Low, steady traffic | Good — pods never saturate, signal always accurate | Good — minimal queue depth, fast dispatch | Tie |
| High uniform burst | Overshoots due to stale metric, vLLM queues grow | Central queue buffers burst, pods drain at their own rate | Pull |
| Heterogeneous pod speeds | Slow pods receive same rate as fast ones until metric catches up | Slow pods pull less, fast pods pull more — automatic | Pull |
| Single-pod failure | May route to failed pod until health check fires | Failed pod stops pulling — self-removes from rotation | Pull |
| Many gateway replicas | Counts diverge across replicas, same pod gets double-assigned | Central queue is shared — no replica divergence | Pull |
| Very low latency requirement | Minimal overhead, immediate push | Pull adds one round trip per batch | AIBrix |

---

### Pros and Cons Summary — Part 1

**AIBrix**

- Pro: zero infrastructure dependencies, single binary, low per-request overhead
- Pro: no lock contention at scale (gateway-local read)
- Pro: simple failure modes — cache miss degrades to random, never to hard failure
- Con: no backpressure, overload is invisible to the router
- Con: stale signal causes temporary over-assignment under load spikes
- Con: multi-replica deployments require single-leader discipline

**Pull-based**

- Pro: natural backpressure with exact capacity accounting
- Pro: heterogeneous pods self-balance without tuning
- Pro: single observable queue for capacity planning and autoscaling signals
- Con: central queue is a contention point and a single point of failure
- Con: pull round-trip adds latency on the first request after an idle period
- Con: pool re-queuing creates theoretical starvation if pool scoring is skewed

---

## Part 2 — With KV Cache Awareness and Length Ordering

Now add KV prefix scoring and length-aware selection. AIBrix has neither. This section
analyzes what these features add, when they matter, and what they cost.

---

### What Each Method Does

AIBrix does nothing with KV state or request length. It dispatches in arrival order to the
minimum-load pod regardless of cache contents.

Pull-based adds two layers on top of the core scheduling:

- **KV-first tier ordering**: requests are grouped by contiguous prefix hit count on the
  target pod, descending. A request whose first 8 blocks are already cached on pod A scores
  higher than one with 0 hits, and pod A's pull will prefer it.
- **Length refinement within tiers**: inside each KV tier, requests are sorted by predicted
  output token count (character length / 2) according to `LEN_POLICY`. This is secondary —
  it never overrides KV tier assignment.

The KV hit count is produced by a multi-hop pipeline: vLLM emits block events over ZMQ →
sidecar writes to Redis → KVWatcher polls Redis → `register_block_owners` updates in-memory
state → `prefix_len` is called at pull time.

---

### Comparison Table — KV and Length Awareness

| Dimension | AIBrix | Pull-based | Winner |
|---|---|---|---|
| KV prefix awareness | None | Full (contiguous prefix scoring) | Pull |
| Cache reuse for repeated prefixes | None — always re-prefills | High — routes to owning pod | Pull |
| Multi-turn / agentic routing | Unaware — scatters sessions | Natural affinity via KV score | Pull |
| Length-aware scheduling | None | Short-first or long-first within KV tier | Pull |
| HoL blocking from long requests | Present — arrival order only | Reduced by short-first policy | Pull |
| KV signal staleness | N/A | Up to KV_WATCH_INTERVAL_S (default 1s) | AIBrix — no staleness |
| Infrastructure cost of KV | Zero | Redis + hash-service + ZMQ + watcher | AIBrix |

---

### KV Cache Impact by Workload

| Workload | KV benefit level | AIBrix impact | Pull-based impact |
|---|---|---|---|
| Independent short prompts | Low | No loss from missing KV | Minimal gain — short prefixes, low hit rate |
| Shared system prompt (RAG, chatbot) | High | Re-prefills system prompt on every request, every pod | Routes to pod that cached the system prompt — skips prefill |
| Multi-turn conversation | Very high | Each turn lands on a random pod, no turn-over-turn KV reuse | Subsequent turns route to the same pod via KV score — full prefix reuse |
| Agentic pipelines (multi-stage) | Very high | Each stage re-prefills prior context from scratch | Each stage routes to the pod holding prior stage KV — accumulating hit rate |
| Diverse one-shot prompts | None | No difference | No KV hits, degrades to same behavior as Part 1 |
| Burst of identical prompts | High | Each copy sent to whichever pod has least load | All copies routed to the pod that cached the first — maximum reuse |

The key insight for agentic workloads is that the benefit compounds. Stage 1 caches some
blocks. Stage 2 routes to the same pod (high KV score) and caches more. Stage 3 routes there
again. The KV hit rate grows monotonically across the pipeline without any session-tracking
code — it emerges from the scoring function.

---

### KV Signal Staleness and Its Consequences

The KV watcher polls Redis every `KV_WATCH_INTERVAL_S` (default 1s) and discovers pods every
`KV_DISCOVERY_INTERVAL_S` (default 5s). Block ownership may be up to ~1s stale in the
router's in-memory state. This matters in two cases:

- **Eviction lag**: a block evicted from a pod may still appear as owned in the router for
  up to 1s, causing a mis-scored assignment that partially misses the cache.
- **New blocks not yet visible**: blocks written during a very recent request may not be
  visible to the watcher yet, so the very next request in the same session may score 0 and
  be routed elsewhere, breaking the affinity chain.

For long-running sessions (>1s between turns) the staleness window is negligible. For
rapid-fire agentic pipelines where stages complete in under a second, the watcher interval is
a real risk. This is tunable but adds load on Redis as the interval decreases.

---

### Length-Aware Scheduling

AIBrix has no equivalent. Requests are dispatched in arrival order to the chosen pod.

Pull-based applies `short_first` (default) within each KV tier. The theoretical motivation
is shortest-job-first (SJF): shorter requests complete faster, free the pod sooner, and
reduce mean waiting time across the queue. The benefit is real but secondary to KV — a
request in a lower KV tier will not be promoted above a higher-KV-tier request regardless of
its length.

The predictor used is trivial (`char_len / 2`), which introduces noise. A poor length
prediction can reverse the intended ordering inside a tier without providing any benefit. A
real predictor (token-count model, decode-length model) would make this feature much more
valuable.

---

### Pros and Cons Summary — Part 2

**AIBrix (with KV consideration)**

- Pro: no KV infrastructure, no staleness, no additional failure modes
- Pro: correct behavior for workloads with no prefix reuse — no wasted work
- Con: pays full prefill cost on every request regardless of cache state
- Con: inherently incompatible with session affinity — would require an external sticky
  routing layer to achieve what pull-based gets automatically

**Pull-based (with KV and length)**

- Pro: KV-aware routing is structurally natural — no separate affinity mechanism needed
- Pro: benefit compounds for multi-stage workloads, giving super-linear throughput gains as
  session depth increases
- Pro: length-aware ordering reduces HoL blocking within locality tiers
- Con: KV pipeline adds Redis, hash-service, and ZMQ as hard or soft dependencies
- Con: watcher polling creates a staleness window that can break affinity for rapid-fire
  sessions
- Con: length predictor is too coarse to reliably deliver SJF benefits without a real
  prediction model

---

### Overall Winner by Scenario

| Scenario | AIBrix | Pull-based |
|---|---|---|
| Stateless API, diverse prompts | ✓ | |
| Low-latency, minimal infra | ✓ | |
| Multi-replica gateway | | ✓ |
| Bursty traffic | | ✓ |
| Shared system prompt workloads | | ✓ |
| Multi-turn conversation | | ✓ |
| Agentic / multi-stage pipelines | | ✓ |
| Heterogeneous pod capacities | | ✓ |
| Overload and backpressure | | ✓ |

AIBrix wins where the workload has no prefix reuse and operational simplicity is the
priority. Pull-based wins in all scenarios involving session continuity, repeated prefixes,
or sustained load — which describes the majority of production LLM serving patterns beyond
simple single-turn API calls.
