# LA-Boom Roadmap

> **Single source of truth for the LA-Boom feature roadmap.** This document
> consolidates what used to be spread across `vision.md` and
> `open-sourcing-plan.md` into one clean file: the vision, what ships today, a
> scan of the competitive landscape, and a complete mapping of every proposed
> feature request (what we have, who else has it, and when we plan to build it).
>
> The detailed Go rewrite/implementation spec that backs the v0.1 milestone is
> [§8 v0.1 implementation](#8-v01-implementation-go-runtime). For what is *actually
> implemented today*, the [architecture docs](../architecture/overview.md) are
> authoritative — this roadmap is a planning record.

---

## 1. How to read this document

- **[§2 Vision](#2-vision)** — the thesis and design goals.
- **[§3 What ships today](#3-what-ships-today)** — capabilities already in the codebase.
- **[§4 Competitive landscape](#4-competitive-landscape)** — the 2026 scan of comparable frameworks and their signature features.
- **[§5 Feature-request mapping](#5-feature-request-mapping)** — the master table: every proposed feature, its status in LA-Boom, which competitors ship it, priority, and target milestone.
- **[§6 Feature catalog](#6-feature-catalog)** — a one-paragraph description of every feature in the table, grouped by area. Anything newly proposed is described here.
- **[§7 Phased milestones](#7-phased-milestones)** — v0.1 → v1.0.
- **[§8 v0.1 implementation (Go runtime)](#8-v01-implementation-go-runtime)** — the condensed Go rewrite/migration spec that backs the v0.1 milestone.
- **[§9 Additions from the competitive scan](#9-additions-from-the-competitive-scan)** — the delta: features added to this roadmap from the landscape research that were not in the previous planning docs, each with rationale and source.
- **[§10 Sources](#10-sources)** — references for the competitive scan.

**Status legend** (used throughout):

| Symbol | Meaning |
|--------|---------|
| ✅ **Shipped** | Implemented and usable in the current codebase (Python and/or Go). |
| 🟡 **Partial** | Available in a limited form, only through an integrated third party (e.g. LMCache/Mooncake), or implemented in one runtime but not the other. |
| 🔵 **Planned** | Already committed to a milestone in the previous planning docs. |
| 🟣 **Proposed** | New to this roadmap, added from the competitive scan in §4. See §9. |

---

## 2. Vision

Serving large generative models means balancing user-facing latency (time-to-first-token, time-between-tokens) against system goals (throughput, accelerator utilization, cost). Traditional serving scales whole replicas on generic signals (CPU / accelerator utilization), which fits LLM workloads poorly: requests vary enormously in prompt length, decode length, KV-cache footprint, and service time. Continuous batching strongly couples throughput and latency; KV-cache reuse can eliminate most prefill cost but creates placement and memory-management problems; and modern clusters are increasingly heterogeneous (GPUs **and** NPUs) with different memory and compute profiles.

**LA-Boom is a Kubernetes-native serving platform that puts KV-cache reality at the center of routing, scheduling, autoscaling, and memory management, wrapped around unmodified inference engines (vLLM today).** It aims to be both a production-ready serving platform *and* a reproducible research framework for scheduling and memory strategies — every routing claim is backed by the bundled benchmark harness.

The design pillars:

1. **Centralized admission + pull-based scheduling** — a global queue owns every request; workers pull when they have capacity, so placement is a scheduling decision, not a hash of the client's connection.
2. **KV-, length-, and SLO-aware routing** — route for cache reuse and deadline headroom, not round-robin.
3. **Kubernetes-native, one Helm chart** — router, sidecars, Redis, engine, gateways, and per-model autoscaling deploy together; no engine fork.
4. **Heterogeneous-accelerator awareness** — GPU and NPU are first-class.
5. **Extensible backend integration** — engines, KV-transfer backends, and gateways are swappable.

---

## 3. What ships today

These capabilities exist in the codebase now (see the [architecture docs](../architecture/overview.md) for detail). They anchor the "LA-Boom status" column in §5.

**Queueing & scheduling**
- Centralized per-model request queue with pull-based dispatch (default); push strategies `push-rr`, `push-random`, `push-leastq`, `push-throughput`, `push-p2c`, `push-kv-cost`, `push-least-kv`, `push-least-latency`, `push-least-busy`; plus hybrid `central-push` and `external-push`. See [router.md](../architecture/router.md). Mode × placement × rebalancing coverage: [Routing Compatibility Matrix](../architecture/router.md#routing-compatibility-matrix).
- Length-aware queue ordering within KV tiers (`short_first` / `long_first`).
- SLO-aware slack scheduling (opt-in) with deadline math for TTFT / TPOT / E2E.
- Admission control that binary-searches the largest batch keeping predicted TPOT under budget (opt-in).
- Fair-pull throttle across models/keys (opt-in).
- Sidecar-less delivery for all `push-*` and `central-push` (`ROUTER_SIDECAR_ENABLED=false` / `sidecar.enabled=false`).
- Token-aware pull sizing (prefill-token budget + sidecar KV pull gate; inflight token gauge always on).

**KV-cache-aware routing**
- Inline, vLLM-compatible chained block hashing in the router (Python in-process; Go in-container hasher), with an optional external prefix-hash service.
- Sidecars publish block ownership to Redis from vLLM ZMQ KV events; router builds a live `block-hash → replica` map via targeted per-request lookup (default) or a background watcher.
- Contiguous-prefix scoring and KV-hit tiering; `router_strategy = none | prefix | affinity | both`.
- Conversation affinity (soft/hard) with a stable per-conversation key and an optional Redis-backed **persistent affinity map** that survives router restarts.
- **Soft KV divert** — trims cold work away from GPU-pressured pods while preserving affinity pins and warm requests (Python + Go).

**Serving topologies & KV transfer**
- Multi-model serving from one cluster; dense TP; data-parallel + expert-parallel for MoE via LeaderWorkerSet.
- Cross-node KV transfer through **Mooncake** and **LMCache** (P2P + host-staging), toggled by `lmcache.mode`.

**Autoscaling**
- Per-model **KEDA** autoscaling on router-queue depth or vLLM KV-cache-usage signals, across dense / multi-model / DP-LWS topologies (opt-in, backward compatible).

**Accelerators, gateways, observability**
- Runs on Ascend **NPU** clusters; sidecar scrapes and normalizes vLLM KV-cache usage; SLO-driven dynamic pull backpressure (opt-in).
- **BooM** (Rust) and **LiteLLM** gateways for auth, virtual keys, rate limiting, and spend tracking; Claude Code support.
- Prometheus metrics, per-request tracing, Grafana dashboards, async ZMQ pub/sub result transport.
- Two functionally-parallel implementations (Python FastAPI and Go chi) selectable via `serviceImpl`.

---

## 4. Competitive landscape

A 2026 scan of the frameworks LA-Boom is most often compared to. This is the basis for the "seen in" column in §5 and the additions in §9. Sources in [§10](#10-sources).

| Framework | What it is | Signature features relevant to us |
|-----------|-----------|-----------------------------------|
| **llm-d** | CNCF-adjacent, Kubernetes-native distributed inference; router + InferencePool + model servers | Router = Envoy proxy + **Endpoint Picker (EPP)** over `ext-proc`; **precise** prefix-cache-aware routing via a KV-Cache Indexer fed by vLLM/SGLang ZMQ `KVEvents`; **speculative indexing** (short-lived predicted cache entries with TTL); **P/D and E/P/D disaggregation** with a disaggregation sidecar + NIXL transfers; **tiered KV offloading** (GPU/CPU/SSD, now upstreamed into vLLM's multi-tier connector); Gateway API Inference Extension native |
| **AIBrix** | Cloud-native control plane for vLLM (control + data plane split) | LLM-aware **Envoy gateway** (prefix-cache-aware, least-GPU-memory, instance routing); **high-density multi-LoRA** management; **distributed KV cache** with cross-engine reuse + scan-resistant eviction; **LLM-specific autoscaler** on KV/queue metrics (second-level); **GPU optimizer** for SLO-driven **heterogeneous** serving; unified **AI runtime sidecar**; **GPU failure detection**; fairness + **TPM/RPM rate control**; `StormService` + `RayClusterFleet` multi-node; **vLLM Semantic Router**; **PD disaggregation** |
| **NVIDIA Dynamo** | Engine-agnostic distributed inference (vLLM, SGLang, TRT-LLM) | **KV-aware "Smart Router"** with radix-tree cache-overlap scoring + load; **disaggregated prefill/decode**; **SLO / GPU Planner** that recommends P/D configs and parallelism; **KV Block Manager (KVBM)** tiered offload/recall; **NIXL** transfer library; Kubernetes **operator + CRDs**; Grove/KAI topology-aware grouped scheduling |
| **vLLM production-stack** | Official vLLM reference stack (Berkeley/UChicago) | Router with `roundrobin` / `session` / `prefixaware` / `kvaware` logic; **LMCache** KV offload (CPU/disk) + prefetch; **semantic caching**; **PII request-rewrite**; Grafana dashboards with cache-hit-rate; autoscaling; fast vLLM bootstrapping |
| **KServe** | CNCF model-serving platform; `LLMInferenceService` CRD | **Workload Variant Autoscaler (WVA)** on KV/queue metrics with **HPA or KEDA** actuators (idle scale-down, initial cooldown for model load, metric fallback); **independent prefill scaling**; multi-node **LWS** autoscaling; **OpenAI Responses API** + embeddings routing; llm-d integration |
| **SGLang router** | Rust load balancer / model gateway | Policies: `cache_aware` (radix tree, load-adaptive), `power_of_two`, `round_robin`, `random`, `bucket`; **PD disaggregation** with separate prefill/decode policies; reliability core: **circuit breakers, token-bucket rate limiting, retries w/ jitter, health checks**; K8s service-discovery selectors; cache-aware **DP-rank** routing |
| **Mooncake** | KVCache-centric disaggregated serving platform (Moonshot/Kimi) | **Conductor** global KV-centric scheduler; **prediction-based early rejection** under overload; **Transfer Engine** (multi-NIC bandwidth aggregation, topology/NUMA-aware paths, RDMA/TCP/NVMe-oF/CXL, failover); **Mooncake Store** multi-producer/multi-consumer cluster KV pool with tiering |
| **Gateway API Inference Extension (GAIE)** | Kubernetes SIG standard for inference routing | `InferencePool` (GA) + **EPP** over Envoy `ext-proc`; **Body-Based Router** (model-name-aware); prefix-cache-aware LB with remote-cache interfaces; **LoRA-aware** routing + rollout; **fairness/priority** within a criticality band; HPA on LB-derived metrics; disaggregated pools; heterogeneous accelerators |

**Takeaway.** LA-Boom is already competitive — arguably ahead — on **precise KV-aware pull scheduling**, **conversation affinity with durable pins**, **SLO slack scheduling**, and **NPU support**. The clearest gaps versus the field are: **prefill/decode disaggregation orchestration**, a **standard gateway integration (Gateway API / EPP `ext-proc`)**, a **native tiered KV-offload store and cross-engine KV pool with a prefix-location index**, **LoRA-aware routing**, **multi-engine backends**, and **heterogeneity-aware, cost/SLO-driven autoscaling**.

---

## 5. Feature-request mapping

The master table. "Seen in" lists frameworks from §4 that ship a comparable capability (evidence that the feature is proven, and a place to borrow design). Priority is a rough product judgment (P0 = differentiator/near-term, P2 = long-horizon).

### 5.1 Queueing & scheduling

| Feature | LA-Boom status | Seen in | Priority | Milestone |
|---------|----------------|---------|----------|-----------|
| Centralized admission queue | ✅ Shipped | llm-d, AIBrix, Dynamo | — | Done |
| Pull-based scheduling | ✅ Shipped | (LA-Boom-distinctive) | — | Done |
| Push dispatch strategies (`push-rr` / `push-random` / `push-leastq` / `push-throughput` / `push-p2c` / `push-kv-cost` / `push-least-kv` / `push-least-latency` / `push-least-busy` / `central-push` / `external-push`) | ✅ Shipped | SGLang (`power_of_two`/`round_robin`/`random`), AIBrix (least-GPU-memory), Dynamo, prod-stack | — | Done; [compat matrix](../architecture/router.md#routing-compatibility-matrix) |
| Token-length-aware ordering | ✅ Shipped | — | — | Done |
| SLO-aware slack scheduling | ✅ Shipped (opt-in) | Dynamo, Mooncake | — | Done |
| Admission control (TPOT budget) | ✅ Shipped (opt-in) | — | — | Done |
| Fair-pull / fairness throttle | ✅ Shipped (opt-in) | AIBrix, GAIE | — | Done |
| Adaptive batch-size control | 🔵 Planned | — | P1 | v0.2 |
| Multi-queue scheduling (priority/workload classes) | 🔵 Planned | AIBrix, GAIE | P1 | v0.2 |
| Token-level capacity scheduling | 🔵 Planned | Dynamo | P1 | v0.2 |
| Hierarchical queueing (cluster/node/worker) | 🔵 Planned | — | P2 | v0.3 |
| Preemption-aware scheduling | 🔵 Planned | — | P2 | v0.3 |
| Prediction-based early rejection under overload | 🟣 Proposed | Mooncake | P1 | v0.2 |
| Circuit breakers / token-bucket rate limiting / retry-with-jitter in the router core | 🟣 Proposed | SGLang, AIBrix | P1 | v0.2 |

### 5.2 Autoscaling & capacity management

| Feature | LA-Boom status | Seen in | Priority | Milestone |
|---------|----------------|---------|----------|-----------|
| LLM-aware autoscaling (queue / KV signals) | ✅ Shipped (KEDA) | AIBrix, KServe, Dynamo | — | Done |
| KEDA idle scale-down / initial cooldown / metric fallback | 🟡 Partial | KServe (WVA) | P1 | v0.2 |
| Operator/fine-grained scaling (prefill/decode/KV-manager as targets) | 🔵 Planned | Dynamo, KServe, AIBrix | P1 | v0.3 |
| Joint load-balancing + autoscaling control | 🔵 Planned | — | P2 | v1.0 |
| KV-aware scale-down (preserve replicas holding valuable KV) | 🔵 Planned | Dynamo | P1 | v0.3 |
| Heterogeneity-aware, cost/SLO-driven autoscaling | 🟣 Proposed | AIBrix (GPU optimizer), Dynamo (Planner) | P0 | v0.3 |
| Fast replica startup / layered model loading | 🔵 Planned | prod-stack, Dynamo | P2 | v1.0 |

### 5.3 KV cache & memory infrastructure

| Feature | LA-Boom status | Seen in | Priority | Milestone |
|---------|----------------|---------|----------|-----------|
| KV-cache awareness (scheduler tracks reuse) | ✅ Shipped | all | — | Done |
| Prefix caching / prefix-aware routing | ✅ Shipped | all | — | Done |
| Precise KV ownership index (event-driven) | ✅ Shipped | llm-d, Dynamo | — | Done |
| Cross-node KV transfer | 🟡 Partial (Mooncake / LMCache) | Dynamo, Mooncake, llm-d | P1 | v0.3 |
| Native tiered KV offloading store (GPU→CPU→SSD) | 🟡 Partial (via LMCache) → 🔵 native planned | llm-d, Dynamo (KVBM), AIBrix, prod-stack | P0 | v0.3 |
| Cross-engine / distributed KV pool + prefix-location index | 🟣 Proposed | AIBrix, Mooncake Store, Dynamo | P1 | v0.3 |
| Speculative / predictive KV indexing | 🟣 Proposed | llm-d (speculativeIndexing) | P1 | v0.3 |
| CacheBlend-style non-prefix KV reuse | 🔵 Planned | (research) | P2 | v0.3 |
| KV hierarchy + eviction policies (scan-resistant) | 🔵 Planned | AIBrix, Mooncake | P2 | v0.3 |

### 5.4 Routing intelligence

| Feature | LA-Boom status | Seen in | Priority | Milestone |
|---------|----------------|---------|----------|-----------|
| KV/prefix-aware routing | ✅ Shipped | all | — | Done |
| Conversation affinity (soft/hard, durable pins) | ✅ Shipped | prod-stack (session) | — | Done |
| SLO-based routing | ✅ Shipped (opt-in) | Dynamo, GAIE | — | Done |
| Agent-/session-aware routing & scaling | 🔵 Planned | Dynamo (agentic) | P1 | v0.2 |
| LoRA adapter-aware routing (+ high-density multi-LoRA) | 🟣 Proposed | AIBrix, GAIE | P0 | v0.2 |
| Semantic / task-aware routing | 🔵 Planned | AIBrix (semantic router), prod-stack | P2 | v0.2 |
| Multi-level routing (cluster/node/worker) | 🔵 Planned | — | P2 | v0.3 |
| Semantic response caching | 🟣 Proposed | prod-stack | P2 | v1.0 |

### 5.5 Runtime optimization

| Feature | LA-Boom status | Seen in | Priority | Milestone |
|---------|----------------|---------|----------|-----------|
| Continuous-batching awareness | ✅ Shipped | all | — | Done |
| Token-length prediction | 🟡 Partial (length policy uses predictions) | — | P1 | v0.2 |
| Prefill/decode disaggregation orchestration | 🟣 Proposed | llm-d, Dynamo, SGLang, Mooncake, KServe, AIBrix | P0 | v0.3 |
| Speculative decoding (engine feature; support/expose) | 🔵 Planned | (engine) | P2 | v1.0 |
| Layered model loading | 🔵 Planned | prod-stack | P2 | v1.0 |

### 5.6 Backend & infrastructure abstraction

| Feature | LA-Boom status | Seen in | Priority | Milestone |
|---------|----------------|---------|----------|-----------|
| Multiple accelerator types (GPU + NPU) | ✅ Shipped (Ascend NPU) | AIBrix, GAIE | — | Done |
| GPU/NPU metrics integration + normalization | 🟡 Partial (KV-usage scrape) | AIBrix, Dynamo | P1 | v0.1 |
| Envoy / Gateway API (EPP `ext-proc`) integration | 🟣 Proposed | llm-d, AIBrix, GAIE | P0 | v0.2 |
| Multi-backend engines (SGLang, TRT-LLM, …) | 🔵 Planned | Dynamo, llm-d, SGLang | P1 | v1.0 |
| Heterogeneous hardware scheduling (capacity model per class) | 🔵 Planned | AIBrix, Dynamo | P1 | v0.3 |
| GPU/NPU time-sharing / partitioning | 🔵 Planned | — | P2 | v1.0 |
| Kubernetes operator + CRD (`ModelDeployment`) | 🔵 Planned | Dynamo, KServe, AIBrix | P1 | v0.1/v1.0 |
| GPU/accelerator hardware-failure detection | 🟣 Proposed | AIBrix | P1 | v0.3 |
| OpenAI Responses API + `/v1/embeddings` + `/v1/models` | 🟣 Proposed | KServe | P2 | v1.0 |
| Multi-cloud support (Huawei / Ali / GCP) | 🔵 Planned | — | P2 | v1.0 |

### 5.7 Novel / differentiating bets

| Feature | LA-Boom status | Seen in | Priority | Milestone |
|---------|----------------|---------|----------|-----------|
| Energy-/power-/thermal-aware routing (GPU + NPU) | 🔵 Planned | — (differentiator) | P2 | v0.3+ |

### 5.8 Reliability, security & platform (v1.0 hardening)

| Feature | LA-Boom status | Seen in | Priority | Milestone |
|---------|----------------|---------|----------|-----------|
| Security & authentication | 🟡 Partial (via BooM/LiteLLM) | all | P1 | v1.0 |
| Multi-tenancy + workload isolation | 🟡 Partial (BooM keys, fair-pull) | AIBrix, GAIE | P1 | v1.0 |
| Model lifecycle management | 🔵 Planned | KServe, AIBrix | P2 | v1.0 |
| Observability dashboards (built-in) | ✅ Shipped (Grafana) | all | — | Done |
| Reliability & recovery (heartbeat TTL, idempotent completion, graceful drain) | 🔵 Planned | Dynamo, SGLang | P1 | v0.1 |
| OpenTelemetry tracing | 🔵 Planned | — | P1 | v0.1 |

---

## 6. Feature catalog

One paragraph per feature, grouped by area, so the table above is self-contained. Features marked 🟣 are new to this roadmap (see §9); their descriptions here satisfy the rule that anything added is documented.

### 6.1 Queueing & scheduling

- **Centralized admission queue** ✅ — a global per-model queue receives all requests before assignment, making placement an explicit scheduling decision.
- **Pull-based scheduling** ✅ — workers pull from the queue when capacity frees up (capacity-gated), which naturally load-balances and avoids head-of-line stalls from static hashing.
- **Push dispatch strategies** ✅ — proactive routing modes: `push-rr`, `push-random`, `push-leastq`, `push-throughput` (token counters), `push-p2c` (power-of-two choices), `push-kv-cost` (KV-aware cost), `push-least-kv`, `push-least-latency`, `push-least-busy`, plus `central-push` and `external-push`. See [router.md](../architecture/router.md) and the [compatibility matrix](../architecture/router.md#routing-compatibility-matrix).
- **Token-length-aware ordering** ✅ — within a KV tier, order by predicted output length (`short_first`/`long_first`) to reduce head-of-line blocking.
- **SLO-aware slack scheduling** ✅ — sort by deadline headroom (`slack = deadline − predicted_completion`) in latency bands for TTFT/TPOT/E2E targets.
- **Admission control (TPOT budget)** ✅ — binary-search the largest admit set that keeps predicted TPOT under budget.
- **Fair-pull / fairness throttle** ✅ — bound how much of a pull grant any one model/key may consume.
- **Adaptive batch-size control** 🔵 — dynamically size batches to latency targets and memory headroom instead of a fixed cap.
- **Multi-queue scheduling** 🔵 — separate queues for priority tiers or workload classes, with isolation guarantees.
- **Token-level capacity scheduling** 🔵 — gate on token workload (prompt + expected decode) rather than request counts.
- **Hierarchical queueing** 🔵 — queues at cluster, node, and worker levels for very large fleets.
- **Preemption-aware scheduling** 🔵 — factor backend memory pressure and preemption cost into placement.
- **Prediction-based early rejection** 🟣 — under overload, reject requests that provably cannot meet their SLO *before* spending prefill on them (Mooncake's admission strategy); protects goodput during spikes.
- **Router-core reliability primitives** 🟣 — per-worker circuit breakers, token-bucket rate limiting with queuing, and retry-with-jitter inside the router, so a slow/broken pod is shed automatically (SGLang's "reliability core", AIBrix rate control).

### 6.2 Autoscaling & capacity management

- **LLM-aware autoscaling** ✅ — per-model KEDA scaling on router-queue depth or KV-cache-usage, not CPU.
- **KEDA idle scale-down / initial cooldown / metric fallback** 🟡 — adopt KServe/WVA-style actuator ergonomics: scale to zero when idle, hold off scale-up during model load, and fall back to a fixed replica count on metric outage.
- **Operator/fine-grained scaling** 🔵 — scale prefill workers, decode workers, and KV managers as independent targets with their own signals.
- **Joint load-balancing + autoscaling control** 🔵 — coordinate the routing and scaling loops so they don't fight.
- **KV-aware scale-down** 🔵 — defer terminating replicas that hold reusable KV until it expires or transfers.
- **Heterogeneity-aware, cost/SLO-driven autoscaling** 🟣 — maintain a capacity model per accelerator class (token throughput, max batch, KV headroom), scale each class as its own pool, and prefer lower-cost classes when SLOs allow (AIBrix GPU optimizer, Dynamo Planner). This is the autoscaling counterpart to heterogeneous scheduling and a strong cost lever.
- **Fast replica startup / layered model loading** 🔵 — cut scale-out latency with optimized images and incremental weight loading.

### 6.3 KV cache & memory infrastructure

- **KV-cache awareness / prefix caching / precise ownership index** ✅ — already the core of LA-Boom (event-driven Redis ownership + contiguous-prefix scoring).
- **Cross-node KV transfer** 🟡 — works today through Mooncake and LMCache; the roadmap item is first-class router orchestration of transfers (choosing source/target) rather than delegating entirely to the connector.
- **Native tiered KV offloading store** 🟡→🔵 — a first-party `OffloadStore` (GPU→CPU→SSD/PVC) with soft/hard pressure thresholds and offload metrics, so capacity isn't bounded by GPU HBM. Today this is only available via LMCache; the field (llm-d offloader, Dynamo KVBM, AIBrix) treats it as table stakes.
- **Cross-engine / distributed KV pool + prefix-location index** 🟣 — a cluster metadata service mapping reusable prefixes to locations across engines/nodes, enabling reuse beyond a single pod (AIBrix distributed KV cache, Mooncake Store). Complements the existing per-pod ownership map with a global, engine-agnostic view.
- **Speculative / predictive KV indexing** 🟣 — insert short-lived predicted cache entries for the just-selected pod right after a routing decision (TTL'd until a confirming `BlockStored` arrives), so bursts of similar requests route coherently before events land (llm-d `speculativeIndexing`). Directly attacks the hit-rate-collapse-under-load issue documented in [kv-cache-hit-rate-collapse.md](kv-cache-hit-rate-collapse.md).
- **CacheBlend-style non-prefix reuse** 🔵 — reuse KV for cached segments that aren't strict prefixes.
- **KV hierarchy + scan-resistant eviction** 🔵 — multi-tier storage with eviction that resists cache-polluting scans (AIBrix).

### 6.4 Routing intelligence

- **KV/prefix-aware + affinity + SLO routing** ✅ — shipped (see §3).
- **Agent-/session-aware routing & scaling** 🔵 — treat session identity as first-class; keep an agent's dependent calls on the pod holding its KV, give blocking tool-use stage-aware priority, and count expected follow-ups in queue-pressure signals.
- **LoRA adapter-aware routing** 🟣 — propagate adapter identity through admission/queue/scheduling, track per-worker resident adapters, route to workers that already have the adapter loaded, and key the prefix map by `(model, adapter, prefix)` since KV is adapter-specific. High-density multi-LoRA is a core AIBrix/GAIE capability and a common multi-tenant pattern LA-Boom currently ignores.
- **Semantic / task-aware routing** 🔵 — route by task type / reasoning complexity (AIBrix vLLM Semantic Router).
- **Multi-level routing** 🔵 — routing logic at cluster/node/worker tiers.
- **Semantic response caching** 🟣 — cache responses by semantic similarity of requests to short-circuit repeats (prod-stack). Cross-cutting with the request-rewrite/PII idea.

### 6.5 Runtime optimization

- **Continuous-batching awareness** ✅ — sidecar surfaces batch size / running requests / TPS.
- **Token-length prediction** 🟡 — length policy already consumes predictions; the roadmap item is a dedicated predictor with accuracy metrics.
- **Prefill/decode disaggregation orchestration** 🟣 — split the compute-bound prefill phase and memory-bound decode phase onto specialized pools, with the router selecting a prefill *and* a decode endpoint and coordinating the KV handoff (via Mooncake/NIXL). This is the single most common capability across llm-d, Dynamo, SGLang, Mooncake, KServe, and AIBrix that LA-Boom lacks at the orchestration layer, even though the KV-transfer plumbing (Mooncake/LMCache) is already integrated.
- **Speculative decoding** 🔵 — support/expose engine-side draft-model acceleration.
- **Layered model loading** 🔵 — incremental weight loading for faster starts.

### 6.6 Backend & infrastructure abstraction

- **Multiple accelerator types (GPU + NPU)** ✅ — runs on Ascend NPU today.
- **GPU/NPU metrics normalization** 🟡 — extend the sidecar's KV-usage scrape into a normalized accelerator-telemetry schema (util, memory used/total, type) behind one collector interface (planned in the v0.1 spec).
- **Envoy / Gateway API (EPP `ext-proc`) integration** 🟣 — expose LA-Boom's scoring as a Gateway API Inference Extension **Endpoint Picker** so it can front a standard Inference Gateway (Envoy/Istio) instead of relying on a NodePort router. This is how llm-d, AIBrix, and the K8s SIG standard integrate; it makes LA-Boom drop-in for teams already on Gateway API.
- **Multi-backend engines** 🔵 — a backend adapter layer so the scheduler is engine-agnostic (SGLang, TRT-LLM), as in Dynamo/llm-d.
- **Heterogeneous hardware scheduling** 🔵 — capacity/compatibility model per accelerator class; match requests to classes at admission.
- **GPU/NPU time-sharing / partitioning** 🔵 — share or partition accelerators across workloads.
- **Kubernetes operator + CRD** 🔵 — a `ModelDeployment` CRD + controller managing deployment shape and sidecar injection (deployment-shape only, never in the request hot path); see [§8](#8-v01-implementation-go-runtime).
- **GPU/accelerator hardware-failure detection** 🟣 — proactively detect failing accelerators and drain/replace the pod, plus failure-injection mocks for resilience testing (AIBrix). Improves reliability on large heterogeneous fleets.
- **OpenAI Responses API + embeddings/models endpoints** 🟣 — broaden the northbound surface beyond chat/completions (`/v1/responses`, `/v1/embeddings`, `/v1/models`) as KServe now does, for wider client compatibility.
- **Multi-cloud support** 🔵 — Huawei / Ali / GCP portability.

### 6.7 Novel / differentiating bets

- **Energy-/power-/thermal-aware routing** 🔵 — collect and normalize per-accelerator power draw, thermal state, and power headroom, and use headroom as a soft routing preference under tight SLOs; surface aggregate energy for cost/carbon reporting. No comparable framework does this today, so it is a potential differentiator rather than a catch-up item.

### 6.8 Reliability, security & platform

- **Security & authentication** 🟡 / **multi-tenancy** 🟡 — partially covered by BooM/LiteLLM (keys, spend, rate limits) and fair-pull; the roadmap item is first-class tenancy and isolation in the core.
- **Model lifecycle management** 🔵, **reliability & recovery** 🔵, **OpenTelemetry tracing** 🔵 — production hardening (heartbeat TTL, idempotent completion, graceful drain, structured logs, OTel spans) detailed in [§8](#8-v01-implementation-go-runtime).
- **Observability dashboards** ✅ — Grafana dashboards ship today.

---

## 7. Phased milestones

Milestones describe focus, not calendar dates. The detailed v0.1 engineering plan is in [§8 v0.1 implementation](#8-v01-implementation-go-runtime).

### v0.1 — Core system, hardened (Go production runtime)
Harden today's proven behavior into a production Go runtime: centralized queue ownership, pull scheduling, KV/prefix-aware scheduling, worker registry with heartbeat TTL, normalized accelerator telemetry, autoscaling signals, structured logging + OpenTelemetry, and a thin controller/CRD for deployment shape. Reliability boundaries (idempotent completion, graceful drain, overload rejection) become explicit.

### v0.2 — Scheduling & routing expansion
Adaptive batch sizing, multi-queue / token-level capacity scheduling, prediction-based early rejection, router-core reliability primitives (circuit breakers, rate limiting, retries). Routing: agent/session-aware, **LoRA-aware**, semantic/task-aware. Platform: **Gateway API / EPP `ext-proc`** integration; KEDA idle-scale-down ergonomics.

### v0.3 — KV infrastructure & heterogeneity
Native tiered KV-offload store; router-orchestrated cross-node transfer; **cross-engine/distributed KV pool + prefix-location index**; **speculative KV indexing**; CacheBlend; scan-resistant eviction. **Prefill/decode disaggregation orchestration**. Heterogeneity-aware scheduling **and** cost/SLO-driven autoscaling; KV-aware scale-down; GPU failure detection. Energy-aware routing (bet).

### v1.0 — Production platform
Operator-level & joint autoscaling; multi-backend engines; speculative decoding; accelerator partitioning; security/multi-tenancy/model-lifecycle; fast startup / layered loading; broader northbound API surface; multi-cloud.

---

## 8. v0.1 implementation (Go runtime)

> Condensed from the former `open-sourcing-v01.md`. This is the engineering spec behind the [§7](#7-phased-milestones) v0.1 milestone: harden today's proven Python behavior into a production **Go** runtime. The Python stack remains for load generation, offline analysis, and migration-validation comparison.

**Go becomes the source of truth for:** request admission, centralized queue ownership, worker coordination, pull scheduling, runtime metrics, autoscaling signals, pod-local sidecar state, backend abstraction, accelerator-metric normalization, and Kubernetes deployment integration.

### Tech stack
- Language Go 1.22+ · HTTP `net/http` + chi · Metrics Prometheus `client_golang` · Tracing OpenTelemetry (OTLP) · Logging zap · Config env (+ optional Viper).
- Kubernetes client-go + controller-runtime/Kubebuilder · Packaging OCI images + Helm.
- Accelerator telemetry behind one interface: NVML (GPU) and an Ascend/NPU collector.
- Primary backend vLLM (OpenAI-compatible); optional future internal transport gRPC.

### Target repository layout
```
cmd/{gateway,sidecar,controller}/main.go
internal/
  gateway/     handlers, middleware, models, queue, scheduler, registry, service, internal_api
  sidecar/     worker, backend, metrics, accel, kv, state, heartbeat
  autoscaling/ signals, aggregator
  controller/  reconciler, resources
  common/      config, logging, tracing, health, prefix, time
pkg/api/v1alpha1/modeldeployment_types.go
deploy/{crds,helm,manifests}
```

### Architecture rules
- Gateway owns request admission and queue mutation; the scheduler runs in the gateway.
- Sidecar is the worker agent and the source of pod-local runtime state.
- Controller manages deployment shape only — never the live scheduling path.
- The Kubernetes API is kept off the hot scheduling path; autoscaling-signal aggregation runs outside request execution.

### Components (responsibilities)
- **Gateway / north-south API** — OpenAI-compatible `POST /v1/chat/completions` (+ `/v1/completions`); internal `POST /internal/pull`, `/internal/complete`, `GET /metrics|/healthz|/readyz`. Assigns RequestID + arrival timestamps + PrefixHash, normalizes to a canonical internal request before enqueue, and rejects overload before the queue.
- **Centralized queue** — owns queued requests; FIFO by default with bounded prefix-aware scans; explicit inflight-assignment map; oldest-age + pressure metrics; requeue + expiration hooks. Avoid O(n) full scans; mutex-first.
- **Pull scheduler** — worker eligibility (healthy, not saturated, under memory threshold); selection = bounded-scan prefix hit → FIFO fallback → no_work; explicit `SchedulerReason`; starvation guard so locality never starves old requests. Experiment-only strategies stay out of the production path.
- **Worker registry** — in-memory, heartbeat-updated, TTL staleness; aggregates ready workers / running requests / mean batch / util for autoscaling; runtime truth independent of K8s watch latency.
- **Sidecar runtime** — stats loop (backend + accelerator + KV summary), pull loop (heartbeat + request work), completion/report loop; bounded local executor; `GET /metrics|/healthz|/readyz|/state`.
- **Backend adapter** — hides engine transport; normalizes health, runtime stats, request submission, response parsing; an `Adapter` interface so non-vLLM backends drop in without touching the scheduler.
- **Continuous-batching + prefix + KV awareness** — per-worker batch size / running / TPS; one canonical versioned PrefixHash; normalized KV usage + known-prefix summary feed scheduler locality and pressure; engine internals stay behind the sidecar/backend layer.
- **KV offloading (initial)** — an `OffloadStore` interface (local FS / PVC) with soft/hard KV-pressure thresholds and offload metrics; distributed restore deferred.
- **Accelerator metrics + heterogeneity** — one normalized collector (type, util, mem used/total) with freshness + error handling; compatibility filtering by model/backend/accelerator/memory class; hardware identity in traces/metrics.
- **Autoscaling signals** — a framework-global surface (queue length, oldest-age, running, ready workers, mean batch/KV/util) exposed for HPA/KEDA; aggregated off the hot path.
- **Controller + CRD** — reconciles a `ModelDeployment` CRD (backend, model, replicas, accelerator, sidecar/metrics toggles); creates workloads, sidecar injection, services, monitors, and autoscaling objects.
- **Observability + reliability** — zap JSON logs with correlation IDs; OTel spans (admission / queue-wait / sidecar exec / backend); heartbeat TTL, idempotent completion, bounded requeue, overload rejection, graceful drain.

### API standardization
OpenAI-compatible JSON is the external contract; a single canonical Go request model flows through queue/scheduler/sidecar; backend adapters translate to engine-native payloads and back. KServe is a deployment/control-plane compatibility target (optional protocol adapter later), not the v0.1 northbound schema.

### v0.1 acceptance criteria
- Go gateway serves the public inference APIs; admission + the authoritative central queue + pull scheduling are the only production assignment path.
- Go sidecar runs per worker pod, exports health/metrics/state, pulls work, and reports completion.
- Health/capacity guards enforced; prefix-local assignment as a soft preference; KV usage visible to the scheduler; heterogeneous compatibility filtering in place.
- Framework + pod-local metrics + autoscaling signals exist; trace/log coverage reconstructs a request lifecycle.
- A controller can deploy the runtime shape (Helm/manifests during transition); every production-relevant Python capability is represented in the Go runtime, and experiment-only Python code is off the live path.

### Python → Go module map
- router/API entrypoint → `cmd/gateway/main.go`, `internal/gateway/{handlers,service}.go`
- `router_core.py` → `internal/gateway/{queue,scheduler,registry}.go`
- `router_modes.py` → policy/config concepts only (not ported as a module)
- `utils_prom.py` → `internal/sidecar/{metrics,accel}.go`, `internal/autoscaling/aggregator.go`
- `utils_k8s.py` → `internal/gateway/registry.go` (runtime view) + `internal/controller/*` (deploy shape)
- `utils.py` → `internal/common/{logging,tracing}.go`
- `sidecar/config.py` → `internal/common/config.go`
- backend request code / `http_client.py` → `internal/sidecar/backend.go` (+ external wire-compat reference)
- `loadgen.py`, analysis notebooks → stay outside the production runtime (reused for migration validation)

---

## 9. Additions from the competitive scan

Per the requirement that *anything added must be documented*, these are the features **new to this roadmap** — not present in the previous `open-sourcing-plan.md` / `vision.md` — introduced from the §4 landscape research. Each has a description in §6 and a source in §10.

| Added feature | Why it was added | Primary source(s) |
|---------------|------------------|-------------------|
| Prefill/decode disaggregation orchestration | Ubiquitous across the field (llm-d, Dynamo, SGLang, Mooncake, KServe, AIBrix); our KV-transfer plumbing exists but the router doesn't orchestrate P/D pools | llm-d, Dynamo, SGLang, Mooncake, KServe |
| Gateway API / EPP `ext-proc` integration | The emerging K8s standard for inference routing; makes LA-Boom drop-in behind Envoy/Istio and GA `InferencePool` | GAIE, llm-d, AIBrix |
| LoRA adapter-aware routing (+ high-density multi-LoRA) | Core multi-tenant pattern we ignore; prefix map must be adapter-keyed for correctness | AIBrix, GAIE |
| Cross-engine / distributed KV pool + prefix-location index | Extends per-pod ownership to a global, engine-agnostic reuse map | AIBrix, Mooncake Store, Dynamo |
| Speculative / predictive KV indexing | Directly mitigates our documented hit-rate collapse under load | llm-d `speculativeIndexing` |
| Prediction-based early rejection under overload | Protects goodput during spikes by not prefilling doomed requests | Mooncake |
| Router-core reliability primitives (circuit breakers, rate limiting, retries-with-jitter) | Production resilience the current router lacks in-core | SGLang, AIBrix |
| Heterogeneity-aware, cost/SLO-driven autoscaling | Turns NPU/GPU heterogeneity into a cost lever, not just a scheduling constraint | AIBrix GPU optimizer, Dynamo Planner |
| GPU/accelerator hardware-failure detection | Reliability on large heterogeneous fleets; failure-injection for testing | AIBrix |
| Semantic response caching | Cheap win for repeat/near-repeat traffic | vLLM production-stack |
| OpenAI Responses API + embeddings/models endpoints | Wider client compatibility as the northbound surface broadens | KServe |

The following were **already** in the previous docs and are retained (not additions): centralized queue, pull scheduling, length/SLO/adaptive-batch/multi-queue/token-level/hierarchical/preemption scheduling, LLM-aware & operator & joint autoscaling, KV-aware scale-down, fast startup, KV offloading, distributed KV transfer, CacheBlend, KV hierarchy, predictive caching, SLO/agent/semantic/multi-level routing, token-length prediction, continuous-batching awareness, speculative decoding, layered loading, Envoy support (now specified as Gateway API), multi-backend, GPU/NPU + metrics + heterogeneous scheduling + time-sharing, multi-cloud, energy-aware routing, and the v1.0 platform items.

---

## 10. Sources

Competitive scan performed 2026-07. Primary references:

- **llm-d** — architecture, router/EPP, prefix-cache-aware routing (approximate vs precise), KV-Cache Indexer, KV offloader: `github.com/llm-d/llm-d` docs, `github.com/llm-d/llm-d-router`, `github.com/llm-d/llm-d-kv-cache`.
- **AIBrix** — `aibrix.readthedocs.io`, "Introducing AIBrix" blog (`aibrix.github.io`), and the paper *AIBrix: Towards Scalable, Cost-Effective LLM Inference Infrastructure* (`arxiv.org/abs/2504.03648`).
- **NVIDIA Dynamo** — `developer.nvidia.com/dynamo`, `docs.nvidia.com/dynamo` (overall architecture), Baseten "2x faster inference with KV cache-aware routing".
- **vLLM production-stack** — `vllm.ai/blog/2025-01-21-stack-release`, `docs.vllm.ai` production-stack integration + KV-cache-aware routing tutorial, `github.com/vllm-project/production-stack`.
- **KServe** — `kserve.github.io` v0.18 release notes, `LLMInferenceService` configuration + WVA autoscaling docs.
- **SGLang** — PD disaggregation docs (`docs.sglang.io`), SGLang Model Gateway / router docs, `sglang-router` on PyPI, cache-aware DP routing PR #26561.
- **Mooncake** — `kvcache-ai.github.io/Mooncake`, the paper *Mooncake: A KVCache-centric Disaggregated Architecture for LLM Serving* (`arxiv.org/abs/2407.00079`), Transfer Engine + Store docs.
- **Gateway API Inference Extension** — `gateway-api-inference-extension.sigs.k8s.io` (InferencePool, EPP, BBR), `github.com/kubernetes-sigs/gateway-api-inference-extension`.

---

## Related documents

- [Architecture overview](../architecture/overview.md) — what is implemented today.
- [Router strategies](../architecture/router-strategies.md), [KV cache flow](../architecture/kv-cache-flow.md), [SLO-aware routing](../architecture/slo-aware-routing.md), [key affinity](../architecture/key-affinity.md) — component deep-dives.
- [persistent-affinity-map.md](persistent-affinity-map.md), [kv-cache-hit-rate-collapse.md](kv-cache-hit-rate-collapse.md) — design/investigation records referenced above.
