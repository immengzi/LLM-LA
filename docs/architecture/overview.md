# Architecture overview

How the LA-Boom serving platform fits together. Start here, then dive into the component docs linked below.

## The big picture

```mermaid
flowchart TB
  Client["Client / Gateway"]
  Router["Router :8080 / :30080"]
  Hash["Prefix-Hash :9095"]
  Redis[("Redis :6379")]

  subgraph pod [vLLM pod]
    Sidecar["Sidecar :9000"] --> vLLM["vLLM :8200"]
  end

  Client -->|"/enqueue or /submit"| Router
  Router -->|compute_hashes| Hash
  Router -->|"pull / push"| Sidecar
  Router -->|"SCAN {model}:kvblock:*"| Redis
  Sidecar -->|"ZMQ kv@ events -> Redis"| Redis
  Sidecar -->|"/result or ZMQ"| Router
  Router -->|result| Client
```

LA-Boom places a custom **router** and per-pod **sidecars** around stock vLLM, plus a **prefix-hash** service and **Redis** for KV-aware placement.

## Components

| Component | Role | Port (ClusterIP / NodePort) | Deep dive |
|-----------|------|------------------------------|-----------|
| Router | Central queue, pull/push dispatch, KV/length/SLO scheduling, OpenAI shim | 8080 / 30080 (ZMQ results 5559 / 30559) | [router.md](router.md) |
| Sidecar | Per-pod local queue, vLLM forwarding, KV event reporting | 9000 (in-pod) | [sidecar.md](sidecar.md) |
| vLLM | Model inference (OpenAI-compatible API) | 8200 / 30034 (+offset) | — |
| Prefix-Hash | Computes vLLM-compatible KV block hashes | 9095 / 30095 | [prefix-hash.md](prefix-hash.md) |
| Redis | KV block ownership (`{MODEL}:kvblock:*`) | 6379 / 30079 | [kv-cache-flow.md](kv-cache-flow.md) |

## Request lifecycle

1. A client sends a request to the router via `/enqueue` (synchronous, blocks for the result) or `/submit` (asynchronous, returns a `req_id`; results arrive over ZMQ).
2. If KV-aware routing is on, the router asks the prefix-hash service for the request's block hashes and records them in memory.
3. In **pull** mode, sidecars poll `/pull` when they have capacity; the router scores and returns the best-matching queued requests. In **push** mode, the router dispatches proactively (`push-rr`, `push-random`, `push-leastq`).
4. The sidecar forwards the request to its local vLLM and returns the result to the router (via `/result` or, for async, the router publishes over ZMQ).

## KV-aware routing in one paragraph

Each sidecar subscribes to vLLM's ZMQ KV-cache events and writes block ownership to Redis. The router runs a background watcher that scans Redis to build a live `block hash -> replica` map. At dispatch time the router scores each queued request by how many of its leading block hashes are already cached on a given replica, groups requests into KV-hit tiers, and (when length-aware routing is enabled) orders within each tier by predicted output length. Full detail: [kv-cache-flow.md](kv-cache-flow.md).

## Routing modes and policies

- **Dispatch modes**: `pull` (capacity-gated, default), `push-rr`, `push-random`, `push-leastq`.
- **Length-aware policies**: `short_first`, `long_first` (applied within KV tiers).
- **SLO-aware scheduling**: an alternative slack-based sort that orders by deadline headroom; see [slo-aware-routing.md](slo-aware-routing.md).

## Implementations

The router and sidecar exist in both **Python** (FastAPI) and **Go** (chi), selectable via the Helm value `serviceImpl`. They are near-identical ports sharing the same APIs, Redis schema, ZMQ formats, and metric names; the prefix-hash service stays Python for both. See [go-services.md](go-services.md).

## Transports

- **Synchronous**: client `POST /enqueue` blocks until the result is ready.
- **Asynchronous pub/sub**: client `POST /submit` returns immediately; the router publishes completions over ZMQ (`tcp://<router>:5559` / NodePort 30559), and the client subscribes.

## See also

- [Request tracing](trace.md) — per-request stage timing
- [Configuration: client config](../configuration/client-config.md) and [Helm values](../configuration/helm-values.md)
