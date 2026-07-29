# Go Services Implementation

## Overview

The Go services (`src/core/services/go`) are a **near-parity port** of the Python
`router_service` and `sidecar`. They expose the same HTTP endpoints, read the
same environment variables, write the same Redis keys, emit the same Prometheus
metrics (names/labels/buckets), speak the same ZMQ wire formats, and return
compatible request/response shapes. They are a drop-in replacement that
works with existing Python configs.

Known differences: the SLO `piecewise` latency predictor is not implemented in
either runtime (it falls back to `linear`); and internally the Go router keeps
KV block hashes as decimal strings while the Python router parses them to `int`
(equivalent for typical hashes).

**KV-block hashing**: by default (`KV_HASH_SOURCE=inline`) the Go router hashes
in-container using the same `router/prefix_hash.py` as the Python router (shipped
inside the gateway image, served on `127.0.0.1:9095`), so no external pod is
needed and Go/Python hashes match. The legacy standalone `vllm-cpu-hash` service
remains available as an opt-in (`KV_HASH_SOURCE=external`) and is auto-deployed
only in that mode. See [prefix-hash.md](prefix-hash.md).

The Go router and sidecar support both chart engines: vLLM and the pinned
SGLang v0.5.15 profile. For SGLang, the image stages the complete shared hashing
package (`prefix_hash.py` plus `hash_backends`), and the sidecar implements
descriptor discovery, replay-aware KV projection, `/ready`, and `/health`.

Switching between Python and Go is a single config line:

```yaml
helm:
  service_impl: "go"   # "python" (default) | "go"
```

No client code, Helm templates, or experiment configs need to change.

## Source Layout

```
services/go/
├── go.mod / go.sum
├── build.sh                  # host compile + minimal image build + push
├── Dockerfile.router         # kv-router-go
├── Dockerfile.sidecar        # kv-sidecar-go
├── cmd/
│   ├── gateway/main.go       # router entrypoint (wires queue, SLO, pubsub, push-dispatch)
│   └── sidecar/main.go       # sidecar entrypoint (workers, KV subscriber, pull worker)
└── internal/
    ├── common/               # env helpers + generic health
    ├── gateway/              # router
    │   ├── config.go          # all router env vars
    │   ├── models.go          # request/response types (+ SLO fields)
    │   ├── metrics.go         # router_* + push-dispatch metrics
    │   ├── queue.go           # CentralQueue: per-model FIFO + KV-aware + length-aware + SLO hooks
    │   ├── result_store.go    # result correlation
    │   ├── handlers.go        # HTTP handlers (chi)
    │   ├── kv_aware.go        # per-request owner map (reqOwners) + block-owner map + longest-prefix match
│   ├── affinity.go        # conversation key-affinity map (TTL, prefetch, warm, cache bound)
│   ├── affinity_store.go  # durable Redis-backed affinity store (async write-through + warm)
    │   ├── owner_lookup.go    # default owner source: targeted per-request Redis HGETALL (KV_OWNER_SOURCE=lookup)
    │   ├── hash_client.go     # KV-hash client: in-container hasher (inline) or legacy service (external)
    │   ├── kv_watcher.go      # legacy owner source + pod discovery: Redis KV block scanner (KV_OWNER_SOURCE=watcher)
    │   ├── model_registry.go  # multi-model registry + Resolve / 404
    │   ├── predictors.go      # output-length predictors (singleton)
    │   ├── latency_predictor.go  # linear + bayesian latency models
    │   ├── slo_state.go       # per-request SLO registry
    │   ├── slo_scoring.go     # batch/queue-wait estimators + computeSlack
    │   ├── admission.go       # computeMaxSafeAdmit
    │   ├── slo.go             # SLO engine (sloEngine + sloRegistry)
    │   ├── pubsub.go          # ZMQ result publisher (async_pubsub)
    │   ├── push_router.go     # K8s pod discovery + push dispatch (rr/random/leastq)
    │   ├── push_dispatch.go   # decoupled push-dispatch queue + worker pool
    │   ├── chat.go            # /v1/chat/completions (API key, streaming SSE, tool calls)
    │   ├── health_backends.go # backend health aggregation
    │   └── parity_test.go     # unit tests
    └── sidecar/              # sidecar
        ├── config.go          # all sidecar env vars
        ├── metrics.go         # sidecar_* metrics
        ├── queue.go           # LocalQueue (pending + inflight)
        ├── pull_worker.go     # pull from router (PREFETCH-aware)
        ├── vllm_worker.go     # vLLM client (streaming, tool calls, FORCE_IGNORE_EOS, trace)
        ├── result_poster.go   # async result posting
        ├── kv_subscriber.go   # ZMQ KV-event subscriber -> Redis (msgpack)
        └── kv_subscriber_test.go  # unit tests
```

## Building Docker Images

```bash
cd src/core/services/go
./build.sh
# -> reg.local:32000/kv-router-go:latest
# -> reg.local:32000/kv-sidecar-go:latest
```

`build.sh` compiles both binaries statically on the host (`CGO_ENABLED=0`),
packages them into minimal images, and pushes them. `go mod tidy` runs inside
the script, so transitive dependencies (e.g. `go-zeromq/zmq4`) are resolved at
build time. It sources the shared `../build-common.sh` for cluster-agnostic
registry/proxy resolution.

Registry naming: the cluster-facing image name is `reg.local:32000/...`, but the
push goes to `PUSH_REGISTRY` (default `localhost:32000`) — the same registry
exposed on every k8s node's NodePort and the only insecure (HTTP) target the
local Docker daemon trusts. Both names address one registry, so the cluster
still pulls `reg.local:32000/kv-router-go:latest`.

Custom registry / tag:

```bash
TAG=v0.1 ./build.sh                       # change the image tag
PUSH_REGISTRY=myregistry.io:5000 ./build.sh   # push to a different registry
REGISTRY=myregistry.io ./build.sh         # change the cluster-facing pull name
```

Build and test without relying on the host Go installation:

```bash
docker run --rm \
  -v "$PWD/src/core/services/go":/src \
  -v llm-la-go-mod-cache:/go/pkg/mod \
  -w /src golang:1.26.5 \
  sh -c 'test -z "$(gofmt -l .)" && go vet ./... && go test ./...'
```

## How the Toggle Works

`sweep_methods.py` reads `helm.service_impl` from the experiment config and
passes a single Helm value:

```
--set serviceImpl=go
```

The chart helpers `vllmkv.routerImage` / `vllmkv.sidecarImage` (in
`templates/_helpers.tpl`) then select the image:

| serviceImpl | router image          | sidecar image          | prefix hash (inline default) |
|-------------|-----------------------|------------------------|------------------------------|
| `python`    | `images.router`       | `images.sidecar`       | in-process `prefix_hash.py`  |
| `go`        | `images.routerGo`     | `images.sidecarGo`     | in-container `prefix_hash.py` |

Everything else — env vars, ports, probes, volumes, RBAC — is identical.

## Running with Go Services

### Via sweep_methods.py

Use any config with `helm.service_impl: "go"`, e.g.
`configs/old/non-prod/router-go.yaml` or `configs/old/shadow/prod-yz-shadow-boom-minmax-lmcache-hq-go.yaml`:

```bash
python src/client/sweep_methods.py --config <master_config>
```

### Direct Helm override

```bash
helm upgrade --install vllm ./src/core/vllm-kv-stack \
  --set serviceImpl=go
```

## Parity Surface

| Area | Notes |
|------|-------|
| HTTP endpoints | `/enqueue`, `/submit`, `/pull`, `/result`, `/result_chunk`, `/v1/chat/completions`, `/health*`, `/metrics`, `/latency_log`, `/debug/slo`, `/debug/slo/{req_id}`; `/result_submit` only when `RESULT_TRANSPORT_MODE=submit_ack` |
| Routing modes | `pull`; push: `push-rr`, `push-random`, `push-leastq` (`health`/`local`), `push-throughput`, `push-p2c`, `push-kv-cost`, `push-least-kv`, `push-least-latency`, `push-least-busy`; plus `central-push` / `external-push` ([router.md](router.md); [compatibility matrix](router.md#routing-compatibility-matrix)) |
| KV-aware routing | Owner source `lookup` (default: targeted per-request Redis `HGETALL`, `KVLookupMaxBlocks` cap) or `watcher` (legacy background scan) + longest-prefix match; request hashes from the in-container `prefix_hash.py` (inline) or the legacy `vllm-cpu-hash` service (external) |
| Key-affinity | `soft` / `hard` modes, hard-timeout hold + release, and optional **persistent (Redis-backed) affinity** (`AFFINITY_PERSIST_ENABLED`): write-through claims, startup warm, admission prefetch, per-pod readiness (`AFFINITY_ENDPOINT_STALE_S`) + cache bound (`AFFINITY_CACHE_MAX`) |
| Fairness & soft KV divert | Load-aware fair-pull throttle (`ROUTER_FAIR_*`), stuck-pod release (`ROUTER_STUCK_PULL_SECONDS`, `ROUTER_AFFINITY_RELEASE_ON_STUCK`), and **soft KV divert** (`ROUTER_KV_SOFT_DIVERT`): trims cold work off GPU-KV-saturated pods (hysteresis + healthy-peer gate) using sidecar `kv_usage` from `/pull` and central-push `/health` polls |
| SLO-aware scheduling | slack-based sort, admission throttle, latency predictors (`linear` default, `bayesian`/`hybrid`; `piecewise` accepted but not implemented), batch/queue-wait estimators, full `/debug/slo` |
| Length-aware batching | `short_first`, `long_first` |
| Client transports | `sync`, `async_pubsub` (ZMQ PUB publisher). Note: `submit_ack` is a router-side `RESULT_TRANSPORT_MODE` (sidecar→router result delivery), not a client transport mode. |
| Push decoupling | bounded dispatch queue + worker pool (`PUSH_DECOUPLE_DISPATCH`) with `push_dispatch_*` metrics |
| Multi-model | `MODEL_CONFIG_PATH` registry, per-model queues + per-model KV watcher, 404 on unknown model |
| Sidecar | PREFETCH, FORCE_IGNORE_EOS, STREAMING_MODE + `/result_chunk` forwarding, ZMQ→Redis KV subscriber (msgpack), SLO-driven dynamic pull backpressure (`SLO_DYNAMIC_PULL_ENABLED`), GPU KV-usage reporting on `/pull` + `/health` (`KV_USAGE_REPORT`) |
| Token-aware pull | P0 in-flight token gauge (`router_endpoint_inflight_tokens` + `__isl_tokens__` on the request meta), P1 sidecar KV-memory pull gate (`KV_PULL_GATE_ENABLED/HIGH/LOW`, `sidecar_kv_pull_gate_*`), P2 router prefill-token budget (`PULL_BUDGET_ENABLED`/`PREFILL_TOKEN_BUDGET`, per-pull `want_prefill_tokens`, `router_pull_granted_prefill_tokens`, `router_pull_budget_bound_total`) — same env vars, metric names, and algorithm. See [router.md](router.md) §5e, [sidecar.md](sidecar.md#kv-memory-pull-gate), and the [compatibility matrix](router.md#routing-compatibility-matrix) |
| SGLang v0.5.15 | Shared Python hash backend in the Go router image; native Go descriptor discovery, live/replay KV events, fail-closed Redis projection, `/ready` requiring KV visibility, and `/health` liveness |
| Tracing | identical `__trace__` field set end-to-end |

### Prometheus Metrics

All metric names, label sets, and histogram buckets are identical between Go and
Python, including `sidecar_python_threads` (the Go sidecar reports its active
worker-routine count under the same metric name).

### Redis Key Patterns

Identical to the Python implementation — see the [Redis key schema](kv-cache-flow.md#redis-key-schema) in the KV cache flow doc.

### ZMQ

- **Sidecar**: subscribes to vLLM KV-cache events (`kv@` topic), decodes the
  msgpack `KVEventBatch` (`BlockStored` / `BlockRemoved` / `AllBlocksCleared`),
  and mirrors block ownership into Redis with the exact schema above.
- **Router**: when `TRANSPORT_MODE=async_pubsub`, publishes completed results on
  a ZMQ PUB socket using the same `[topic.run_id, compact-json]` wire format.

## Environment Variables

Both Go and Python services read the same environment variables with the same
defaults. See `internal/gateway/config.go` (router) and
`internal/sidecar/config.go` (sidecar).

## Notes

- **Hashing stays Python (shared code), but in-container by default**: porting
  the HF tokenizer + chat template + block-hashing to Go is out of scope, so the
  Go gateway runs the router's exact `prefix_hash.py` inside its own container
  (inline mode) for byte-identical hashes with no external pod. The legacy
  standalone `vllm-cpu-hash` service is opt-in via `KV_HASH_SOURCE=external`.
- **Block hashes as decimal strings**: vLLM KV block hashes can exceed
  `int64`. Both the sidecar subscriber and the router store/compare them as
  canonical decimal strings to preserve exact equality without overflow.
