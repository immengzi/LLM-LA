# Go Services Implementation

## Overview

The Go services (`src/services/go`) are a **100%-parity port** of the Python
`router_service` and `sidecar`. They expose the same HTTP endpoints, read the
same environment variables, write the same Redis keys, emit the same Prometheus
metrics (names/labels/buckets), speak the same ZMQ wire formats, and return
byte-compatible request/response shapes. They are a drop-in replacement that
works out of the box with existing Python configs.

The Python **prefix-hash service** stays in place (Option C): the Go router
calls it over HTTP exactly like the Python router does. The `service_impl`
toggle never swaps the `cpuHash` image.

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
    │   ├── kv_aware.go        # block-owner map + longest-prefix match
    │   ├── hash_client.go     # HTTP client to the Python prefix-hash service
    │   ├── kv_watcher.go      # Redis KV block scanner (per-model)
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
cd src/services/go
./build.sh
# -> reg.local:32000/kv-router-go:latest
# -> reg.local:32000/kv-sidecar-go:latest
```

`build.sh` compiles both binaries statically on the host (`CGO_ENABLED=0`),
packages them into minimal images, and pushes them. `go mod tidy` runs inside
the script, so transitive dependencies (e.g. `go-zeromq/zmq4`) are resolved at
build time.

Custom registry / tag:

```bash
REGISTRY=myregistry.io TAG=v0.1 ./build.sh
```

## How the Toggle Works

`sweep_methods.py` reads `helm.service_impl` from the experiment config and
passes a single Helm value:

```
--set serviceImpl=go
```

The chart helpers `vllmkv.routerImage` / `vllmkv.sidecarImage` (in
`templates/_helpers.tpl`) then select the image:

| serviceImpl | router image          | sidecar image          | prefix hash |
|-------------|-----------------------|------------------------|-------------|
| `python`    | `images.router`       | `images.sidecar`       | Python      |
| `go`        | `images.routerGo`     | `images.sidecarGo`     | Python      |

Everything else — env vars, ports, probes, volumes, RBAC — is identical.

## Running with Go Services

### Via sweep_methods.py

Use any config with `helm.service_impl: "go"`, e.g.
`configs/router-go.yaml` or `configs/prod-shadow-boom-minmax-lmcache-hq-go.yaml`:

```bash
python sweep_methods.py --config <master_config>
```

### Direct Helm override

```bash
helm upgrade --install vllm ./src/vllm-kv-stack \
  --set serviceImpl=go
```

## Parity Surface

| Area | Notes |
|------|-------|
| HTTP endpoints | `/enqueue`, `/submit`, `/pull`, `/result`, `/result_chunk`, `/result_submit`, `/v1/chat/completions`, `/health*`, `/metrics`, `/latency_log`, `/debug/slo`, `/debug/slo/{req_id}` |
| Routing modes | `pull`, `push-rr`, `push-random`, `push-leastq` (both `health` and `local` modes) |
| KV-aware routing | Redis block-owner scan + longest-prefix match via the Python hash service |
| SLO-aware scheduling | slack-based sort, admission throttle, latency predictors (linear + bayesian), batch/queue-wait estimators, full `/debug/slo` |
| Length-aware batching | `short_first`, `long_first`, `even_short_long` |
| Transports | `sync`, `async_pubsub` (ZMQ PUB publisher), `submit_ack` |
| Push decoupling | bounded dispatch queue + worker pool (`PUSH_DECOUPLE_DISPATCH`) with `push_dispatch_*` metrics |
| Multi-model | `MODEL_CONFIG_PATH` registry, per-model queues + per-model KV watcher, 404 on unknown model |
| Sidecar | PREFETCH, FORCE_IGNORE_EOS, STREAMING_MODE + `/result_chunk` forwarding, ZMQ→Redis KV subscriber (msgpack) |
| Tracing | identical `__trace__` field set end-to-end |

### Prometheus Metrics

All metric names, label sets, and histogram buckets are identical between Go and
Python, including `sidecar_python_threads` (the Go sidecar reports its active
worker-routine count under the same metric name).

### Redis Key Patterns

Both implementations use identical Redis key patterns:
- `{MODEL}:kvblock:{hash}` — HASH: block → {pod: timestamp}
- `{MODEL}:podblocks:{pod}` — SET: pod → {block hashes}
- `{MODEL}:kvblocks` — HASH: index of all blocks

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

- **Prefix hash stays Python**: porting vLLM's tokenizer + block-hashing
  internals to Go is out of scope; the Go router calls the Python service over
  HTTP, identical to the Python router. The `service_impl` toggle never swaps
  the `cpuHash` image.
- **Block hashes as decimal strings**: vLLM KV block hashes can exceed
  `int64`. Both the sidecar subscriber and the router store/compare them as
  canonical decimal strings to preserve exact equality without overflow.
```
