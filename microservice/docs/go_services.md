# Go Services Implementation

## Overview

The Go services are wire-compatible reimplementations of the Python `router_service` and `sidecar` in Go. They expose the same HTTP endpoints, read the same environment variables, write the same Redis keys, and emit the same Prometheus metrics. The prefix hash service remains in Python (Option C).

Switching between Python and Go is a single config line:

```yaml
helm:
  service_impl: "go"   # "python" (default) | "go"
```

No client code, Helm templates, or experiment configs need to change.

## Source Layout

```
services/go/
├── go.mod
├── go.sum
├── build.sh                  # Build + tag Docker images
├── Dockerfile.router         # Multi-stage build for kv-router-go
├── Dockerfile.sidecar        # Multi-stage build for kv-sidecar-go
├── cmd/
│   ├── gateway/main.go       # Router entrypoint
│   └── sidecar/main.go       # Sidecar entrypoint
└── internal/
    ├── common/
    │   ├── config.go          # Env var helpers (EnvStr, EnvInt, EnvFloat, EnvBool)
    │   └── health.go          # Generic health handler
    ├── gateway/
    │   ├── config.go          # Router config (all env vars)
    │   ├── models.go          # Request/response types + QueueEntry
    │   ├── metrics.go         # Prometheus metrics (router_*)
    │   ├── queue.go           # CentralQueue: FIFO + KV-aware + length-aware
    │   ├── result_store.go    # Channel-based result correlation
    │   ├── handlers.go        # HTTP handlers (chi router)
    │   ├── kv_watcher.go      # Redis KV block scanner
    │   └── push_router.go     # K8s pod discovery + push dispatch
    └── sidecar/
        ├── config.go          # Sidecar config (all env vars)
        ├── metrics.go         # Prometheus metrics (sidecar_*)
        ├── queue.go           # LocalQueue (pending + inflight tracking)
        ├── pull_worker.go     # Event-driven pull from router
        ├── vllm_worker.go     # vLLM chat completion client
        ├── result_poster.go   # Async result posting to router
        └── kv_subscriber.go   # ZMQ KV event stub (interface ready for future impl)
```

## Building Docker Images

### Prerequisites

- Docker installed
- Network access to Go module mirrors (or proxy configured)

### Quick Build

```bash
cd services/go
./build.sh
```

This builds:
- `reg.local:32000/kv-router-go:latest`
- `reg.local:32000/kv-sidecar-go:latest`

### Custom Registry or Tag

```bash
REGISTRY=myregistry.io TAG=v0.1 ./build.sh
```

### Behind a Proxy

```bash
HTTP_PROXY=http://proxy:8080 HTTPS_PROXY=http://proxy:8080 ./build.sh
```

### Push to Registry

```bash
docker push reg.local:32000/kv-router-go:latest
docker push reg.local:32000/kv-sidecar-go:latest
```

## How the Toggle Works

`sweep_methods.py` reads `helm.service_impl` from the experiment config:

- When `"python"` (default): uses `kv-router:latest`, `kv-sidecar:latest`, `vllm-cpu-hash:latest`
- When `"go"`: overrides to `kv-router-go:latest`, `kv-sidecar-go:latest` — prefix hash stays `vllm-cpu-hash:latest`

The Helm templates are image-agnostic — they reference `{{ .Values.images.router }}`, etc.

## Running with Go Services

### Option A: Via sweep_methods.py

Use `configs/router-go.yaml`:

```bash
python sweep_methods.py
```

This config is identical to `router.yaml` except for `helm.service_impl: "go"`.

### Option B: Direct Helm Override

```bash
helm upgrade --install vllm-kv-stack ./vllm-kv-stack \
  --set images.router=kv-router-go:latest \
  --set images.sidecar=kv-sidecar-go:latest
```

## API Compatibility

### Router (Go vs Python)

| Endpoint | Method | Go | Python |
|---|---|---|---|
| `/health` | GET | `{"status":"ok","queue_len":N}` | Same |
| `/metrics` | GET | Prometheus text | Same |
| `/enqueue` | POST | Sync (blocks until result) | Same |
| `/submit` | POST | Async (202 + req_id) | Same |
| `/pull` | POST | `{"items":[...]}` | Same |
| `/result` | POST | `{"status":"ok"}` | Same |
| `/v1/chat/completions` | POST | OpenAI format | Same |
| `/debug/slo` | GET | `{}` (stub) | Full SLO debug |
| `{RESULT_SUBMIT_PATH}` | POST | 202 + async ingest | Same |
| `{SUBMIT_PATH}` | POST | Same as /submit | Same |

### Sidecar (Go vs Python)

| Endpoint | Method | Go | Python |
|---|---|---|---|
| `/health` | GET | `{"status":"ok","queue_len":N,"inflight":M,"logical":N+M}` | Same |
| `/metrics` | GET | Prometheus text | Same |
| `/push` | POST | `{"status":"ok"}` | Same |

### Prometheus Metrics

All metric names and label sets are identical between Go and Python, with one exception:

- Python: `sidecar_python_threads` (number of Python threads)
- Go: `sidecar_goroutines` (number of goroutines)

### Redis Key Patterns

Both implementations use identical Redis key patterns:
- `{MODEL}:kvblock:{hash}` — HASH: block → {pod: timestamp}
- `{MODEL}:podblocks:{pod}` — SET: pod → {block hashes}
- `{MODEL}:kvblocks` — HASH: index of all blocks

### ZMQ

The Go sidecar currently runs a stub KV subscriber that logs a warning. KV-aware routing still works because the KV watcher in the router reads Redis directly (populated by whichever sidecar is running). A full ZMQ implementation can be added later via the `KVSubscriber` interface.

## Environment Variables

Both Go and Python services read the same environment variables with the same defaults. See:
- Router: `internal/gateway/config.go`
- Sidecar: `internal/sidecar/config.go`

## Known Differences (v0.1)

1. **SLO debug endpoint**: Go returns `{}` stub. Python returns full SLO state. SLO-aware scheduling logic in the queue is a framework ready for future implementation.

2. **ZMQ KV subscriber**: Go sidecar uses a stub. KV data still flows because the Python prefix hash service and any Python sidecars populate Redis. The Go router's KV watcher reads from Redis regardless.

3. **Prefix hash**: Stays in Python. The Go `service_impl` toggle does not swap the cpuHash image.

4. **Push mode K8s discovery**: Go uses raw HTTP to the K8s API with service account tokens. Python uses the `kubernetes` client library. Both produce the same pod list.
