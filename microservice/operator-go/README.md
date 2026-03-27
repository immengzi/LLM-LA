# Go Service Implementations (operator-go)

Drop-in Go replacements for the Python router, sidecar, and prefix-hash services.  
Same HTTP endpoints, same env vars, same Helm chart — different language runtime.

## Architecture

```
┌──────────────┐   POST /enqueue   ┌────────────────┐
│   Client     │ ────────────────► │  Router (Go)   │
│   (main.py)  │ ◄──────────────── │  :8080         │
└──────────────┘   {result}        └───────┬────────┘
                                           │ POST /pull (pull mode)
                                           │ POST /push (push mode)
                                           ▼
                                   ┌────────────────┐
                                   │  Sidecar (Go)  │──► vLLM :8200
                                   │  :9000         │◄── ZMQ KV events :5557
                                   └───────┬────────┘
                                           │ POST /result
                                           ▼
                                     Router (Go)
                                           │
                                   ┌───────┴────────┐
                                   │  Redis :6379   │  KV block ownership
                                   └────────────────┘

┌──────────────────┐
│ PrefixHash (Go)  │  POST /compute_hashes
│ :9095            │  Computes prefix block hashes for KV-aware routing
└──────────────────┘
```

## Services

### Router (`cmd/router`)

Central request queue + routing engine. Supports all Python router modes:

| Mode | Description |
|------|-------------|
| `pull` (default) | Sidecars pull work via `POST /pull` |
| `push-rr` | Router pushes to sidecars round-robin |
| `push-random` | Router pushes to random sidecar |
| `push-leastq` | Router pushes to sidecar with shortest queue |

Features:
- **KV-aware routing**: Scores requests by prefix cache hits per sidecar (via Redis)
- **Length-aware batching**: Sorts requests by estimated output length within KV tiers
- **ZMQ PUB**: Optional async result publishing (`TRANSPORT_MODE=async_pubsub`)
- **Prometheus**: `/metrics` endpoint with queue depth, enqueue/result/pull counters

Env vars: Same as Python router (see `pkg/router/config.go`).

### Sidecar (`cmd/sidecar`)

Per-vLLM-pod request executor. Runs alongside each vLLM instance.

Features:
- **Pull mode**: Fetches batches from router's `/pull` endpoint
- **Push mode**: Accepts pushed requests on `/push`
- **Worker pool**: Concurrent vLLM requests (`BATCH_SIZE` workers)
- **KV subscriber**: Listens to vLLM ZMQ for KV cache events, writes to Redis
- **Result poster**: Sends completed results back to router's `/result`

Env vars: Same as Python sidecar (see `pkg/sidecar/config.go`).

### Prefix Hash (`cmd/prefixhash`)

CPU-only service computing prefix block hashes for KV-aware routing.

Endpoint: `POST /compute_hashes` with `{"prompt": "..."}` returns `{"block_hashes": [...]}`

## HTTP Endpoints (backward compatible)

### Router
| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | `{"status":"ok","queue_len":N}` |
| GET | `/metrics` | Prometheus metrics |
| POST | `/enqueue` | Sync enqueue (blocks until result) |
| POST | `/submit` | Async submit (returns `req_id`) |
| POST | `/pull` | Sidecar pulls work batch |
| POST | `/result` | Sidecar posts completed result |

### Sidecar
| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | `{"status":"ok","pending":N,"inflight":N,"logical":N}` |
| GET | `/metrics` | Prometheus metrics |
| POST | `/push` | Accept pushed request (push mode) |

### Prefix Hash
| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | `{"status":"ok"}` |
| GET | `/metrics` | Prometheus metrics |
| POST | `/compute_hashes` | Compute block hashes |

## Switching Between Python and Go

### Method 1: Helm values override (recommended)

```bash
# Deploy with Go images
helm upgrade --install vllm ./vllm-kv-stack -n vllm \
  --set images.router=kv-router-go:latest \
  --set images.sidecar=kv-sidecar-go:latest \
  --set images.cpuHash=kv-prefixhash-go:latest

# Revert to Python images (default)
helm upgrade --install vllm ./vllm-kv-stack -n vllm
```

### Method 2: sweep_methods.py config

In your client YAML:

```yaml
helm:
  service_impl: go     # uses Go images
  # service_impl: python  # default, uses Python images
```

### Method 3: Direct values.yaml edit

```yaml
images:
  router: kv-router-go:latest       # was kv-router:latest
  sidecar: kv-sidecar-go:latest     # was kv-sidecar:latest
  cpuHash: kv-prefixhash-go:latest  # was vllm-cpu-hash:latest
```

## CRDs (Optional)

CRDs provide Kubernetes-native configuration. They are **not required** for
basic operation — the Go services work with the existing Helm chart unchanged.

### Install CRDs

```bash
kubectl apply -f config/crd/
kubectl apply -f config/rbac/
```

### Create resources

```bash
kubectl apply -f config/samples/router.yaml
kubectl apply -f config/samples/sidecar.yaml
kubectl apply -f config/samples/prefixhash.yaml
```

### CRD types

| CRD | Short | Description |
|-----|-------|-------------|
| `VllmRouter` | `vr` | Router configuration |
| `VllmSidecar` | `vs` | Sidecar configuration |
| `VllmPrefixHash` | `vph` | Prefix hash configuration |

```bash
kubectl get vr,vs,vph -n vllm
```

## Building

### Prerequisites

- Go 1.22+
- Docker

### Build all images

```bash
cd operator-go
make build-all
```

Or individually:

```bash
make build-router
make build-sidecar
make build-prefixhash
```

### First-time setup

```bash
go mod tidy   # generates go.sum
make build-all
```

### Deploy CRDs + RBAC

```bash
make deploy
```

### Full build + deploy

```bash
make deploy-build
```

## Folder Structure

```
operator-go/
├── api/v1alpha1/          # CRD Go types
│   └── types.go
├── cmd/
│   ├── router/main.go     # Router entrypoint
│   ├── sidecar/main.go    # Sidecar entrypoint
│   └── prefixhash/main.go # Prefix hash entrypoint
├── config/
│   ├── crd/               # CRD manifests
│   ├── rbac/              # ServiceAccount, Role, RoleBinding
│   └── samples/           # Example CR YAMLs
├── pkg/
│   ├── models/types.go    # Shared request/response types
│   ├── router/            # Router implementation
│   │   ├── config.go      # Env var config
│   │   ├── state.go       # Central queue + result store
│   │   ├── pull.go        # Pull-mode logic + KV/len sorting
│   │   ├── push.go        # Push-mode dispatcher
│   │   ├── discovery.go   # K8s pod discovery
│   │   ├── hashclient.go  # Prefix-hash HTTP client
│   │   ├── pubsub.go      # ZMQ PUB for async results
│   │   ├── kvwatcher.go   # Redis KV block scanner
│   │   ├── metrics.go     # Prometheus counters/gauges
│   │   └── server.go      # HTTP handler
│   ├── sidecar/           # Sidecar implementation
│   │   ├── config.go      # Env var config
│   │   ├── queue.go       # Local bounded queue
│   │   ├── worker.go      # vLLM request worker
│   │   ├── puller.go      # Router pull loop
│   │   ├── poster.go      # Result poster
│   │   ├── subscriber.go  # ZMQ KV event subscriber
│   │   ├── metrics.go     # Prometheus counters/gauges
│   │   └── server.go      # HTTP handler
│   └── prefixhash/        # Prefix hash implementation
│       └── server.go      # HTTP handler + hash logic
├── scripts/
│   ├── build-router.sh
│   ├── build-sidecar.sh
│   ├── build-prefixhash.sh
│   ├── build-all.sh
│   └── deploy.sh
├── Dockerfile.router
├── Dockerfile.sidecar
├── Dockerfile.prefixhash
├── Makefile
├── go.mod
├── CHANGELOG.md
└── README.md              # This file
```

## Reverting to Python

The Python services under `services/` are completely untouched. To revert:

1. **Helm**: Remove the `images.*` overrides (or set them back to Python values)
2. **sweep_methods.py**: Set `service_impl: python` (or remove the field — `python` is default)
3. **CRDs**: `kubectl delete -f config/crd/` to remove CRDs (optional; they don't interfere)

No Helm template changes, no Python code changes, no cleanup required.
