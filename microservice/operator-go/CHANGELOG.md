# Changelog

All files in the `operator-go/` folder are **new**. No existing files were
removed or broken. Only two existing files received backward-compatible additions.

## New files (operator-go/)

| File | Description |
|------|-------------|
| `go.mod` | Go module with dependencies (zmq4, redis, prometheus, k8s client-go) |
| `api/v1alpha1/types.go` | CRD Go type definitions (VllmRouter, VllmSidecar, VllmPrefixHash) |
| `cmd/router/main.go` | Router binary entrypoint |
| `cmd/sidecar/main.go` | Sidecar binary entrypoint |
| `cmd/prefixhash/main.go` | Prefix hash binary entrypoint |
| `config/crd/vllmrouters.yaml` | VllmRouter CRD manifest |
| `config/crd/vllmsidecars.yaml` | VllmSidecar CRD manifest |
| `config/crd/vllmprefixhashes.yaml` | VllmPrefixHash CRD manifest |
| `config/rbac/service-account.yaml` | ServiceAccount for Go router |
| `config/rbac/role.yaml` | RBAC Role (pod list/watch + CRD access) |
| `config/rbac/role-binding.yaml` | RoleBinding |
| `config/samples/router.yaml` | Example VllmRouter CR |
| `config/samples/sidecar.yaml` | Example VllmSidecar CR |
| `config/samples/prefixhash.yaml` | Example VllmPrefixHash CR |
| `pkg/models/types.go` | Shared Go types matching Python request/response schemas |
| `pkg/router/config.go` | Router env var configuration (same vars as Python) |
| `pkg/router/state.go` | Central queue, result store, KV block tracking |
| `pkg/router/pull.go` | Pull-mode routing with KV-aware + length-aware sorting |
| `pkg/router/push.go` | Push-mode dispatcher (round-robin, random, least-queue) |
| `pkg/router/discovery.go` | Kubernetes pod discovery for sidecar endpoints |
| `pkg/router/hashclient.go` | HTTP client for prefix-hash service |
| `pkg/router/pubsub.go` | ZMQ PUB socket for async result publishing |
| `pkg/router/kvwatcher.go` | Redis scanner for KV block ownership |
| `pkg/router/metrics.go` | Prometheus metrics registration |
| `pkg/router/server.go` | HTTP handlers (/health, /enqueue, /submit, /pull, /result) |
| `pkg/sidecar/config.go` | Sidecar env var configuration (same vars as Python) |
| `pkg/sidecar/queue.go` | Local bounded queue with inflight tracking |
| `pkg/sidecar/worker.go` | vLLM request worker (concurrent pool) |
| `pkg/sidecar/puller.go` | Router pull loop |
| `pkg/sidecar/poster.go` | Result poster to router |
| `pkg/sidecar/subscriber.go` | ZMQ subscriber for vLLM KV cache events |
| `pkg/sidecar/metrics.go` | Prometheus metrics registration |
| `pkg/sidecar/server.go` | HTTP handlers (/health, /push) |
| `pkg/prefixhash/server.go` | Prefix hash HTTP handler + block hashing logic |
| `Dockerfile.router` | Multi-stage Go build for router |
| `Dockerfile.sidecar` | Multi-stage Go build for sidecar |
| `Dockerfile.prefixhash` | Multi-stage Go build for prefix hash |
| `scripts/build-router.sh` | Build + push router image (same conventions as Python) |
| `scripts/build-sidecar.sh` | Build + push sidecar image |
| `scripts/build-prefixhash.sh` | Build + push prefix hash image |
| `scripts/build-all.sh` | Build all three images |
| `scripts/deploy.sh` | Deploy CRDs + RBAC |
| `Makefile` | Build/deploy/test targets |
| `README.md` | Full documentation |
| `CHANGELOG.md` | This file |

## Modified existing files (backward compatible)

### `config.py`

Added one field to `HelmConfig`:

```python
service_impl: str = "python"  # python | go
```

Default is `"python"` — no behavior change unless explicitly set to `"go"`.

### `sweep_methods.py`

Added 4 lines in the `set_values` construction block:

```python
service_impl = str(getattr(h, "service_impl", "python")).strip().lower()
if service_impl == "go":
    set_values["images.router"] = "kv-router-go:latest"
    set_values["images.sidecar"] = "kv-sidecar-go:latest"
    set_values["images.cpuHash"] = "kv-prefixhash-go:latest"
```

Added `service_impl` to sweep metadata snapshot. Default `"python"` — no behavior
change for existing configs.

## Files NOT modified

- `services/` — Python services completely untouched
- `vllm-kv-stack/` — Helm templates completely untouched
- `main.py` — Client code untouched
- All existing config YAMLs — untouched
