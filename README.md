# LA-Boom

**A Kubernetes-native, KV-aware load balancer and benchmark framework for vLLM serving.**

LA-Boom is two things in one repository: a **distributed serving platform** (router + sidecar microservices around vLLM) and a **reproducible benchmark harness** (open-loop load generator + automated Helm sweep runner). Together they let you deploy, route, and systematically evaluate LLM inference at scale on Kubernetes.

```
Client --> Router --> Sidecar --> vLLM
              |                    |
            Redis            KV events (ZMQ)
              |                    |
         Prefix Hash         Prometheus
```

---

## Key Capabilities

| Area | What LA-Boom provides |
|------|----------------------|
| **Routing** | Pull mode (capacity-gated), push modes (round-robin, random, least-queue), KV-aware prefix affinity, length-aware batching (short-first, long-first, even), SLO-aware scheduling |
| **Serving** | Multi-model from a shared cluster, data-parallel via LeaderWorkerSet, cross-node KV transfer (Mooncake), Go and Python service implementations |
| **Benchmarking** | Open-loop load generator with configurable RPS patterns (deterministic, Poisson, bursty, stepped), multi-turn conversations, multi-model client routing, streaming TTFT/TPOT measurement |
| **Experiment automation** | Helm-based sweep runner that varies routing methods, replicas, and backends while capturing per-request NDJSON logs, Prometheus metrics, and latency traces |
| **Production gateways** | BooM Gateway (Rust) and LiteLLM (Python) for auth, virtual keys, rate limiting, and spend tracking |
| **Observability** | Prometheus scraping, per-request tracing, Redis KV state inspection, K8s pod-node event logging |

---

## Repository Layout

```
.
├── src/                             # All source code and configs
│   ├── main.py                      # Load experiment entry point
│   ├── sweep_methods.py             # Automated Helm sweep runner
│   ├── deploy_vllm.py               # vLLM-only deployment script
│   ├── config.py                    # Configuration dataclasses
│   ├── load_runner.py               # Open-loop load generation engine
│   ├── http_client.py               # HTTP clients for all backends
│   ├── scheduler.py                 # RPS schedule patterns
│   ├── prompts.py                   # Prompt sources (JSON, LMSYS, CodeFlowBench)
│   ├── metrics_prom.py              # Prometheus metrics collector
│   ├── trace_utils.py               # End-to-end latency breakdown
│   ├── experiment_io.py             # Experiment directory and log management
│   ├── configs/                     # Client + sweep YAML configs
│   │   ├── 1-master_config.yaml     # Master sweep definition
│   │   ├── router-tp8-glm.yaml     # GLM-5 TP8 example
│   │   ├── boom-claude.yaml         # Claude via BooM
│   │   └── ...                      # ~50 experiment templates
│   ├── services/                    # Microservice implementations
│   │   ├── router_service/          # Router (Python)
│   │   ├── sidecar/                 # Sidecar (Python)
│   │   ├── go/                      # Router + Sidecar (Go)
│   │   └── prefix_hash/            # Prefix hash computation
│   ├── vllm-kv-stack/              # Helm chart
│   │   ├── Chart.yaml
│   │   ├── values.yaml
│   │   └── templates/              # K8s manifests (Redis, Router, vLLM, BooM, ...)
│   └── jupyters/                   # Post-experiment analysis notebooks
└── docs/                           # Documentation
    ├── quickstart.md               # Deployment & experiment walkthrough
    ├── config_knobs.md             # Full configuration reference
    ├── router_service.md           # Router architecture
    ├── sidecar.md                  # Sidecar architecture
    ├── kv_cache_flow.md            # KV-aware routing deep dive
    ├── boom_gateway.md             # BooM Gateway docs
    ├── mooncake_integration.md     # Cross-node KV transfer
    ├── multi_model_router.md       # Multi-model serving
    ├── slo_aware_routing.md        # SLO-aware scheduling
    ├── data_parallel_lws.md        # Data-parallel deployment
    ├── trace.md                    # Request tracing
    └── ...
```

---

## Quick Start

### Prerequisites

- Kubernetes cluster with `kubectl` and `helm` (>= 3.12)
- Python 3.10+ with `pyyaml`, `requests`, `click`
- NFS or local model storage accessible from worker nodes
- Private registry at `reg.local:32000` (for custom images)

### 1. One-time PV/PVC setup

```bash
helm upgrade --install vllm ./src/vllm-kv-stack -n vllm --create-namespace \
  --set modelVolume.create=true \
  --set modelVolume.modelSubPath=placeholder
```

### 2. Deploy vLLM

```bash
cd src
python deploy_vllm.py --config configs/router-tp8-glm.yaml
kubectl get pods -n vllm -w   # wait for Ready
```

### 3. Run experiments

```bash
# Single experiment
python main.py --config router --n 500

# Automated sweep (preserves running vLLM pods)
python sweep_methods.py --config 1-master_config.yaml --skip-vllm
```

Results are saved to `src/experiments/<N>/` with per-request logs, config snapshots, Prometheus metrics, and Helm manifests.

---

## Architecture

### Serving Stack

LA-Boom deploys as a set of Kubernetes microservices packaged in a single Helm chart (`src/vllm-kv-stack/`):

| Component | Role | Port |
|-----------|------|------|
| **Router Service** | Central queue, KV-aware + length-aware routing, pull/push dispatch | 30080 |
| **Sidecar** | Per-pod worker: local queue, pull from router, forward to vLLM, KV event reporting | (per-pod) |
| **vLLM** | Model inference (OpenAI-compatible API) | 8200 |
| **Redis** | KV block ownership state (`prefix_hash → pod` mapping) | 6379 |
| **Prefix Hash Service** | Computes vLLM-compatible block hashes for KV routing | 8000 |
| **BooM Gateway** | Production API gateway: auth, rate limiting, spend tracking (Rust) | 30401 |
| **LiteLLM Proxy** | Alternative production gateway (Python) | 30400 |
| **Mooncake Master** | Cross-node KV cache transfer coordination (optional, Ascend) | 50088 |

### Routing Modes

**Pull mode** (default) -- sidecars request work when they have capacity. Natural backpressure, stable GPU utilization, and the router has full visibility into queue depth for intelligent dispatch.

**Push modes** -- the router proactively dispatches to sidecars at arrival time using round-robin, random, or least-queue-depth strategies. Lower latency in simple scenarios but no central queue absorption during spikes.

### KV-Aware Routing

When enabled, the router scores queued requests against each sidecar's KV cache state:

1. Sidecars subscribe to vLLM ZMQ events and write block ownership to Redis
2. The router's KV watcher builds an in-memory `block_hash → pod` map
3. At pull time, requests are tiered by contiguous prefix cache hits, then sorted by predicted output length within each tier

### Backend Options

| Backend | Protocol | Use case |
|---------|----------|----------|
| `router` | `/enqueue` or async ZMQ | Benchmarking (clean latency, full observability) |
| `boom` | OpenAI API via BooM | Production auth/spend validation |
| `litellm` | OpenAI API via LiteLLM | Production auth/spend validation (legacy) |
| `aibrix` | OpenAI API via AIBrix | Baseline routing strategy comparison |

---

## Experiment System

### Configuration

Two-layer YAML system:

- **Client configs** (`src/configs/*.yaml`) -- define workload shape (prompts, RPS, request count), backend, generation params, transport mode, and a `helm:` section for deployment knobs
- **Master sweep config** (`src/configs/1-master_config.yaml`) -- maps client configs to lists of routing methods to sweep

### Load Patterns

The scheduler (`src/scheduler.py`) supports: `dump` (all at once), `det` (fixed interval), `poisson`, `bursty`, `steps` (staged RPS ramps), and `rand`.

### Prompt Sources

- `file` -- static JSON prompts
- `hf-lmsys` -- LMSYS Chat 1M from HuggingFace
- `codeflow` -- CodeFlowBench competitive programming dataset

### Sweep Runner

`sweep_methods.py` automates the full deploy-measure cycle:

1. Read master config to build `(client_config, method)` job list
2. For each job: upgrade Helm chart with the appropriate routing settings
3. Wait for all pods Ready
4. Run `main.py` to execute the experiment
5. Save artifacts: `config.json`, `logs.json` (NDJSON), `run_summary.json`, `metrics.jsonl`, `sweep_meta.json`, rendered Helm manifests

Use `--skip-vllm` to preserve running vLLM pods and only redeploy the routing stack between experiments.

### Experiment Artifacts

Each run creates `src/experiments/<N>/` containing:

| File | Contents |
|------|----------|
| `logs.json` | Per-request NDJSON (latency, tokens, endpoint, conversation metadata) |
| `run_summary.json` | Aggregate throughput and latency statistics |
| `config.json` / `config_used.yaml` | Frozen configuration snapshot |
| `metrics.jsonl` | Time-series Prometheus samples during the run |
| `metrics_summary.json` | Metric rollups (queue depth, cache hit rate, ...) |
| `endpoint_tokens.json` | Token distribution per backend pod |
| `sweep_meta.json` | Helm knobs, method, timestamps |

---

## Common Deployment Scenarios

### GLM-5 (MoE, TP8, W4A8 quantization)

```bash
python deploy_vllm.py --config configs/router-tp8-glm.yaml
python sweep_methods.py --config 1-master_config.yaml --skip-vllm
```

### Qwen3-8B (dense, TP1)

```bash
python deploy_vllm.py --config configs/router.yaml
python main.py --config router --n 1000
```

### Data-parallel with LeaderWorkerSet

Configure `dataParallel.enabled: true` and `dataParallel.size: 2` in the `helm.models[]` section. Label node pairs sharing RoCE fabric:

```bash
kubectl label node node5 node6 roce-pair=pair-a
```

### Multi-model serving

Define multiple entries in `helm.models[]`. The router and BooM Gateway automatically discover models from a shared `model-registry` ConfigMap.

### BooM Gateway (production path)

```bash
helm upgrade vllm ./src/vllm-kv-stack \
  --set boom.enabled=true \
  --set boom.masterKey=sk-boom-master
```

---

## Building Custom Images

```bash
# Router (Python)
cd src/services/router_service && bash build.sh

# Sidecar (Python)
cd src/services/sidecar && bash build.sh

# Router + Sidecar (Go)
cd src/services/go && ./build.sh

# BooM Gateway (Rust)
cd BooMGateway-main && cargo build --release -p boom-main
docker build -t reg.local:32000/boom-gateway:latest . && docker push reg.local:32000/boom-gateway:latest
```

---

## Monitoring

| What | How |
|------|-----|
| Pod status | `kubectl get pods -n vllm -w` |
| Router queue depth | `curl http://<node>:30080/metrics \| grep router_central_queue_length` |
| vLLM health | `curl http://<node>:30080/health` |
| Prometheus UI | `http://<node>:31190` |
| Redis KV state | `kubectl exec -it -n vllm <redis-pod> -- redis-cli KEYS '*kvblock*'` |
| Request tracing | Set `TRACE_ENABLED=true` on router/sidecar; traces appear in `result.trace` |

---

## CLI Reference

| Command | Purpose |
|---------|---------|
| `python main.py --config <name> [--n N]` | Run a single load experiment |
| `python sweep_methods.py --config <master> [--skip-vllm]` | Run automated Helm sweep |
| `python deploy_vllm.py --config <cfg> [--reinstall] [--timeout S]` | Deploy vLLM pods only |
| `python router_test.py` | Router smoke test |
| `python watch_redis_kv.py` | Live Redis KV block inspection |
| `python download_codeflowbench.py` | Download CodeFlowBench dataset |

---

## Documentation Index

| Document | Topic |
|----------|-------|
| [docs/quickstart.md](docs/quickstart.md) | Step-by-step deployment and experiment guide |
| [docs/config_knobs.md](docs/config_knobs.md) | Complete configuration reference |
| [docs/router_service.md](docs/router_service.md) | Router architecture and API |
| [docs/sidecar.md](docs/sidecar.md) | Sidecar architecture |
| [docs/kv_cache_flow.md](docs/kv_cache_flow.md) | KV-aware routing deep dive |
| [docs/boom_gateway.md](docs/boom_gateway.md) | BooM Gateway setup and usage |
| [docs/boom_claude.md](docs/boom_claude.md) | Claude Code integration via BooM |
| [docs/multi_model_router.md](docs/multi_model_router.md) | Multi-model serving |
| [docs/slo_aware_routing.md](docs/slo_aware_routing.md) | SLO-aware scheduling |
| [docs/mooncake_integration.md](docs/mooncake_integration.md) | Cross-node KV cache transfer |
| [docs/data_parallel_lws.md](docs/data_parallel_lws.md) | Data-parallel deployment with LWS |
| [docs/multi_turn_conversations.md](docs/multi_turn_conversations.md) | Multi-turn benchmarking |
| [docs/go_services.md](docs/go_services.md) | Go router and sidecar |
| [docs/trace.md](docs/trace.md) | Request tracing |
| [docs/prefix_hash.md](docs/prefix_hash.md) | Prefix hash service |
| [docs/registry.md](docs/registry.md) | Private container registry operations |
| [src/README.md](src/README.md) | Routing algorithms and load client details |
| [src/README_LLMLB.md](src/README_LLMLB.md) | Comprehensive system reference |
| [docs/k8s-dns-troubleshooting.md](docs/k8s-dns-troubleshooting.md) | K8s DNS/networking troubleshooting & post-change verification checklist |
