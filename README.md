<div align="center">

# 💥 LLM-LA

**KV-aware load balancing for LLMs — at cluster scale.**

*Balance the load based on the current load, not historical load.*

![License: TBD](https://img.shields.io/badge/license-TBD-lightgrey)
![Kubernetes](https://img.shields.io/badge/Kubernetes-1.28%2B-326ce5)
![vLLM](https://img.shields.io/badge/vLLM-OpenAI--compatible-4b8bbe)
![Python](https://img.shields.io/badge/Python-3.10%2B-3776ab)
![Go](https://img.shields.io/badge/Go-1.26-00add8)
![Helm](https://img.shields.io/badge/Helm-3.12%2B-0f1689)

[Quickstart](#quickstart-60-seconds) · [Architecture](#architecture) · [Capabilities](#key-capabilities) · [Compatibility](#compatibility) · [Docs](docs/README.md)

</div>

---

**LLM-LA is a distributed serving platform for LLMs on Kubernetes** — a KV-, length-, and SLO-aware router with per-pod sidecars wrapped around unmodified serving engines (**vLLM** by default; **SGLang** opt-in on a pinned profile). Route based on real-time current load for cache reuse; reduce serving bubbles; support per model autoscaling; and reduce the overall E2E latency of your model serving.

## 🤔 Why LLM-LA

> **The one-liner:** Traditional load-balancing results in sub-optimal results. Join-Idle-Queue, the technique underlying LLM-LA, can be proven better!

- **Most routers are cache-blind.** LLM-LA scores every queued request against each replica's *live* KV-block ownership and places it for maximum prefix reuse — higher hit rates, lower TTFT.
- **Accelerator sharing for LLMs.** Many serving deployments are severely underutilized due to the low request rate. Traditional model-switching adds large overheads. LLM-LA uses pipelining to enable faster model-switching resulting in high-efficiency accelerator sharing.
- **Length and deadlines are first-class.** Routing is length-aware (short-first / long-first batching) and SLO-aware (slack-based deadline scheduling), not an afterthought.
- **Kubernetes-native, one chart.** Router, sidecars, Redis, serving engine, gateways, and per-model KEDA autoscaling all deploy from the [`vllm-kv-stack`](src/core/vllm-kv-stack) Helm chart — no engine fork.


## 🏗️ Architecture

```mermaid
flowchart TB
  subgraph clients [Clients]
    Bench["Benchmark harness<br/>src/client (main.py / sweep_methods.py)"]
    App["Apps / Claude Code"]
  end

  subgraph gw [Gateways - optional]
    BooM["BooM :30401 (Rust)"]
    LiteLLM["LiteLLM :30400 (Python)"]
  end

  Router["Router :8080 / :30080<br/>KV-aware + length-aware + SLO scheduling<br/>inline KV-block hashing<br/>(Python or Go)"]
  Redis[("Redis :6379<br/>KV block ownership")]
  Prom["Prometheus"]

  subgraph serving [Engine fleet - multi-model]
    direction LR
    subgraph mA [model A - e.g. GLM-5 TP8 / DP via LWS]
      SA["Sidecar :9000"] --> VA["Engine :8200"]
    end
    subgraph mB [model B - e.g. Qwen3-8B TP1]
      SB["Sidecar :9000"] --> VB["Engine :8200"]
    end
  end

  Mooncake["Mooncake master :50088<br/>cross-node KV transfer"]

  App --> BooM --> Router
  App --> LiteLLM --> Router
  Bench --> Router
  Router -->|"pull / push"| SA
  Router -->|"pull / push"| SB
  Router -->|"SCAN {model}:kvblock:*"| Redis
  SA -->|"ZMQ kv@ events"| Redis
  SB -->|"ZMQ kv@ events"| Redis
  VA -. KV blocks .- Mooncake
  VB -. KV blocks .- Mooncake
  Router -.->|metrics| Prom
  SA -.->|metrics| Prom
```

**How the loop closes:** the router holds a central queue and either **pulls** work to sidecars on demand (capacity-gated, the default) or **pushes** it proactively. Sidecars subscribe to the engine's ZMQ KV events and record block ownership in Redis; the router watches Redis to build a live `block hash → replica` map for prefix-aware placement. KV-block hashing runs **inside the router by default** (in-process for Python, a tiny in-container hasher for Go); the standalone prefix-hash service is an optional legacy mode (`KV_HASH_SOURCE=external`, vLLM path). Full tour → [docs/architecture/overview.md](docs/architecture/overview.md).

## ⚡ Key capabilities

- **Routing** — pull (capacity-gated, default) and push (`push-rr`, `push-random`, `push-leastq`, `push-throughput`, `push-p2c`, `push-kv-cost`, `push-least-kv`, `push-least-latency`, `push-least-busy`, plus `central-push` / `external-push`); KV-aware prefix-tier ordering; length-aware batching (`short_first`, `long_first`); SLO-aware slack scheduling. See [router.md](docs/architecture/router.md).
- **Serving** — many models from one cluster; multi-node data parallel via [LeaderWorkerSet](docs/deployment/data-parallel-lws.md) with expert parallel for MoE models (e.g. GLM-5) on **vLLM**; cross-node KV transfer via [Mooncake / LMCache](docs/deployment/mooncake/helm-integration.md) (**vLLM**); opt-in [SGLang](docs/deployment/sglang.md) for the pinned core stack; NVIDIA [GPU](docs/deployment/gpu.md) or Ascend NPU via `hardware`.
- **Autoscaling** — per-model [KEDA autoscaling](docs/operations/autoscaling.md) across every topology (dense, multi-model, data-parallel LWS) on router-queue or engine KV-/token-usage signals; off by default, fully backward compatible.
- **Benchmarking** — a bundled client harness with an open-loop load generator and automated Helm sweeps that capture full artifacts; see [docs/benchmarking/harness.md](docs/benchmarking/harness.md).
- **Gateways** — [BooM Gateway](docs/gateways/boom/overview.md) (Rust) and LiteLLM (Python) for auth, virtual keys, rate limiting, and spend tracking; Claude Code support.
- **Observability** — Prometheus metrics, per-request [tracing](docs/architecture/trace.md), and live Redis KV-state inspection.

## 🧩 Compatibility

LLM-LA is a routing/benchmark layer around **unmodified inference engines**, so it inherits the engine's model support and adds Kubernetes-native wiring on top.

| Area | Works with | Notes |
|------|-----------|-------|
| Inference engine | **vLLM** (default) · **SGLang** v0.5.15 (opt-in) | Router + sidecar wrap stock engines; no engine fork. SGLang: [sglang.md](docs/deployment/sglang.md). DP/Mooncake/LMCache remain **vLLM-only** today ([#67](https://github.com/LA-Boom/llm-la/issues/67)) |
| Accelerators | **Ascend NPU** (default) · **NVIDIA GPU** | Single `hardware` switch — [gpu.md](docs/deployment/gpu.md) |
| KV transfer | **Mooncake** (default) and **LMCache** P2P + host-staging | **vLLM** topologies; switch via `lmcache.mode: mooncake \| p2p` — see [lmcache-p2p-host-staging.md](docs/internal/lmcache-p2p-host-staging.md) and [Mooncake integration](docs/deployment/mooncake/helm-integration.md) |
| Parallelism | **Tensor parallel** (TP), **data parallel** (DP) + **expert parallel** (EP) for MoE | DP/EP via [LeaderWorkerSet](docs/deployment/data-parallel-lws.md) (**vLLM**; e.g. GLM-5 TP8+DP) |
| Orchestration | **Kubernetes 1.28+**, **Helm 3.12+**, **LeaderWorkerSet** | Deploy/sweep via the `vllm-kv-stack` chart |
| Autoscaling | **KEDA** | Per-model, opt-in, on router-queue or engine usage signals ([autoscaling.md](docs/operations/autoscaling.md)) |
| Gateways / auth | **BooM Gateway** (Rust), **LiteLLM** (Python) | Virtual keys, rate limiting, spend tracking |
| Clients | **OpenAI API**, **Claude Code** | Anthropic-style access via BooM/LiteLLM |
| State / storage | **Redis** (KV-block ownership), **NFS** (model weights), private registry | |
| Observability | **Prometheus** + **Grafana** (kube-prometheus-stack) | Metrics, dashboards, per-request tracing |
| Models (validated) | GLM-5, MiniMax-M2, Qwen3 | Any engine-supported model works on the chosen backend |

## 🚀 Quickstart (60 seconds)

> **Prerequisites:** a Kubernetes cluster with `kubectl` and Helm 3.12+, model weights reachable from worker nodes, and a private image registry. Full details in [docs/getting-started/prerequisites.md](docs/getting-started/prerequisites.md).

```bash
# 1. One-time: create the model PV/PVC
helm upgrade --install vllm ./src/core/vllm-kv-stack -n vllm --create-namespace \
  --set modelVolume.create=true --set modelVolume.modelSubPath=placeholder

# 2. Deploy the platform (vLLM + sidecar) for a model
python src/client/deploy_vllm.py --config configs/router-tp8-glm.yaml

# 3. Send some load (via the benchmark harness)
python src/client/main.py --config router --n 500
```

The load test uses the bundled harness — see [docs/benchmarking/harness.md](docs/benchmarking/harness.md) for sweeps and analysis. Then read the results like a pro → [full walkthrough](docs/getting-started/quickstart.md).

## Documentation

Everything lives in the **[documentation index](docs/README.md)**. Jump by role:

| I want to… | Start here |
|------------|-----------|
| Get running fast | [Getting Started](docs/getting-started/quickstart.md) |
| Understand the design | [Architecture overview](docs/architecture/overview.md) · [Router strategies](docs/architecture/router-strategies.md) · [KV cache flow](docs/architecture/kv-cache-flow.md) |
| Configure a deploy | [Client config](docs/configuration/client-config.md) · [Helm values](docs/configuration/helm-values.md) |
| Ship to production | [BooM Gateway](docs/gateways/boom/overview.md) |
| Deploy at scale | [Multi-model serving](docs/deployment/multi-model.md) · [GPU deployment](docs/deployment/gpu.md) · [SGLang](docs/deployment/sglang.md) · [GLM-5 reference ("HQ") deployment](docs/deployment/docker-reference/glm5-dp-docker.md) |
| Operate the cluster | [Cluster setup](docs/operations/cluster-setup.md) · [Registry & image builds](docs/operations/registry.md) · [Image patches](docs/operations/image-patches.md) |
| Benchmark & analyze | [Benchmark harness](docs/benchmarking/harness.md) · [Load patterns](docs/benchmarking/load-patterns.md) · [Artifacts & analysis](docs/benchmarking/artifacts-and-analysis.md) |
| See the roadmap | [docs/internal/roadmap.md](docs/internal/roadmap.md) |

## Repository layout

```
.
├── README.md                 # This landing page
├── docs/                     # Documentation (see docs/README.md)
├── analysis-notebooks/       # Post-experiment analysis notebooks
├── infra/                    # Cluster-prep automation (docs: docs/operations/cluster-prep-automation.md)
└── src/
    ├── core/                 # The deployed serving platform
    │   ├── services/         # Router, sidecar (Python + Go), prefix-hash
    │   ├── vllm-kv-stack/    # Helm chart (Redis, router, engines, gateways, ...)
    │   ├── boom-integration/ # BooM Gateway image build
    │   └── mutil-node-operations/  # Registry / multi-node setup scripts
    └── client/               # The benchmark harness (see docs/benchmarking/harness.md)
        ├── main.py           # Load experiment entry point
        ├── sweep_methods.py  # Automated Helm sweep runner
        ├── deploy_vllm.py    # Engine deployment helper (vLLM default; SGLang via engine_type)
        ├── config.py         # Client + Helm configuration schema
        ├── configs/          # Client and sweep YAML configs
        └── multiturn-generation/  # Claude Code injection templates
```

---

<div align="center">

Built for real clusters. Router, sidecar, prefix-hash, and gateway images ship to a private registry; internal specifics (registry hostnames, NFS servers, node labels) live under [docs/operations/](docs/operations/), and examples elsewhere use placeholders like `<node-ip>` and `<repo-root>`.

</div>
