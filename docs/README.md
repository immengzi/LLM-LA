# LA-Boom Documentation

The serving platform and benchmark harness for KV-aware vLLM on Kubernetes. New here? Start with [Getting Started → Quickstart](getting-started/quickstart.md).

## Quick paths by role

- **New user** → [Getting Started](#getting-started) then [Architecture overview](architecture/overview.md)
- **Operator** → [Operations](#operations) and [Helm values reference](configuration/helm-values.md)
- **Benchmarker** → [Benchmarking](#benchmarking) and [Client config reference](configuration/client-config.md)
- **Researcher** → [Architecture](#architecture) and [Comparisons](#comparisons-research)

## Getting started

| Doc | Description |
|-----|-------------|
| [quickstart.md](getting-started/quickstart.md) | Deploy the stack and run your first experiment end-to-end |
| [prerequisites.md](getting-started/prerequisites.md) | Cluster, NPU, NFS, registry, and tooling requirements |
| [first-experiment.md](getting-started/first-experiment.md) | Anatomy of a run: `deploy_vllm.py` + `main.py` |

## Architecture

| Doc | Description |
|-----|-------------|
| [overview.md](architecture/overview.md) | How the serving platform fits together (start here) |
| [router.md](architecture/router.md) | Central queue, pull/push dispatch, HTTP/ZMQ APIs |
| [sidecar.md](architecture/sidecar.md) | Local queue, vLLM forwarding, KV event reporting |
| [kv-cache-flow.md](architecture/kv-cache-flow.md) | KV-aware routing deep dive (Redis schema, scoring) |
| [prefix-hash.md](architecture/prefix-hash.md) | vLLM-compatible block hashing service |
| [slo-aware-routing.md](architecture/slo-aware-routing.md) | Slack-based deadline scheduling |
| [trace.md](architecture/trace.md) | Per-request distributed tracing |
| [go-services.md](architecture/go-services.md) | Go router/sidecar port and parity with Python |

## Configuration

| Doc | Description |
|-----|-------------|
| [client-config.md](configuration/client-config.md) | Load-client YAML reference (`config.py`) |
| [helm-values.md](configuration/helm-values.md) | `vllm-kv-stack` Helm values reference |
| [experiment-configs.md](configuration/experiment-configs.md) | Config naming, `1-master_config.yaml`, sweep matrix |

## Deployment

| Doc | Description |
|-----|-------------|
| [multi-model.md](deployment/multi-model.md) | Serving multiple models from one cluster |
| [data-parallel-lws.md](deployment/data-parallel-lws.md) | Data parallel + EP via LeaderWorkerSet |
| [docker-reference/glm5-dp-docker.md](deployment/docker-reference/glm5-dp-docker.md) | Bare-Docker GLM-5 DP+EP — the reference ("HQ") deployment |
| [mooncake/helm-integration.md](deployment/mooncake/helm-integration.md) | Mooncake KV transfer Helm wiring |
| [mooncake/glm5-production.md](deployment/mooncake/glm5-production.md) | Production GLM-5 + Mooncake topology |
| [docker-reference/mooncake-pd-test.md](deployment/docker-reference/mooncake-pd-test.md) | Bare-Docker Mooncake prefiller/decoder (P/D) lab |

## Operations

| Doc | Description |
|-----|-------------|
| [cluster-setup.md](operations/cluster-setup.md) | Post-Kubernetes prep checklist (NFS, registry, RoCE, LWS) |
| [infra/](../infra/README.md) | Ansible automation of the cluster-setup checklist (`make prep` / `make verify`) |
| [multi-node-setup-guide.md](operations/multi-node-setup-guide.md) | Deep multi-node infra reference (NFS, PV, registry, RoCE) |
| [registry.md](operations/registry.md) | Private registry (`reg.local:32000`) operations |
| [k8s-dns-troubleshooting.md](operations/k8s-dns-troubleshooting.md) | DNS/networking runbook |
| [disaster-recovery.md](operations/disaster-recovery.md) | Incident runbook: disk pressure, NPU/scheduling, node loss, OOM, stuck routing |
| [docker-proxy-fix.md](operations/docker-proxy-fix.md) | Docker pulls behind an SSL-inspecting proxy |
| [aibrix-long-running-requests.md](operations/aibrix-long-running-requests.md) | AIBrix Envoy stream timeout fix |

## Gateways

| Doc | Description |
|-----|-------------|
| [boom/overview.md](gateways/boom/overview.md) | BooM Gateway (Rust): auth, virtual keys, spend |
| [boom/models.md](gateways/boom/models.md) | Model-name routing chain and multi-model |
| [boom/build.md](gateways/boom/build.md) | Building and pushing the BooM image |
| [boom/claude-code.md](gateways/boom/claude-code.md) | Claude Code via BooM |
| [litellm/claude-code.md](gateways/litellm/claude-code.md) | Claude Code via LiteLLM (legacy path) |

## Benchmarking

| Doc | Description |
|-----|-------------|
| [load-patterns.md](benchmarking/load-patterns.md) | RPS schedules and the open-loop load model |
| [multi-turn.md](benchmarking/multi-turn.md) | Multi-turn conversation benchmarking |
| [codeflowbench.md](benchmarking/codeflowbench.md) | CodeFlowBench dataset experiments |
| [artifacts-and-analysis.md](benchmarking/artifacts-and-analysis.md) | `experiments/<N>/` layout and analysis notebooks |

## Comparisons (research)

| Doc | Description |
|-----|-------------|
| [pull-vs-aibrix-lr.md](comparisons/pull-vs-aibrix-lr.md) | Pull vs AIBrix least-request scheduling |
| [pull-vs-aibrix-kv.md](comparisons/pull-vs-aibrix-kv.md) | Pull KV vs AIBrix prefix-cache routing |

## Internal (historical / planning)

> These are design and planning records, not user guides.

| Doc | Description |
|-----|-------------|
| [internal/vision.md](internal/vision.md) | Project vision and design goals |
| [internal/open-sourcing-plan.md](internal/open-sourcing-plan.md) | Open-source feature plan and roadmap |
| [internal/open-sourcing-v01.md](internal/open-sourcing-v01.md) | Go v0.1 runtime/migration design |
| [internal/boom-integration-notes.md](internal/boom-integration-notes.md) | Dev record: adding `backend: boom` to the framework |
| [internal/stability-test-findings.md](internal/stability-test-findings.md) | Incident report: 24h stability test |
