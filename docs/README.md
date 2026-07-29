# LA-Boom Documentation

The serving platform and benchmark harness for KV-aware vLLM on Kubernetes. New here? Start with [Getting Started → Quickstart](getting-started/quickstart.md).

## Quick paths by role

- **New user** → [Getting Started](#getting-started) then [Architecture overview](architecture/overview.md)
- **Operator** → [Operations](#operations) and [Helm values reference](configuration/helm-values.md)
- **Benchmarker** → [Benchmark harness](benchmarking/harness.md), [Benchmarking](#benchmarking), and [Client config reference](configuration/client-config.md)
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
| [router-strategies.md](architecture/router-strategies.md) | Routing strategies overview with figures (`none/prefix/affinity/both`) |
| [router.md](architecture/router.md) | Central queue, pull/push dispatch (`push-rr` … `push-least-busy`, `central-push`, `external-push`), [compatibility matrix](architecture/router.md#routing-compatibility-matrix), HTTP/ZMQ APIs |
| [sidecar.md](architecture/sidecar.md) | Local queue, vLLM forwarding, KV event reporting |
| [kv-cache-flow.md](architecture/kv-cache-flow.md) | KV-aware routing deep dive (Redis schema, scoring) |
| [key-affinity.md](architecture/key-affinity.md) | Conversation stickiness (same chat → same pod) |
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
| [switch_cluster.md](operations/switch_cluster.md) | `switch_cluster` knob: per-cluster profile defaults (`yz`/`bz`) |

## Deployment

| Doc | Description |
|-----|-------------|
| [multi-model.md](deployment/multi-model.md) | Serving multiple models from one cluster |
| [gpu.md](deployment/gpu.md) | Deploy on NVIDIA GPUs: device plugin, RuntimeClass, and the `hardware` switch |
| [data-parallel-lws.md](deployment/data-parallel-lws.md) | Data parallel + EP via LeaderWorkerSet |
| [prefill-decode-disaggregation.md](deployment/prefill-decode-disaggregation.md) | Vanilla prefill/decode (P/D) disaggregation: prefill pool + decode pool + proxy |
| [docker-reference/glm5-dp-docker.md](deployment/docker-reference/glm5-dp-docker.md) | Bare-Docker GLM-5 DP+EP — the reference ("HQ") deployment |
| [mooncake/helm-integration.md](deployment/mooncake/helm-integration.md) | Mooncake KV transfer Helm wiring |
| [mooncake/glm5-production.md](deployment/mooncake/glm5-production.md) | Production GLM-5 + Mooncake topology |
| [docker-reference/mooncake-pd-test.md](deployment/docker-reference/mooncake-pd-test.md) | Bare-Docker Mooncake prefiller/decoder (P/D) lab |
| [LMCache-p2p-build.md](deployment/LMCache-p2p-build.md) | Build LMCache-Ascend with the P2P (HCCL host-staging) backend from the pinned fork into the `lmcache-ascend:hccl-p2p` image.

## Operations

| Doc | Description |
|-----|-------------|
| [cluster-setup.md](operations/cluster-setup.md) | Post-Kubernetes prep checklist (NFS, registry, RoCE, LWS) |
| [bz-cluster-nodes.md](operations/bz-cluster-nodes.md) | BZ cluster node inventory: public/private IPs, SSH aliases, NPU, schematic |
| [bz-dashboard-access.md](operations/bz-dashboard-access.md) | Reach BZ dashboards/notebooks (Prometheus, Grafana, Jupyter) over SSH tunnels |
| [autoscaling.md](operations/autoscaling.md) | Per-model KEDA autoscaling (dense, multi-model, data-parallel) |
| [cluster-prep-automation.md](operations/cluster-prep-automation.md) | Ansible automation of the cluster-setup checklist (`make prep` / `make verify`) |
| [multi-node-setup-guide.md](operations/multi-node-setup-guide.md) | Deep multi-node infra reference (NFS, PV, registry, RoCE) |
| [registry.md](operations/registry.md) | Private registry (`reg.local:32000`) operations |
| [image-patches.md](operations/image-patches.md) | vLLM/LMCache-Ascend source patches baked into the image (negative-counter crash fix) |
| [k8s-dns-troubleshooting.md](operations/k8s-dns-troubleshooting.md) | DNS/networking runbook |
| [disaster-recovery.md](operations/disaster-recovery.md) | Incident runbook: disk pressure, NPU/scheduling, node loss, OOM, stuck routing |
| [docker-proxy-fix.md](operations/docker-proxy-fix.md) | Docker pulls behind an SSL-inspecting proxy |
| [aibrix-long-running-requests.md](operations/aibrix-long-running-requests.md) | AIBrix Envoy stream timeout fix |
| [claude-code-setup.md](operations/claude-code-setup.md) | Claude Code install/setup record (Node, npm, pinned version) |

## Gateways

| Doc | Description |
|-----|-------------|
| [boom/overview.md](gateways/boom/overview.md) | BooM Gateway (Rust): auth, virtual keys, spend |
| [boom/models.md](gateways/boom/models.md) | Model-name routing chain and multi-model |
| [boom/build.md](gateways/boom/build.md) | Building and pushing the BooM image |
| [boom/boom-gateway-openeuler-walkthrough.zh.md](gateways/boom/boom-gateway-openeuler-walkthrough.zh.md) | openEuler BooM Gateway source walkthrough (Chinese) |
| [boom/claude-code.md](gateways/boom/claude-code.md) | Claude Code via BooM |
| [litellm/claude-code.md](gateways/litellm/claude-code.md) | Claude Code via LiteLLM (legacy path) |

## Benchmarking

| Doc | Description |
|-----|-------------|
| [harness.md](benchmarking/harness.md) | The client: load generator, sweep runner, deploy helper, external observer |
| [load-patterns.md](benchmarking/load-patterns.md) | RPS schedules and the open-loop load model |
| [multi-turn.md](benchmarking/multi-turn.md) | Multi-turn conversation benchmarking |
| [codeflowbench.md](benchmarking/codeflowbench.md) | CodeFlowBench dataset experiments |
| [artifacts-and-analysis.md](benchmarking/artifacts-and-analysis.md) | `experiments/<N>/` layout and analysis notebooks |
| [gateway-overhead-benchmark.md](benchmarking/gateway-overhead-benchmark.md) | LiteLLM-style mock/overhead: LLM-LA vs BooM vs LiteLLM (`src/client/bench_mock/`) |

## Comparisons (research)

| Doc | Description |
|-----|-------------|
| [pull-vs-aibrix-lr.md](comparisons/pull-vs-aibrix-lr.md) | Pull vs AIBrix least-request scheduling |
| [pull-vs-aibrix-kv.md](comparisons/pull-vs-aibrix-kv.md) | Pull KV vs AIBrix prefix-cache routing |

## Internal (historical / planning)

> These are design and planning records, not user guides.

| Doc | Description |
|-----|-------------|
| [internal/roadmap.md](internal/roadmap.md) | **Consolidated roadmap**: vision, what ships today, competitive landscape, the complete feature-request mapping, and the condensed Go v0.1 implementation spec |
| [internal/boom-integration-notes.md](internal/boom-integration-notes.md) | Dev record: adding `backend: boom` to the framework |
| [internal/stability-test-findings.md](internal/stability-test-findings.md) | Incident report: 24h stability test |
| [internal/kv-cache-hit-rate-collapse.md](internal/kv-cache-hit-rate-collapse.md) | Investigation: prefix-cache hit-rate collapse under peak load |
| [internal/lmcache-p2p-host-staging.md](internal/lmcache-p2p-host-staging.md) | As-built reference: LMCache P2P + host-staging mode (the "direct142" replica) |
| [internal/persistent-affinity-map.md](internal/persistent-affinity-map.md) | Redis-backed affinity map: durable conversation→pod pins across router restarts, single-signal readiness design, with live BZ validation |
