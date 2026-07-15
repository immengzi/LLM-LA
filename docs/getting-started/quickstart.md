# Quickstart

Deploy the LA-Boom stack on Kubernetes with the Helm chart. This guide assumes the cluster prerequisites are already met; if not, start with [prerequisites.md](prerequisites.md).

> Conventions: replace `<node-ip>` with any cluster node IP and `<repo-root>` with your checkout path. Commands are run from `<repo-root>` unless noted. Cluster-specific values (registry host, NFS server, node labels) are documented under [operations/](../operations/).

## Prerequisites (short)

- A Kubernetes cluster with `kubectl` access and Helm 3.12+
- Model weights reachable from worker nodes (NFS or local path)
- A private image registry holding the LA-Boom images (router, sidecar, prefix-hash, vLLM, gateways)

Full details: [prerequisites.md](prerequisites.md).

## 1. One-time PV/PVC setup

Run once per cluster (not per deploy) to create the model `PersistentVolume`/`PersistentVolumeClaim` over your model storage. After this, leave `modelVolume.create` unset or `false` in later upgrades.

```bash
helm upgrade --install vllm ./src/core/vllm-kv-stack -n vllm --create-namespace \
  --set modelVolume.create=true \
  --set modelVolume.modelSubPath=placeholder
```

## 2. Deploy the stack

Create a values overlay that defines at least one model, then install the full chart (router + Redis + vLLM + sidecar by default):

```yaml
# my-values.yaml
models:
  - name: qwen3-8b
    servedModelName: qwen3-8b
    replicas: 1
    modelSubPath: Qwen3-8B
    tensorParallelSize: 1
    batchSize: 64
    vllm:
      gpuMemoryUtilization: 0.90
```

```bash
helm upgrade --install vllm ./src/core/vllm-kv-stack -n vllm --create-namespace \
  -f my-values.yaml
```

Override cluster-specific knobs (`global.imageRegistry`, NFS, node selectors) in the same file or via `--set`. Full key reference: [Helm values](../configuration/helm-values.md). For multiple models or data-parallel layouts, see [multi-model](../deployment/multi-model.md) and [data parallel with LWS](../deployment/data-parallel-lws.md).

## 3. Verify

```bash
kubectl get pods -n vllm -w
kubectl logs -f <vllm-pod> -n vllm -c vllm   # watch model load progress

# Router health (NodePort 30080)
curl http://<node-ip>:30080/health

# Optional smoke request
curl http://<node-ip>:30080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "qwen3-8b",
    "messages": [{"role": "user", "content": "Hello"}],
    "max_tokens": 32
  }'
```

## 4. Monitor

```bash
kubectl get pods -n vllm -w

# Router queue depth
curl http://<node-ip>:30080/metrics | grep router_central_queue_length

# Prometheus UI (NodePort)
open http://<node-ip>:31190
```

## 5. Production gateways (optional)

For auth, virtual keys, rate limiting, and spend tracking in front of the router, enable a gateway in Helm values (`boom.enabled=true` or `litellm.enabled=true`).

- [BooM Gateway](../gateways/boom/overview.md) (Rust, NodePort 30401) — recommended
- LiteLLM (Python, NodePort 30400) — alternative

## Benchmarking (optional)

To drive open-loop load and collect experiment artifacts against a deployed stack, use the [benchmark harness](../benchmarking/harness.md). Chart configuration from the client side is documented in [client config](../configuration/client-config.md) and [experiment configs](../configuration/experiment-configs.md).

## Common issues

| Symptom | Fix |
|---|---|
| `modelSubPath must be set` | Set `models[].modelSubPath` (or legacy `modelVolume.modelSubPath`) in your values overlay |
| `NPU out of memory` | Model loaded unquantized — set `models[].vllm.quantization: ascend` for W4A8 MoE |
| `KV cache too small for max seq len` | Lower `models[].vllm.maxModelLen` (e.g. `80000`) |
| RBAC ownership conflict | Keep the release name `vllm` for this chart |
| Router image stale | Router uses `imagePullPolicy: Always`; delete the cached image on the node with `crictl rmi` |
| ClusterIP `Connection error` from pods | kube-proxy iptables broken on a node — install `iptables-libs` and restart kube-proxy (see [k8s DNS runbook](../operations/k8s-dns-troubleshooting.md)) |

## See also

- [First deploy walkthrough](first-experiment.md)
- [Architecture overview](../architecture/overview.md)
- [Helm values reference](../configuration/helm-values.md)
