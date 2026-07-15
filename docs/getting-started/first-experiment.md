# Your first deploy

Anatomy of a Helm install of the `vllm-kv-stack` chart: what the release creates, how values map to resources, and how to upgrade. For the condensed command list, see the [quickstart](quickstart.md).

## Entry point

Deploy with Helm against [`src/core/vllm-kv-stack`](../../src/core/vllm-kv-stack):

```bash
cd <repo-root>
helm upgrade --install vllm ./src/core/vllm-kv-stack -n vllm --create-namespace \
  -f my-values.yaml
```

Use a single release name (`vllm`) for this chart so upgrades stay consistent with RBAC ownership.

## What the release creates

With chart defaults and a populated `models[]` list, a typical install includes:

| Resource | Role |
|----------|------|
| Deployment / LeaderWorkerSet `vllm-{name}` | Per-model vLLM engine + co-located sidecar |
| Service `vllm-{name}` | ClusterIP (and optional NodePort) to the engine |
| Deployment `router-service` | Central queue and routing |
| Deployment Redis | KV-block ownership store |
| ConfigMap `model-registry` | Shared `models.yaml` when `models[]` is set |

Gateways (BooM / LiteLLM) and Mooncake / LMCache pieces render only when enabled in values. Full key map: [Helm values](../configuration/helm-values.md). How the pieces talk: [architecture overview](../architecture/overview.md).

## Values → resources

1. **`models[]`** — one Deployment (or LWS group set) per entry; `modelSubPath` selects the weights folder under the model volume.
2. **`deploy.*`** — toggles components (`vllm`, `router`, `redis`, …) without changing their templates.
3. **`router.*` / `sidecar.*`** — routing mode, KV awareness, batching, and sidecar pull behavior.
4. **`global.imageRegistry`** — rewrites chart images to your private registry when set.

Minimal single-model overlay:

```yaml
models:
  - name: qwen3-8b
    servedModelName: qwen3-8b
    replicas: 1
    modelSubPath: Qwen3-8B
    tensorParallelSize: 1
    batchSize: 64
```

For multi-model or DP/EP layouts, see [multi-model](../deployment/multi-model.md) and [data parallel with LWS](../deployment/data-parallel-lws.md).

## Upgrade and redeploy

Change values and re-run the same `helm upgrade --install` command. Pods roll as their templates change. To force a clean vLLM pod while keeping the release, delete the Deployment/LWS or scale it down and up; the chart ownership stays on release `vllm`.

One-time PV/PVC creation (`modelVolume.create=true`) is covered in the [quickstart](quickstart.md#1-one-time-pvpvc-setup); leave create off afterward.

## Verify

```bash
kubectl get pods -n vllm -w
curl http://<node-ip>:30080/health
```

## Next steps

- Tune chart values: [Helm values reference](../configuration/helm-values.md)
- Understand the routing: [architecture overview](../architecture/overview.md)
- Generate load and collect artifacts: [benchmark harness](../benchmarking/harness.md)
