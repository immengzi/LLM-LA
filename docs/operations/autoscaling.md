# Autoscaling (KEDA)

LA-Boom autoscales vLLM **per model** across every deployment topology:

- **Single dense model** (legacy/back-compat) — scales a `Deployment`
- **Multi-model** — one `Deployment` per `models[]` entry, scaled independently
- **Data parallel** — a `LeaderWorkerSet`, scaled via its `/scale` subresource

Autoscaling is driven by [KEDA](https://keda.sh/), which translates a Prometheus
query into a Horizontal Pod Autoscaler. It is **off by default**: with
`autoscaling.enabled: false` the chart renders no autoscaling resources and the
replica counts stay exactly as configured. Nothing about an existing release
changes until you opt in.

## Scaling signals

| Signal | Metric | Use when |
|--------|--------|----------|
| `queue` (default) | `router_central_queue_length_by_model{model="<servedModelName>"}` | A router-style backend is in front (`backend: router\|boom\|litellm`). Scales on central-queue backlog per model. |
| `vllm` | `vllm:gpu_cache_usage_perc{model_name="<servedModelName>"}` | Router-less / direct topologies. Scales on vLLM KV-cache pressure (0..1). |

The `queue` signal relies on the additive `router_central_queue_length_by_model`
gauge emitted by both the Python and Go routers. The legacy global
`router_central_queue_length` gauge is **unchanged** — existing dashboards and
alerts keep working.

## Prerequisites

1. **Prometheus** in the cluster. The `monitoring` Ansible role deploys
   kube-prometheus-stack; the router/vLLM `PodMonitor`s ship with the chart.
2. **KEDA** installed. Use the bundled Ansible role:

   ```bash
   cd infra && make keda
   ```

   This installs KEDA into the `keda` namespace and grants the `keda-operator`
   permission to scale `LeaderWorkerSet` objects (built-in Deployments/
   StatefulSets need no extra RBAC). Verify with:

   ```bash
   cd infra && ./preflight.sh    # check 11 reports the KEDA CRD
   kubectl get crd scaledobjects.keda.sh
   ```

## Enabling autoscaling

Set the values (via `--set`, a values file, or the sweep `HelmConfig`):

```yaml
autoscaling:
  enabled: true
  signal: queue                 # or: vllm
  minReplicaCount: 1            # null -> the model's own replica count
  maxReplicaCount: 8
  threshold: "16"              # queue depth target (queue signal)
  vllmThreshold: "0.8"         # KV-cache target (vllm signal)
  pollingInterval: 10
  cooldownPeriod: 300
  prometheusServerAddress: http://kube-prometheus-stack-prometheus.monitoring.svc:9090
```

When you `helm upgrade` with this enabled, the chart:

- renders one `ScaledObject` named `vllm-<model>-autoscale` per model, and
- **omits** the static `replicas` field from each autoscaled
  `Deployment`/`LeaderWorkerSet`, so KEDA/HPA owns the replica count and helm
  upgrades no longer fight the autoscaler.

### Per-model overrides

```yaml
autoscaling:
  enabled: true
  perModel:
    qwen:     { signal: queue, threshold: "12", maxReplicaCount: 8 }
    minimax:  { enabled: false }          # opt this model out entirely
    glm:      { signal: vllm, vllmThreshold: "0.85" }
```

Models without an entry inherit the global settings. Setting `enabled: false`
for a model leaves its replicas static.

## How scaling behaves

The generated HPA behavior is intentionally aggressive on scale-up and
conservative on scale-down:

- **Scale up:** no stabilization window; up to +100% or +4 pods per minute.
- **Scale down:** 300s stabilization; at most −20% per minute.

Tune via `pollingInterval` / `cooldownPeriod` and, for finer control, edit the
`advanced.horizontalPodAutoscalerConfig.behavior` block in
`templates/60-keda-scaledobject.yaml`.

## Verifying

```bash
kubectl -n vllm get scaledobjects
kubectl -n vllm get hpa                       # KEDA creates one HPA per ScaledObject
kubectl -n vllm describe scaledobject vllm-<model>-autoscale

# Confirm the per-model queue metric is exported by the router:
kubectl -n vllm port-forward svc/router-service 8080:8080 &
curl -s localhost:8080/metrics | grep router_central_queue_length_by_model
```

If an HPA shows `<unknown>` for the metric, check that:

- the `prometheusServerAddress` resolves from the `keda` namespace,
- the query returns data in the Prometheus UI, and
- the `model` label matches the model's `servedModelName`.

## Rollout guidance (production)

1. Build/push the updated router + sidecar images (they carry the new
   `router_central_queue_length_by_model` metric).
2. Install KEDA (`make keda`) — harmless on its own; it scales nothing until a
   `ScaledObject` exists.
3. Enable autoscaling on a **shadow** release first (one dense model and one DP
   model) and watch `kubectl get hpa` react to load.
4. Roll out to production by flipping `autoscaling.enabled: true`. Because the
   `replicas` field is dropped for autoscaled workloads, confirm
   `minReplicaCount` is set to your desired floor before the upgrade.

## Disabling / rollback

Set `autoscaling.enabled: false` and `helm upgrade`. The `ScaledObject`s are
removed, KEDA deletes their HPAs, and the static `replicas` field returns to the
manifests. KEDA itself can stay installed; it is inert without ScaledObjects.
