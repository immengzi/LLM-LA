# Multi-Model Router

Serve 10+ models through a single router instance, each with its own
request queue and vLLM pod pool, while keeping BooM as the unified
external gateway.

---

## Architecture

```
                           ┌──────────────────────────────────┐
                           │  ConfigMap: model-registry       │
                           │  (models.yaml — router registry)  │
                           └──────────────┬───────────────────┘
                                          │ vol mount
                                          ▼
  Client ──► BooM Gateway ──► Router Service ──► Sidecar ──► vLLM
              (model_list             (reads
               baked into             router_params)
               boom-config)
```

### Flow

1. Client sends a request to **BooM Gateway** with `model: glm5-chat`.
2. BooM looks up `glm5-chat` in its `model_list` (from the shared
   `models.yaml`) and forwards to `router-service:8080/v1/chat/completions`.
3. **Router** reads `req.model`, resolves it against the model registry,
   and enqueues into the `glm5-chat` queue.
4. The **sidecar** on a `vllm-glm5-chat` pod calls `/pull` with
   `model: glm5-chat`, pulling work from that specific queue.
5. The sidecar forwards to its local **vLLM** instance.

### Single Source of Truth

Both definitions derive from the same `models[]` list in `values.yaml`, but the
wiring differs:

- The **router** mounts the `model-registry` ConfigMap (rendered from `models[]`)
  and reads `model_name` + `router_params`.
- **BooM** does not mount that ConfigMap — its `model_list` is generated from
  `models[]` and baked into the `boom-config` ConfigMap at Helm render time.

Either way, `models[]` is the single place to add/remove/rename models.

---

## Backward Compatibility

Legacy configs with flat `vllm_*` and `data_parallel_*` fields are
auto-migrated to a `models[]` entry at runtime by
`migrate_legacy_helm_to_models()` in `config.py`. A deprecation
warning is printed but the deployment works identically.

When `models: []` (the default in `values.yaml`), the unified Helm
template synthesizes a single model from top-level values, so
everything works as before.

| Aspect | Single-model | Multi-model | Data-parallel |
|--------|-------------|-------------|---------------|
| `model-registry` ConfigMap | Created when models[] populated | Created | Created |
| vLLM resource | Deployment `vllm-{name}` | Per-model Deployments | LeaderWorkerSet `vllm-{name}` |
| Template | `40-vllm-unified.yaml` | `40-vllm-unified.yaml` | `40-vllm-unified.yaml` |
| Config surface | `helm.models[]` | `helm.models[]` | `helm.models[].dataParallel` |

---

## Configuration

### values.yaml

Add a `models` list:

```yaml
models:
  - name: glm5-chat
    servedModelName: glm5-chat
    replicas: 2
    modelSubPath: GLM-5-w4a8-mtp-QuaRot
    tensorParallelSize: 8
    batchSize: 32
    vllm:
      gpuMemoryUtilization: 0.95
      quantization: ascend
      enableExpertParallel: true

  - name: qwen3-8b
    servedModelName: qwen3-8b
    replicas: 4
    modelSubPath: Qwen3-8B
    tensorParallelSize: 1
    batchSize: 64
    vllm:
      gpuMemoryUtilization: 0.90
```

### Model entry fields

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `name` | Yes | — | Model identifier. Used in Deployment names, labels, queue names |
| `servedModelName` | No | `name` | vLLM `--served-model-name`. Clients use this in `model:` field |
| `replicas` | No | 1 | Number of instances (Deployment pods, or LWS groups in DP mode) |
| `modelSubPath` | No | `modelVolume.modelSubPath` | NFS subpath to model weights |
| `tensorParallelSize` | No | `tensorParallelSize` (global) | TP size for this model |
| `batchSize` | No | `batchSize` (global) | Sidecar batch size for this model |
| `image` | No | `images.vllm` | vLLM container image (override per model) |
| `vllm` | No | `{}` | Per-model vLLM flags (see below) |
| `dataParallel` | No | `{}` | Per-model LWS config (see below) |

### Per-model data parallel config (inside `dataParallel:`)

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `enabled` | Yes | `false` | Enables LeaderWorkerSet instead of Deployment |
| `size` | No | 2 | Pods per DP group (1 leader + N-1 workers) |
| `groups` | No | (uses model `replicas`) | Deprecated — use model-level `replicas` instead |
| `sizeLocal` | No | 1 | `--data-parallel-size-local` per pod |
| `rpcPort` | No | 13389 | `--data-parallel-rpc-port` |
| `nicName` | No | `""` | NIC name override (empty = auto-detect from NODE_IP) |
| `hcclBuffSize` | No | 200 | `HCCL_BUFFSIZE` for RoCE transfers |
| `ompNumThreads` | No | 16 | `OMP_NUM_THREADS` |
| `pairTopologyKey` | No | `""` | Node label key for RoCE-pair pinning (K8s 1.29+). When set, pods in the same LWS group are forced onto nodes sharing the same label value, preventing HCCL cross-pair interference. See [Node-pair pinning](#node-pair-pinning). |

### Per-model vLLM flags (inside `vllm:`)

| Field | Description |
|-------|-------------|
| `gpuMemoryUtilization` | `--gpu-memory-utilization` |
| `quantization` | `--quantization` |
| `enableExpertParallel` | `--enable-expert-parallel` |
| `maxModelLen` | `--max-model-len` |
| `kvCacheDtype` | `--kv-cache-dtype` |
| `cpuOffloadGb` | `--cpu-offload-gb` |
| `enablePrefixCaching` | Prefix caching toggle |
| `maxNumBatchedTokens` | `--max-num-batched-tokens` |
| `trustRemoteCode` | `--trust-remote-code` |
| `compilationConfig` | `{cudagraphMode: "..."}` |
| `seed` | `--seed` |
| `additionalConfig` | `{multistream_overlap_shared_expert: true}` |
| `speculativeConfig` | `{numSpeculativeTokens: 3, method: "deepseek_mtp"}` |
| `toolCallParser` | `--tool-call-parser` |
| `reasoningParser` | `--reasoning-parser` |

Unset fields fall back to the global `vllm.*` values in `values.yaml`.

### Client config (sweep runner)

In your client config YAML (e.g. `configs/multi-model-example.yaml`):

```yaml
helm:
  models:
    - name: glm5-chat
      servedModelName: glm5-chat
      replicas: 2
      modelSubPath: GLM-5-w4a8-mtp-QuaRot
      tensorParallelSize: 8
      batchSize: 32
      vllm:
        gpuMemoryUtilization: 0.95
        quantization: ascend
    - name: qwen3-8b
      servedModelName: qwen3-8b
      replicas: 4
      modelSubPath: Qwen3-8B
      tensorParallelSize: 1
      batchSize: 64
```

The sweep runner writes the models list to a temporary values file and
passes it to Helm via `-f`, keeping all existing `--set` knobs intact.

### Node-pair pinning

When Ascend nodes are RoCE-cabled in fixed pairs (e.g. node1↔node2,
node3↔node4), each LWS group **must** land on a physically connected
pair. Without pinning, the Kubernetes scheduler may mix nodes from
different pairs, causing HCCL failures (`aclnnMoeDistributeDispatchV4`).

**Step 1 — Label the nodes:**

```bash
kubectl label node node1 node2 roce-pair=pair-a
kubectl label node node3 node4 roce-pair=pair-b
```

**Verify current pairs:**

```bash
kubectl get nodes -L roce-pair
```

**Step 2 — Set `pairTopologyKey` in the model config:**

```yaml
models:
  - name: glm5-chat
    replicas: 2          # 2 groups → pair-a and pair-b
    dataParallel:
      enabled: true
      size: 2
      pairTopologyKey: roce-pair
```

This adds two scheduling rules to every pod in the LWS:

| Rule | Effect |
|------|--------|
| `podAffinity` with `matchLabelKeys: [group-index]` on `roce-pair` | Pods in the **same** group land on nodes with the **same** `roce-pair` value |
| `podAntiAffinity` on `kubernetes.io/hostname` (always present) | Leader and worker in the same group go to **different** nodes within the pair |

Together, these guarantee group 0 stays on pair-a and group 1 on pair-b
(assuming each pair has exactly `size` nodes).

> **Requires Kubernetes 1.29+** for the `matchLabelKeys` field in
> `podAffinityTerm`. On older clusters, use `nodeSelector` with manual
> per-deployment label overrides.

---

## Deployment

### Prerequisites

- NFS PV/PVC with all model weight directories accessible
- All model images available in the local registry

### Deploy

```bash
helm upgrade --install vllm ./vllm-kv-stack \
  -n vllm --create-namespace \
  -f values.yaml \
  -f models-override.yaml   # or use --set-json for simple cases
```

Or via the sweep runner:

```bash
python sweep_methods.py --master configs/multi-model-master.yaml
```

### Verify

```bash
# Check all model Deployments
kubectl get deploy -n vllm | grep vllm-

# Check shared ConfigMap
kubectl get cm model-registry -n vllm -o yaml

# Check router health (should list all models)
curl http://<node-ip>:30080/health
# {"status":"ok","queue_len":0,"models":["glm5-chat","qwen3-8b"]}

# Test specific model via BooM
curl http://<node-ip>:30401/v1/chat/completions \
  -H "Authorization: Bearer sk-boom-master" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "glm5-chat",
    "messages": [{"role": "user", "content": "Hello"}],
    "max_tokens": 50
  }'
```

---

## What Gets Created (multi-model mode)

For a `models` list with 2 entries (`glm5-chat`, `qwen3-8b`):

| Resource | Name | Purpose |
|----------|------|---------|
| ConfigMap | `model-registry` | Shared `models.yaml` (single source of truth) |
| Deployment | `vllm-glm5-chat` | 2 replicas of GLM-5 (TP=8) + sidecar |
| Deployment | `vllm-qwen3-8b` | 4 replicas of Qwen3-8B (TP=1) + sidecar |
| Service | `vllm-glm5-chat` | ClusterIP for direct access |
| Service | `vllm-qwen3-8b` | ClusterIP for direct access |
| Deployment | `router-service` | Mounts `model-registry`, runs model-aware routing |
| Deployment | `boom-proxy` | Merges `models.yaml` into its config via init container |

The existing single-model resources (`vllm-qwen` Deployment/Service from
`40-vllm-unified.yaml`) are **not** created — the guard skips them.

---

## How It Works Internally

### Router model-aware routing

1. On startup, the router loads `MODEL_CONFIG_PATH` (mounted from the
   shared ConfigMap) and builds a `Dict[str, ModelEntry]` registry.
2. Each model gets its own deque in `RouterState._queues`.
3. When a request arrives at `/v1/chat/completions` with `model: glm5-chat`:
   - `_resolve_model("glm5-chat")` validates against the registry
   - Request is enqueued into `_queues["glm5-chat"]`
4. When a sidecar on a `vllm-glm5-chat` pod calls `/pull` with
   `model: glm5-chat`:
   - Router pulls from `_queues["glm5-chat"]` only
   - The sidecar never gets work for the wrong model

### KV watcher multi-model discovery

When the model registry is active:

- Pod discovery runs per-model with each model's label selector
  (e.g., `model=glm5-chat`)
- Redis key scanning uses per-model prefixes
  (e.g., `glm5-chat:kvblock:*`)

### BooM config generation

When `models[]` is populated, `75-boom.yaml` emits a full `model_list`
(one entry per model, from `servedModelName`) directly into the `boom-config`
ConfigMap at Helm render time. There is no init container or runtime merge —
BooM reads the complete config straight from the mounted `boom-config`.

---

## Adding a New Model

1. Add an entry to `models[]` in your values file:

```yaml
models:
  # ... existing models ...
  - name: deepseek-v3
    servedModelName: deepseek-v3
    replicas: 2
    modelSubPath: DeepSeek-V3
    tensorParallelSize: 8
    batchSize: 16
```

2. Ensure the model weights are available on NFS at
   `<nfsPath>/<modelSubPath>`.

3. Run `helm upgrade`:

```bash
helm upgrade vllm ./vllm-kv-stack -n vllm -f values.yaml
```

This automatically:
- Creates a new `vllm-deepseek-v3` Deployment + Service
- Updates the `model-registry` ConfigMap
- Router picks up the new model on restart
- BooM picks up the new model on restart

---

## Troubleshooting

### Router returns 404 for a model name

The model name in the request must exactly match a `model_name` (which
comes from `servedModelName` or `name`) in the model registry.

```bash
# Check registered models
curl http://<node-ip>:30080/health
```

### Sidecar pulls from wrong model queue

Check the sidecar's `MODEL_NAME` env var:

```bash
kubectl exec -n vllm <pod> -c kv-sidecar -- env | grep MODEL_NAME
```

This must match the model's `servedModelName` in the `models[]` config.

### BooM can't find a model

Verify the merged config inside the BooM pod:

```bash
kubectl exec -n vllm <boom-pod> -c boom -- cat /merged/boom_config.yaml
```

The `model_list` section should contain all models.

### Per-model queue lengths

The router's `/health` endpoint reports total queue length, and the global
gauge `router_central_queue_length{namespace="vllm"}` is the cluster-wide
total. For **per-model** depth (and per-model autoscaling), use the additive
gauge with the `model` label (the model's `servedModelName`):

```
router_central_queue_length_by_model{namespace="vllm", model="<servedModelName>"}
```

Or use the `/debug/slo` endpoint for per-request tracking. For autoscaling on
this metric, see [operations/autoscaling.md](../operations/autoscaling.md).
