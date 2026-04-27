# Multi-Model Router

Serve 10+ models through a single router instance, each with its own
request queue and vLLM pod pool, while keeping BooM as the unified
external gateway.

---

## Architecture

```
                           ┌──────────────────────────────────┐
                           │  ConfigMap: model-registry       │
                           │  (models.yaml — single source    │
                           │   of truth)                      │
                           └──────┬──────────────┬────────────┘
                                  │              │
                        vol mount │    vol mount │
                                  ▼              ▼
  Client ──► BooM Gateway ──► Router Service ──► Sidecar ──► vLLM
              (reads              (reads
              litellm_params)     router_params)
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

Both BooM and the router mount the same `model-registry` ConfigMap.
This ConfigMap is generated from the `models[]` list in `values.yaml`:

- BooM reads `model_name` + `litellm_params` (ignores `router_params`)
- Router reads `model_name` + `router_params` (ignores `litellm_params`)

No duplicate model definitions. One place to add/remove/rename models.

---

## Backward Compatibility

When `models: []` (the default), everything works exactly as before:

| Aspect | Single-model (default) | Multi-model |
|--------|----------------------|-------------|
| `model-registry` ConfigMap | Not created | Created |
| vLLM Deployments | `vllm-qwen` (from `40-vllm.yaml`) | `vllm-{name}` per model (from `41-vllm-multi.yaml`) |
| Router queues | Single `_queue` | Per-model `_queues[model]` |
| KV watcher | Single `LABEL_SELECTOR` | Per-model label selectors |
| BooM config | Inline `model_list` | Merged from shared ConfigMap |
| Sidecar `/pull` | `model: ""` (ignored) | `model: "glm5-chat"` |

Existing single-model configs (`boom-claude-glm-dp.yaml`, etc.) are
completely unaffected.

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
| `replicas` | No | 1 | Number of vLLM pods for this model |
| `modelSubPath` | No | `modelVolume.modelSubPath` | NFS subpath to model weights |
| `tensorParallelSize` | No | `tensorParallelSize` (global) | TP size for this model |
| `batchSize` | No | `batchSize` (global) | Sidecar batch size for this model |
| `image` | No | `images.vllm` | vLLM container image (override per model) |
| `vllm` | No | `{}` | Per-model vLLM flags (see below) |

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
| `toolCallParser` | `--tool-call-parser` |
| `reasoningParser` | `--reasoning-parser` |

Unset fields fall back to the global `vllm.*` values in `values.yaml`.

### Client config (sweep runner)

In your client config YAML (e.g. `configs/multi-model.yaml`):

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
`40-vllm.yaml`) are **not** created — the guard skips them.

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

### BooM config merging

When `models[]` is populated:

1. `boom-config` ConfigMap contains only `general_settings` +
   `router_settings` (no `model_list`)
2. An init container (`merge-config`) concatenates
   `models.yaml` + `boom_config.yaml` into a single file
3. BooM reads the merged file from `/merged/boom_config.yaml`

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

The router's `/health` endpoint reports total queue length. For
per-model debugging, check the Prometheus metric:

```
router_central_queue_length{namespace="vllm"}
```

Or use the `/debug/slo` endpoint for per-request tracking.
