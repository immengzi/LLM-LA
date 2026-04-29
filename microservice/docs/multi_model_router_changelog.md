# Multi-Model Router — Changelog

Track of all files added/modified to enable multi-model routing support
and the subsequent unified config refactor that consolidated all three
deployment modes (single-model, multi-model, data-parallel) into a
single `models[]` configuration surface.

---

## v2 — Unified Config Refactor

### Summary

Replaced three separate Helm templates and three separate config
variable sets with a single unified `models[]` list that handles all
deployment modes. Each model entry can optionally include a
`dataParallel:` block to deploy as a LeaderWorkerSet instead of a
standard Deployment.

### New Files

| File | Description |
|------|-------------|
| `vllm-kv-stack/templates/40-vllm-unified.yaml` | Single template that handles all modes — loops `models[]`, renders Deployment or LWS per entry |

### Deleted Files

| File | Reason |
|------|--------|
| `vllm-kv-stack/templates/40-vllm.yaml` | Merged into `40-vllm-unified.yaml` |
| `vllm-kv-stack/templates/41-vllm-multi.yaml` | Merged into `40-vllm-unified.yaml` |
| `vllm-kv-stack/templates/43-vllm-lws.yaml` | Merged into `40-vllm-unified.yaml` |

### Modified Files

| File | What changed |
|------|--------------|
| `config.py` | +`migrate_legacy_helm_to_models()` function that auto-converts legacy flat `vllm_*/data_parallel_*` fields into a `models[]` entry with deprecation warning |
| `sweep_methods.py` | Removed 60+ line flat `vllm.*` and `dataParallel.*` `--set` blocks; always calls `migrate_legacy_helm_to_models()` and writes `models[]` YAML overlay |
| `deploy_vllm.py` | Removed duplicate vLLM flag mapping; uses same `migrate_legacy_helm_to_models()` + models YAML approach |
| `vllm-kv-stack/values.yaml` | Expanded `models[]` documentation to cover all three modes including per-model `dataParallel:`; top-level `dataParallel:` kept as legacy fallback |
| `vllm-kv-stack/templates/_helpers.tpl` | +`vllmkv.vllmRuntimeFlags` (shared vLLM CLI flags), +`vllmkv.vllmBaseEnv` (shared env vars), +`vllmkv.nicDetectScript` (NIC auto-detection), +`vllmkv.threadExporter` (Prometheus exporter) |
| `configs/boom-claude.yaml` | Migrated to `models[]` format (single Qwen3-8B) |
| `configs/boom-claude-glm.yaml` | Migrated to `models[]` format (single GLM-5) |
| `configs/boom-claude-glm-dp.yaml` | Migrated to `models[]` format (GLM-5 with `dataParallel:`) |
| `configs/multi-model-example.yaml` | Removed redundant top-level `replicas`/`batch_size`/`tensor_parallel_size` |

### Config Migration

**Before** (three different formats):

```yaml
# Single model — flat vllm_* fields
helm:
  replicas: 2
  tensor_parallel_size: 8
  vllm_quantization: ascend
  vllm_gpu_memory_utilization: 0.95

# Data parallel — flat data_parallel_* fields
helm:
  data_parallel_enabled: true
  data_parallel_size: 2

# Multi-model — models[] list
helm:
  models:
    - name: glm5-chat
      ...
```

**After** (one format for all):

```yaml
helm:
  models:
    - name: glm5-chat
      servedModelName: served-model
      replicas: 2
      tensorParallelSize: 8
      batchSize: 32
      vllm:
        quantization: ascend
        gpuMemoryUtilization: 0.95
      # Optional — only for DP deployments:
      dataParallel:
        enabled: true
        size: 2
```

Legacy flat configs are auto-migrated at runtime with a deprecation
warning. No immediate breakage.

---

## v1 — Multi-Model Router (initial)

### New Files (create these)

| File | Description |
|------|-------------|
| `vllm-kv-stack/templates/10-model-registry.yaml` | Shared ConfigMap `model-registry` — single source of truth for model definitions consumed by both BooM and the router |
| `vllm-kv-stack/templates/41-vllm-multi.yaml` | Per-model vLLM Deployments + Services (superseded by `40-vllm-unified.yaml` in v2) |
| `docs/multi_model_router.md` | Full documentation (architecture, config, deployment, backward compat) |
| `docs/multi_model_router_changelog.md` | This file |

### Modified Files (diff carefully)

| File | What changed |
|------|--------------|
| `services/router_service/router/config.py` | +`MODEL_CONFIG_PATH` field, +`ModelEntry` dataclass, +`load_model_registry()`, +`get_model_registry()`, +`get_known_models()` |
| `services/router_service/router/router_state.py` | Replaced `self._queue` with `self._queues: Dict[str, deque]`; `enqueue()` and `pull_for_endpoint()` accept `model` param |
| `services/router_service/router/models.py` | +`model: str = ""` on `PullRequest` and `EnqueueRequest` |
| `services/router_service/router/api.py` | +`_resolve_model()` helper; `/pull`, `/enqueue`, `/submit`, `/v1/chat/completions` all route by model; `/health` reports registered models |
| `services/router_service/router/kv_watcher.py` | `_discover_pods()` accepts `label_selector` param; per-model discovery and Redis key prefix scanning |
| `services/sidecar/sidecar/router_client.py` | Added `"model": _cfg.MODEL_NAME` to `/pull` request body |
| `services/go/internal/sidecar/pull_worker.go` | +`Model string` field on `pullRequest` struct, populated from `w.cfg.ModelName` |
| `config.py` | +`models: list = field(default_factory=list)` on `HelmConfig` |
| `sweep_methods.py` | Writes `models` list to temp values file, passes via `-f`; updated `_helm_install_or_upgrade` for `extra_values_files`; updated `_vllm_pods_exist` for multi-model pods; added models to `sweep_meta.json` |
| `vllm-kv-stack/values.yaml` | +`models: []` section with documentation |
| `vllm-kv-stack/templates/40-vllm.yaml` | Guard updated to `(not .Values.models)` — skips single-model Deployment when multi-model is active |
| `vllm-kv-stack/templates/31-router.yaml` | Conditional `MODEL_CONFIG_PATH` env var + model-registry volume mount when `models[]` is populated |
| `vllm-kv-stack/templates/75-boom.yaml` | Init container merges shared `models.yaml` with BooM settings; `boom-config` ConfigMap omits `model_list` in multi-model mode; Claude aliases point to first model |

---

## Detailed Change Log

### services/router_service/router/config.py

**New config field** on `RouterConfig`:

```python
MODEL_CONFIG_PATH: str = ""  # /etc/model-registry/models.yaml (empty = single-model legacy)
```

**New dataclass** `ModelEntry`:

```python
@dataclass
class ModelEntry:
    name: str
    label_selector: str = ""
    batch_size: int = 0
```

**New functions**:

- `load_model_registry(path)` — parses `models.yaml`, builds `Dict[str, ModelEntry]` keyed by `model_name`
- `get_model_registry()` — returns the loaded registry or `None` (single-model mode)
- `get_known_models()` — returns sorted list of registered model names

On startup (`get_config()`), if `MODEL_CONFIG_PATH` points to a valid file, the registry is loaded and cached as a module-level `_MODEL_REGISTRY`.

### services/router_service/router/router_state.py

**Queue refactor** — replaced single `self._queue: deque` with:

```python
self._queues: Dict[str, Deque[Tuple[str, str, float, dict]]] = {}
```

Key method signature changes:

```python
def enqueue(self, prompt, t_enq_client, meta, model="") -> str
def pull_for_endpoint(self, endpoint, want, model="") -> List[JobItem]
def size(self, model="") -> int
```

Added `_get_queue(model)` helper and `_total_size()` for cross-queue metrics. All methods default to `_DEFAULT_MODEL` (= `MODEL_NAME` from config) when no model specified.

### services/router_service/router/models.py

Added `model: str = ""` to both:

- `EnqueueRequest` — used by `/enqueue` and `/submit`
- `PullRequest` — used by `/pull` (sidecar sends its `MODEL_NAME`)

Both default to empty string for backward compatibility.

### services/router_service/router/api.py

**New helper** `_resolve_model(model)`:

- Multi-model mode: validates against registry, raises 404 for unknown models
- Single-model mode: always returns `MODEL_NAME`

Updated endpoints:

- `/pull` — reads `req.model`, passes to `pull_for_endpoint(..., model=model)`
- `/enqueue` — reads `req.model`, passes to `router_state.enqueue(..., model=model)`
- `/submit` — same as `/enqueue`
- `/v1/chat/completions` — passes `req.model` through `_enqueue_and_wait()`
- `/health` — includes `"models": [...]` in response when registry is active

### services/router_service/router/kv_watcher.py

- `_discover_pods(label_selector="")` now accepts an explicit label selector
- When model registry is active, discovery iterates per-model with each model's `label_selector`
- `_scan_once(redis, pods, model_name="")` uses per-model Redis key prefix `{model_name}:kvblock:*`

### services/sidecar/sidecar/router_client.py

Added `"model": _cfg.MODEL_NAME` to the `/pull` request body:

```python
json={"endpoint": self.endpoint_id, "want": want, "model": _cfg.MODEL_NAME}
```

### services/go/internal/sidecar/pull_worker.go

Added `Model` field to `pullRequest` struct:

```go
type pullRequest struct {
    Endpoint string `json:"endpoint"`
    Want     int    `json:"want"`
    Model    string `json:"model"`
}
```

Populated from `w.cfg.ModelName` in `doPull()`.

### config.py (HelmConfig)

New field:

```python
models: list = field(default_factory=list)
```

Each entry is a dict with keys: `name`, `servedModelName`, `replicas`, `modelSubPath`, `tensorParallelSize`, `batchSize`, `vllm` (nested dict).

### sweep_methods.py

- When `models` list is non-empty, writes to temp values file and passes to Helm via `-f`
- `_helm_install_or_upgrade` accepts new `extra_values_files` parameter
- `_vllm_pods_exist` also detects multi-model pods (labelled `model=...`)
- `helm_knobs_from_config` in `sweep_meta.json` includes `"models"` list

### vllm-kv-stack/values.yaml

New top-level section:

```yaml
models: []
```

With comprehensive documentation and example entries.

### vllm-kv-stack/templates/10-model-registry.yaml (NEW)

Shared ConfigMap `model-registry`. Only rendered when `models[]` is non-empty. Generates `models.yaml` with `model_list` entries containing both `litellm_params` (for BooM) and `router_params` (for the router).

### vllm-kv-stack/templates/41-vllm-multi.yaml (NEW)

Per-model Deployment + Service. Only rendered when `models[]` is non-empty. Each model gets:

- Deployment `vllm-{name}` with labels `app: vllm-{name}`, `model: {name}`
- Sidecar with `MODEL_NAME` and `MODEL_NAME_REDIS` set to `servedModelName`
- ClusterIP Service `vllm-{name}` for direct access
- Per-model vLLM flags from the model's `vllm:` sub-config

### vllm-kv-stack/templates/40-vllm.yaml

Guard changed from:

```
{{- if and .Values.deploy.vllm (not .Values.dataParallel.enabled) }}
```

To:

```
{{- if and .Values.deploy.vllm (not .Values.dataParallel.enabled) (not .Values.models) }}
```

Skips single-model Deployment when multi-model is active.

### vllm-kv-stack/templates/31-router.yaml

When `models[]` is populated:

- Adds `MODEL_CONFIG_PATH=/etc/model-registry/models.yaml` env var
- Mounts `model-registry` ConfigMap at `/etc/model-registry/models.yaml`
- Adds corresponding volume definition

### vllm-kv-stack/templates/75-boom.yaml

When `models[]` is populated:

- `boom-config` ConfigMap omits `model_list` (comes from shared ConfigMap)
- Init container (`merge-config`) concatenates `models.yaml` + BooM settings
- BooM reads merged config from `/merged/boom_config.yaml`
- Claude Code aliases point to first model in `models[]` list
