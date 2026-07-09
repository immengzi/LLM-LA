# BooM Gateway framework integration (notes)

> Internal development record of how `backend: "boom"` was added to the load framework. For usage, see [BooM overview](../gateways/boom/overview.md) and [build](../gateways/boom/build.md).

## Overview

BooM Gateway was integrated into the LA-Boom load framework as
`backend: "boom"` — a drop-in alternative to `backend: "litellm"`. Both
speak the same OpenAI-compatible `/v1/chat/completions` protocol with Bearer
auth, so the integration reuses the LiteLLM HTTP client functions with a
`label` parameter to differentiate log messages.

---

## Design: maximum reuse, zero duplication

The BooM backend reuses `send_one_litellm()` and
`_request_thread_litellm_http()` directly — no copied functions. A `label`
parameter (default `"LiteLLM"`) controls log/error message prefixes:

- `backend: litellm` → calls with `label="LiteLLM"` (unchanged behavior)
- `backend: boom` → calls with `label="BooM"`

Duck typing makes this work: `BooMConfig` has the same fields as
`LiteLLMConfig` (`base_url`, `chat_path`, `model`, `api_key`, `timeout_s`,
`stream`), so the functions accept either.

---

## Files modified (7)

### config.py
- Added `BooMConfig` dataclass (same fields as `LiteLLMConfig`, different defaults)
- Added `boom: BooMConfig` field on `ClientConfig`
- Added `"boom"` to backend allowlist
- Added URL normalization block for `backend == "boom"`

### http_client.py
- Added `label: str = "LiteLLM"` parameter to `send_one_litellm()`
- Replaced 7 hardcoded `"LiteLLM"` strings with `label` / `label_lower`
- Added `BooMConfig` import

### load_runner.py
- Added `label: str = "LiteLLM"` parameter to `_request_thread_litellm_http()`
- Passes `label` through to `send_one_litellm()`
- Added `boom: Optional[BooMConfig] = None` to `run_open_loop_load()`
- Added `"boom"` to backend allowlist
- Added warmup branch for `backend == "boom"` (calls `send_one_litellm` with `label="BooM"`)
- Added dispatch block for `backend == "boom"` (threads call `_request_thread_litellm_http` with `label="BooM"`)

### main.py
- Added `backend == "boom"` summary print block
- Added `boom=getattr(cfg, "boom", None)` to `run_open_loop_load()` call
- Added `boom_*` fields to `run_summary.json`

### sweep_methods.py
- Added `"boom"` to backend allowlist
- Added method-as-label echo for `backend == "boom"`
- Added `boom.enabled` / `boom.masterKey` Helm toggle
- Added `boom_*` fields to `sweep_meta.json`

### vllm-kv-stack/values.yaml
- Added `boom:` section (enabled: false, image, masterKey, nodePort: 30401, databaseUrl, resources)

### configs/1-master_config.yaml
- Added commented-out `boom:` sweep entry

---

## Files created (6)

| File | Purpose |
|---|---|
| `configs/boom.yaml` | Experiment config for `backend: "boom"` (Qwen3-8B) |
| `configs/boom_master.yaml` | Sweep master config mapping boom.yaml → methods |
| `vllm-kv-stack/templates/75-boom.yaml` | Helm template: ConfigMap + Deployment + Service (gated by `boom.enabled`) |
| `docs/gateways/boom/overview.md` | Full documentation |
| `src/core/boom-integration/Dockerfile` | Container image (host-compiled binary + openeuler) |

---

## Backward compatibility

All changes are fully backward compatible:

- New function parameters have defaults (`label="LiteLLM"`, `boom=None`)
- `boom.enabled` defaults to `false` — existing Helm deploys are unaffected
- Backend allowlists are supersets of old values
- No existing code paths modified — only new `elif backend == "boom"` branches
- Every existing config file, experiment, and sweep works identically

---

## Running

```bash
# Single experiment
python src/client/main.py --config boom

# Sweep
python src/client/sweep_methods.py --config boom_master --skip-vllm
```

## Config reference

### Client config (configs/boom.yaml)

```yaml
backend: "boom"

boom:
  base_url: "http://<node-ip>:30401"
  chat_path: "/v1/chat/completions"
  model: "served-model"
  api_key: "sk-boom-master"
  timeout_s: 1000.0
  stream: false
```

### Helm values (boom section in values.yaml)

```yaml
boom:
  enabled: false
  image: boom-gateway:v5
  masterKey: "sk-boom-master"
  nodePort: 30401
  databaseUrl: ""
  resources:
    requests: { cpu: "250m", memory: "128Mi" }
    limits:   { cpu: "2000m", memory: "512Mi" }
```
