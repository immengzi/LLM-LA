# BooM Gateway Integration

## Overview

BooM Gateway is a Rust-based LLM API gateway that replaces LiteLLM as the
production auth/spend layer in front of the router. It provides:

- Virtual key authentication (Bearer `sk-...`)
- Per-key/team spend tracking and budget enforcement
- Rate limiting with named plans, sliding windows, and concurrency guards
- Multi-provider routing (OpenAI, Anthropic, Azure, Gemini, vLLM, Ollama)
- Embedded admin dashboard (models, keys, teams, aliases, logs)
- Zero-downtime config reloads via `ArcSwap` + SIGHUP

The `backend: boom` path is for production/demo validation only.
**Use `backend: router` for benchmarking** — it bypasses BooM entirely.

---

## Architecture

```
Client → BooM Gateway (port 30401)
       → router /v1/chat/completions (port 30080)
       → sidecars → vLLM pods
```

BooM Gateway receives OpenAI-compatible requests, enforces auth and spend
limits, then proxies to the router's `/v1/chat/completions` shim. The
mu-load-test framework sends requests through BooM using the same threaded
HTTP model as the LiteLLM backend.

---

## How it differs from LiteLLM

| Aspect | LiteLLM | BooM Gateway |
|---|---|---|
| Runtime | Python + FastAPI | Rust + Axum |
| Config format | YAML (`model_list`, `litellm_params`) | Same YAML shape |
| Startup time | ~17s (Python imports) | ~1s (native binary) |
| Memory footprint | 512Mi–2Gi | 128Mi–512Mi |
| Port (NodePort) | 30400 | 30401 |
| Container image | `litellm:main-stable` | `boom-gateway:latest` |
| Dashboard | Separate UI | Embedded SPA in binary |
| Hot reload | Restart required | SIGHUP / ArcSwap (zero downtime) |

---

## Client integration

The BooM backend reuses the exact same HTTP client code as LiteLLM — both
speak OpenAI `/v1/chat/completions` with Bearer auth. The shared functions
(`send_one_litellm`, `_request_thread_litellm_http`) accept a `label`
parameter to differentiate log messages:

- `backend: litellm` → `label="LiteLLM"`
- `backend: boom` → `label="BooM"`

### Files modified

| File | Change |
|---|---|
| `config.py` | `BooMConfig` dataclass, `boom` field on `ClientConfig` |
| `http_client.py` | `label` param on `send_one_litellm()` (default `"LiteLLM"`) |
| `load_runner.py` | `label` param on worker, `boom` backend branches |
| `main.py` | `boom` print block + `run_summary` fields |
| `sweep_methods.py` | `boom` in allowlist, Helm toggle, sweep_meta fields |

### Files created

| File | Purpose |
|---|---|
| `configs/boom.yaml` | Experiment config for Qwen3-8B via BooM |
| `vllm-kv-stack/templates/75-boom.yaml` | Helm template (ConfigMap + Deployment + Service) |
| `vllm-kv-stack/values.yaml` (`boom:` section) | Helm values (disabled by default) |
| `docs/boom_gateway.md` | This document |
| `apply_boom_patch.sh` | Patch script to copy BooM files to another repo |

---

## Building the BooM Gateway image

BooM Gateway is built from `BooMGateway-main/` and pushed to your private
registry:

```bash
cd /home/saeid/microservice/BooMGateway-main

# Build the release binary
cargo build --release -p boom-main

# Build and push the container image
docker build -t reg.local:32000/boom-gateway:latest .
docker push reg.local:32000/boom-gateway:latest
```

The Helm chart uses `global.imageRegistry` rewriting, so if your registry is
set in `values.yaml`, the image reference is automatically prefixed.

---

## Running a BooM Gateway sweep

### 1. Deploy vLLM (if not already running)

```bash
python deploy_vllm.py --config configs/boom.yaml
```

### 2. Run the sweep

```bash
python sweep_methods.py --config boom_master --skip-vllm
```

Where `configs/boom_master.yaml` contains:

```yaml
configs/boom.yaml:
  - boom-pull
```

The sweep runner will:
1. Set `boom.enabled=true` and `boom.masterKey` in the Helm upgrade
2. Deploy the BooM Gateway pod alongside the router
3. Run `main.py` with `backend=boom`, routing through BooM

### 3. Single experiment (without sweep)

```bash
python main.py --config boom
```

---

## Configuration reference

### Client config (`configs/boom.yaml`)

```yaml
backend: "boom"

boom:
  base_url: "http://7.216.57.215:30401"
  chat_path: "/v1/chat/completions"
  model: "served-model"
  api_key: "sk-boom-master"
  timeout_s: 1000.0
  stream: false
```

### Helm values (`vllm-kv-stack/values.yaml`)

```yaml
boom:
  enabled: false              # set true to deploy BooM Gateway pod
  image: boom-gateway:latest
  masterKey: "sk-boom-master"
  nodePort: 30401
  databaseUrl: ""             # optional Postgres for spend persistence
  resources:
    requests:
      cpu: "250m"
      memory: "128Mi"
    limits:
      cpu: "2000m"
      memory: "512Mi"
```

### Key config differences (boom.yaml vs litellm.yaml)

| Field | litellm.yaml | boom.yaml |
|---|---|---|
| `backend` | `litellm` | `boom` |
| `litellm.base_url` | `http://<node>:30400` | not used |
| `boom.base_url` | not used | `http://<node>:30401` |
| `boom.api_key` | not used | `sk-boom-master` |

---

## Applying the patch to another repo

```bash
./apply_boom_patch.sh ~/microservice ~/external-microservice
```

This copies all new and modified files, creates backups of overwritten files,
and prints a summary. To undo, restore from the backup directory.

---

## Common issues

| Symptom | Fix |
|---|---|
| BooM pod `CrashLoopBackOff` | Check config YAML mount — `boom_config.yaml` must be valid |
| `Connection refused` on 30401 | BooM pod not ready yet — check readiness probe logs |
| `401 Unauthorized` | API key mismatch — check `boom.masterKey` in Helm and `boom.api_key` in client config |
| Image pull error | Build and push `boom-gateway:latest` to `reg.local:32000` |
| BooM starts but 502 to router | Router `/v1/chat/completions` shim not present — rebuild router image from latest source |
