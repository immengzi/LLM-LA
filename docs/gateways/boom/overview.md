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
Client → BooM Gateway (NodePort 30401)
       → router-service:8080/v1/chat/completions (in-cluster)
       → sidecars → vLLM pods
```

BooM Gateway receives OpenAI-compatible requests, enforces auth and spend
limits, then proxies to the router's `/v1/chat/completions` shim at the
in-cluster address `http://router-service:8080/v1` (30080 is the router's
external NodePort, not the path BooM uses internally). The
LA-Boom load framework sends requests through BooM using the same threaded
HTTP model as the LiteLLM backend.

---

## How it differs from LiteLLM

| Aspect | LiteLLM | BooM Gateway |
|---|---|---|
| Runtime | Python + FastAPI | Rust + Axum |
| Config format | YAML (`model_list`, `litellm_params`) | Same YAML shape |
| Startup time | ~17s (Python imports) | fast native binary (probe `initialDelaySeconds: 10`) |
| Memory footprint | 512Mi–2Gi | 128Mi–512Mi |
| Port (NodePort) | 30400 | 30401 |
| Container image | `litellm:main-stable` | `boom-gateway:v4` (chart default) |
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
| `docs/gateways/boom/overview.md` | This document |

---

## Building the BooM Gateway image

BooM Gateway is built from the Rust source at `boom-src/BooMGateway-main/BooMGateway-main/`
(`cargo build --release -p boom-main`) and pushed to your private registry as
`boom-gateway:v4`. See [build.md](build.md) for the full, proxy-aware build and
push procedure.

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
boom:
  - pull
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

The full BooM configuration is documented in [models.md](models.md): **Layer 1** (client `configs/boom.yaml` — `backend: "boom"` with a `boom:` block: `base_url` on NodePort 30401, `model`, `api_key`, `timeout_s` default `7200`) and **Layer 3** (Helm `boom.*` values — `enabled`, `image`, `masterKey`, `nodePort` 30401, optional `databaseUrl`). See also the [Helm values reference](../../configuration/helm-values.md#boom-gateway-boom).

### Key config differences (boom.yaml vs litellm.yaml)

| Field | litellm.yaml | boom.yaml |
|---|---|---|
| `backend` | `litellm` | `boom` |
| `litellm.base_url` | `http://<node>:30400` | not used |
| `boom.base_url` | not used | `http://<node>:30401` |
| `boom.api_key` | not used | `sk-boom-master` |

---

## Common issues

| Symptom | Fix |
|---|---|
| BooM pod `CrashLoopBackOff` | Check config YAML mount — `boom_config.yaml` must be valid |
| `Connection refused` on 30401 | BooM pod not ready yet — check readiness probe logs |
| `401 Unauthorized` | API key mismatch — check `boom.masterKey` in Helm and `boom.api_key` in client config |
| Image pull error | Build and push `boom-gateway:v4` to `reg.local:32000` |
| BooM starts but 502 to router | Router `/v1/chat/completions` shim not present — rebuild router image from latest source |
