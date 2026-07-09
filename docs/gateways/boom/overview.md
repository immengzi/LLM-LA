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
| Container image | `litellm:main-stable` | `boom-gateway:v5` (chart default) |
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

BooM Gateway is built from the Rust source at `boom-gateway/`
(`cargo build --release -p boom-main`) and pushed to your private registry as
`boom-gateway:v5`. See [build.md](build.md) for the full, proxy-aware build and
push procedure.

The Helm chart uses `global.imageRegistry` rewriting, so if your registry is
set in `values.yaml`, the image reference is automatically prefixed.

---

## Running a BooM Gateway sweep

### 1. Deploy vLLM (if not already running)

```bash
python src/client/deploy_vllm.py --config configs/boom.yaml
```

### 2. Run the sweep

```bash
python src/client/sweep_methods.py --config boom_master --skip-vllm
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
python src/client/main.py --config boom
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

## openEuler Gateway implementation notes

The BooM Gateway source used here is aligned with `openeuler/gateway`:

| Field | Value |
|---|---|
| Source repo | `/home/haiting/llm-la/boom-gateway` |
| Upstream | `https://gitcode.com/openeuler/gateway` |
| Branch | `master` |
| HEAD | `6e76f6b !34 feat(rewrite): strip Claude Code attribution block from /v1/messages` |

The gateway is a Rust workspace with the main crates under
`boom-gateway/boom-gateway/`: `boom-core`, `boom-config`, `boom-auth`,
`boom-provider`, `boom-limiter`, `boom-routing`, `boom-audit`,
`boom-flowcontrol`, `boom-main`, `boom-dashboard`, `boom-promptlog`, and
`boom-kvindex`.

### Route-based protocol selection

BooM chooses the semantic protocol by request path and request type, not by
client fingerprints:

| Route | Handler | Meaning |
|---|---|---|
| `/v1/chat/completions` | `routes::chat_completions` | OpenAI-compatible chat |
| `/v1/messages` | `routes::messages` | Anthropic Messages / Claude Code |
| `/v1/completions` | `routes::completions` | Legacy OpenAI completions |
| `/internal/kv-index` | `routes::kv_index_status` | Internal KVC debug endpoint |

There is no Codex-specific route, header parser, or client enum. Codex follows
the OpenAI-compatible path if it uses OpenAI Chat Completions or legacy
Completions. The auth extractor accepts `Authorization: Bearer ...`,
Anthropic-style `x-api-key`, and Azure-style `api-key`. `allowed_routes` exists
in the auth model, but current request handlers do not appear to enforce it.

### Claude Code and Anthropic conversion

Claude Code is expected to send Anthropic Messages traffic:

```text
Claude Code
  -> POST /v1/messages
  -> routes::messages()
  -> optional strip_cc_attribution_anthropic()
  -> anthropic_request_to_openai()
  -> KVC tokenization + provider selection
  -> provider.chat/chat_stream(OpenAI internal request)
  -> upstream OpenAI-compatible vLLM / Anthropic / other provider
  -> response converted back to Anthropic shape
```

The `/v1/messages` handler strips Claude Code attribution before converting to
an internal OpenAI request. The rewrite is controlled by
`router_settings.strip_claude_code_attribution` and is disabled by default. It
targets `x-anthropic-billing-header` blocks in system-semantic positions:

- Top-level system text blocks whose text starts with
  `x-anthropic-billing-header`.
- Nested `role="system"` message text blocks whose text starts with that prefix.
- String-form system prompts are left untouched.
- User-role messages are not scanned.

This is not a whole-request search-and-delete. It removes the changing `cch=`
attribution block Claude Code can inject, restoring stable token prefixes for
KVC-aware routing. Enable it only when routing Claude Code to a non-Anthropic
backend, because stripping can change behavior against the official Anthropic
API.

Anthropic-to-OpenAI conversion lives in `boom-core/src/anthropic.rs`.
Top-level `system` becomes an OpenAI system message, Anthropic
`tools[].input_schema` becomes OpenAI `tools[].function.parameters`,
Anthropic `tool_use` becomes OpenAI assistant `tool_calls`, and Anthropic
`tool_result` becomes OpenAI `role=tool`. Responses are converted back to
Anthropic shape, including `thinking` / `reasoning_content` and streaming
tool-call deltas.

### Provider mapping and KVC worker identity

Model routing resolves in this order:

```text
client model
  -> alias / hybrid router resolved_model
  -> DeploymentStore candidate providers for resolved_model
  -> provider actual model from litellm_params.model
  -> OpenAIProvider rewrites request model to actual provider model
  -> upstream vLLM receives that model
```

For KVC-aware routing, the provider worker identity comes from
`OpenAIProvider::kv_worker_id()`, which is derived from the host part of
`litellm_params.api_base`. For example,
`http://10.0.0.5:8000/v1` maps to worker id `10.0.0.5`. The vLLM ZMQ topic
worker id must match this value. If `api_base` uses a Service/VIP while ZMQ
publishes pod IPs or pod names, KVC-aware selection can degrade or fail to find
a match. In this implementation, `model_info.id` is not the KVC worker
identity.

### KVC-aware routing path

The current gateway has a Rust-native KVC-aware subsystem in
`boom-kvindex`:

```text
Client
  -> BooM Gateway route handler
  -> optional protocol rewrite / Anthropic -> OpenAI conversion
  -> TokenizerPool tokenizes request for resolved model
  -> Router.select_provider_with_prefix()
  -> KvcAwarePolicy queries TokenPrefixIndex
  -> selected Provider sends request to upstream vLLM/OpenAI-compatible endpoint

In background:
vLLM ZMQ PUB
  -> boom-kvindex subscriber
  -> vllm_event parser
  -> GatewayKvEvent
  -> TokenPrefixIndex trie
```

Enable it with `router_settings.routing_strategy: kvc_aware`. Important KVC
settings include:

| Setting | Purpose |
|---|---|
| `block_size` | Token block size; must match vLLM `--block-size` |
| `cache_weight` | Weight for prefix hit ratio |
| `tier_weight` | Weight for GPU/CPU/SSD/remote storage tier |
| `load_weight` | Weight for worker load, though current ZMQ events usually do not update load |
| `tokenizer_dir` | Directory containing `{model}/tokenizer.json` |
| `zmq_endpoints` | vLLM ZMQ PUB endpoints |
| `zmq_topic_prefix` | Default `kv@` |
| `full_report_hit_threshold` | Default `0.8`; below this hit ratio, request full KV reporting |

The ZMQ subscriber expects frames shaped as:

```text
[topic, seq, msgpack_payload]
```

Topics are expected to look like `kv@{worker_id}@{model}`. Stored block events
are converted into `GatewayKvEvent` records and inserted into a token-prefix
trie. Trie edges are `xxhash3_64` hashes of token blocks, not vLLM block
hashes. vLLM `block_hash` is still stored to maintain parent and eviction
relationships.

The routing policy asks the `TokenPrefixIndex` for matches against candidate
worker ids. The score is:

```text
combined_score = cache_weight * hit_ratio
               + tier_weight * tier_score
               + load_weight * load_score
```

If there is only one candidate, if tokenization yields no token ids, or if no
KVC match is found, the policy falls back to the lowest-load selection path.

### Full KV report injection

When KVC is enabled and `kv_hit_ratio < full_report_hit_threshold`, the gateway
sets an internal `kv_cache_report_full` flag on the OpenAI request. The
OpenAI-compatible provider then injects:

```json
{
  "vllm_xargs": {
    "kv_cache_report_mode": "full"
  }
}
```

This is only implemented in the OpenAI-compatible provider path. Non-OpenAI
providers will not receive the full-report hint unless a separate adapter adds
that behavior.

### Practical requirements and risks

To make KVC-aware routing work end-to-end:

- Configure `router_settings.routing_strategy: kvc_aware`.
- Match `router_settings.kvc_aware.block_size` with vLLM `--block-size`.
- Provide per-model tokenizer assets under `tokenizer_dir`.
- Configure `zmq_endpoints` for all vLLM workers.
- Ensure ZMQ topics use `kv@{worker_id}@{model}`.
- Ensure each ZMQ `worker_id` equals the provider worker id derived from
  `api_base` host.
- Enable `strip_claude_code_attribution: true` for Claude Code routed to
  non-Anthropic backends.
- Use an OpenAI-compatible provider if relying on `vllm_xargs` full-report
  injection.

Known caveats:

- `tokenize_anthropic()` does not receive Anthropic `tools`; Claude Code
  tool-heavy requests need validation because downstream OpenAI conversion does
  include tool schemas.
- `tokenize_anthropic()` only lifts string-form top-level system prompts into
  messages. Other block-form system content may need validation.
- Worker id alignment is strict and can break when `api_base` and ZMQ identity
  use different naming schemes.
- The upstream design doc describes trie keys as raw token blocks, while this
  implementation uses `u64` `xxhash3_64` trie edges.

---

## Common issues

| Symptom | Fix |
|---|---|
| BooM pod `CrashLoopBackOff` | Check config YAML mount — `boom_config.yaml` must be valid |
| `Connection refused` on 30401 | BooM pod not ready yet — check readiness probe logs |
| `401 Unauthorized` | API key mismatch — check `boom.masterKey` in Helm and `boom.api_key` in client config |
| Image pull error | Build and push `boom-gateway:v5` to `reg.local:32000` |
| BooM starts but 502 to router | Router `/v1/chat/completions` shim not present — rebuild router image from latest source |
