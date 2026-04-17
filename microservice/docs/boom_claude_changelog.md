# BooM Gateway + Claude Code Integration — Changelog

Track of all files added/modified to enable a live Claude Code agent to
connect through BooM Gateway to the vLLM cluster.

---

## Files to Copy

### New Files (create these)

| File | Description |
|------|-------------|
| `configs/boom-claude.yaml` | Deployment config (`backend: boom` + `boom_claude_aliases: true`, 8-request validation) |
| `docs/boom_claude.md` | Full integration guide (architecture, deploy, settings.json, debug) |
| `docs/boom_claude_changelog.md` | This file |
| `apply_boom_claude_patch.sh` | Standalone patch script (copies only boom-claude additions) |

### Modified Files (diff carefully)

| File | What changed |
|------|--------------|
| `config.py` | +1 field: `boom_claude_aliases: bool = False` on `HelmConfig` dataclass |
| `sweep_methods.py` | Reads `h.boom_claude_aliases`, sets `boom.claudeCodeAliases` in Helm values, writes to `sweep_meta.json` |
| `vllm-kv-stack/templates/75-boom.yaml` | Conditional `router_settings.model_group_alias` block when `boom.claudeCodeAliases=true` |
| `vllm-kv-stack/values.yaml` | +2 values: `boom.claudeCodeAliases: false`, `boom.extraModelAliases: []` |
| `configs/1-master_config.yaml` | Boom + boom-claude entries (boom active, boom-claude commented) |
| `apply_boom_patch.sh` | Added boom-claude files to `NEW_FILES` array |

---

## Detailed Change Log

### config.py

**HelmConfig dataclass** — 1 new field after existing boom knobs:

```python
boom_claude_aliases: bool = False
```

When `True`, the Helm upgrade enables `boom.claudeCodeAliases` so Claude's
default model names resolve to `served-model` in BooM's config.

### sweep_methods.py

**BooM deployment block** — 2 additions:

1. Reads `boom_claude_aliases` from HelmConfig and passes it to Helm:

```python
boom_claude_aliases = bool(getattr(h, "boom_claude_aliases", False))
set_values["boom.claudeCodeAliases"] = boom_claude_aliases
```

2. Writes `boom_claude_aliases` to `sweep_meta.json` for experiment reproducibility.

### vllm-kv-stack/templates/75-boom.yaml

**ConfigMap** — conditional block added after `general_settings`:

```yaml
{{- if .Values.boom.claudeCodeAliases }}
router_settings:
  model_group_alias:
    "claude-sonnet-4-20250514": "served-model"
    "claude-3-5-sonnet-20241022": "served-model"
    "claude-3-haiku-20240307": "served-model"
    "claude-3-opus-20240229": "served-model"
    {{- range .Values.boom.extraModelAliases }}
    {{ .alias | quote }}: {{ .target | quote }}
    {{- end }}
{{- end }}
```

Maps Claude's default model names to `served-model` so Claude Code can use
its built-in model names without manual overrides.

### vllm-kv-stack/values.yaml

**boom section** — 2 new values:

```yaml
boom:
  claudeCodeAliases: false
  extraModelAliases: []
```

### configs/1-master_config.yaml

Boom entry uncommented (was previously commented). Boom-claude entry added
(commented) after the boom block:

```yaml
boom:
  - pull
  # - push-rr
  # - push-random
  # - push-least-queue

# boom-claude:
#   - pull
```

The separate `configs/boom_master.yaml` was removed — all entries live in
`1-master_config.yaml`.

---

## What Already Existed (no changes needed)

The BooM Gateway Rust code already has full Anthropic `/v1/messages` support:

| Component | File | What it does |
|-----------|------|-------------|
| Route handler | `boom-main/src/routes.rs` (`messages()`) | Accepts Anthropic requests, converts to OpenAI, routes to provider |
| Request conversion | `boom-core/src/anthropic.rs` (`anthropic_request_to_openai()`) | Anthropic Messages → OpenAI ChatCompletion (system, tools, thinking, tool_result) |
| Response conversion | `boom-core/src/anthropic.rs` (`openai_response_to_anthropic()`) | OpenAI ChatCompletion → Anthropic Messages (text, tool_use, thinking blocks) |
| Stream transcoder | `boom-core/src/anthropic.rs` (`AnthropicStreamTranscoder`) | Per-token OpenAI SSE → Anthropic SSE events (message_start, content_block_delta, etc.) |
| Error handling | `boom-main/src/routes.rs` (`AnthropicErrorReply`) | Anthropic-format error responses (not OpenAI format) |

These were implemented as part of the base BooM Gateway feature, not this
Claude Code integration. This changelog only covers the deployment/config
plumbing to activate Claude aliases and point Claude Code at BooM.

---

## Applying the Patch

```bash
./apply_boom_claude_patch.sh ~/microservice ~/external-microservice
```

The target repo must already have the base BooM patch (`75-boom.yaml` must
exist). The script copies from source to target, backs up overwritten files,
and prints next steps.

A `boom_claude_patch.tar.gz` is also provided for transporting the source
files to another machine. Extract it and use as the source directory.

---

## Rollback

Set `boom.claudeCodeAliases=false` (the default). The aliases are not
rendered in the BooM ConfigMap and Claude Code falls back to using
`served-model` directly.

To fully remove: delete `configs/boom-claude.yaml`, remove the commented
entries from the master configs, and revert the 1-line additions in
`config.py`, `sweep_methods.py`, `values.yaml`, and `75-boom.yaml`.
