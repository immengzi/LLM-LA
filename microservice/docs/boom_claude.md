# Claude Code via BooM Gateway

Route a live Claude Code agent through BooM Gateway to your vLLM cluster.

**Prerequisites:**
- Claude Code installed and working (see [claude-steup.md](claude-steup.md))
- BooM Gateway image built (see [boom_gateway.md](boom_gateway.md))
- vLLM cluster running with router

**Related docs:**
- [claude-steup.md](claude-steup.md) — Claude Code install + LiteLLM path (unchanged)
- [boom_gateway.md](boom_gateway.md) — BooM Gateway build, load testing, Helm config (unchanged)

---

## Architecture

```
Claude Code (terminal)
  │
  │  POST /v1/messages (Anthropic Messages API)
  ▼
BooM Gateway (port 30401)
  │  - authenticates via x-api-key header
  │  - resolves model aliases (claude-sonnet-4-20250514 → served-model)
  │  - converts Anthropic format → OpenAI format
  │  - streams back Anthropic SSE events (message_start, content_block_delta, ...)
  ▼
router /v1/chat/completions (port 30080)
  │
  ▼
sidecars → vLLM pods (Qwen3-8B)
```

BooM Gateway natively supports Anthropic's `/v1/messages` endpoint. Claude
Code speaks this protocol, so it connects directly — no LiteLLM translation
layer needed.

---

## How this differs from the existing paths

| Path | Doc | Client | Endpoint | Format |
|---|---|---|---|---|
| LiteLLM | [claude-steup.md](claude-steup.md) | Claude Code | LiteLLM `:8888` | OpenAI `/v1/chat/completions` |
| BooM load test | [boom_gateway.md](boom_gateway.md) | `main.py` (load runner) | BooM `:30401` | OpenAI `/v1/chat/completions` |
| **BooM + Claude** | **this doc** | **Claude Code** | **BooM `:30401`** | **Anthropic `/v1/messages`** |

The existing BooM load test path (`backend: boom`) and the LiteLLM Claude path
are fully unchanged. This doc adds a new deployment option that combines BooM
Gateway with a live Claude Code agent.

---

## Step 1: Deploy the stack

### Option A: Via sweep (end-to-end)

Uncomment the `boom-claude` entry in `configs/1-master_config.yaml`:

```yaml
boom-claude:
  - pull
```

Then run:

```bash
# Full deploy (vLLM + router + BooM with Claude aliases)
python sweep_methods.py --config 1-master_config

# Or skip vLLM if pods are already running
python sweep_methods.py --config 1-master_config --skip-vllm
```

This uses `configs/boom-claude.yaml` which sets `helm.boom_claude_aliases: true`.
The sweep deploys the stack, runs a small validation pass (8 requests via
`backend=boom` / OpenAI format), then BooM Gateway sits ready for Claude Code.

### Option B: Via Helm directly

```bash
helm upgrade vllm ./vllm-kv-stack \
  --set boom.enabled=true \
  --set boom.masterKey=sk-boom-master \
  --set boom.claudeCodeAliases=true
```

The `claudeCodeAliases` flag adds `router_settings.model_group_alias` entries
so Claude's default model names map to `served-model`:

| Claude model name | Maps to |
|---|---|
| `claude-sonnet-4-20250514` | `served-model` |
| `claude-3-5-sonnet-20241022` | `served-model` |
| `claude-3-haiku-20240307` | `served-model` |
| `claude-3-opus-20240229` | `served-model` |

---

## Step 2: Verify BooM is running

```bash
kubectl get pods -n vllm -l app=boom-proxy
curl http://7.216.57.215:30401/health
```

---

## Step 3: Test the /v1/messages endpoint

```bash
curl http://7.216.57.215:30401/v1/messages \
  -H "x-api-key: sk-boom-master" \
  -H "Content-Type: application/json" \
  -H "anthropic-version: 2023-06-01" \
  -d '{
    "model": "served-model",
    "max_tokens": 64,
    "messages": [
      {"role": "user", "content": "Hello"} 
    ]
  }'
```

A successful response confirms BooM Gateway is accepting Anthropic-format
requests and routing through the router to vLLM.

---

## Step 4: Configure Claude Code

Replace `~/.claude/settings.json` with:

```json
{
  "env": {
    "ANTHROPIC_AUTH_TOKEN": "sk-boom-master",
    "ANTHROPIC_BASE_URL": "http://7.216.57.215:30401",
    "ANTHROPIC_DEFAULT_HAIKU_MODEL": "served-model",
    "ANTHROPIC_DEFAULT_OPUS_MODEL": "served-model",
    "ANTHROPIC_DEFAULT_SONNET_MODEL": "served-model",
    "ANTHROPIC_MODEL": "served-model",
    "ANTHROPIC_REASONING_MODEL": "served-model"
  },
  "permissions": {
    "allow": [
      "Bash(command:*)",
      "Bash(git log:*)",
      "Bash(git status:*)",
      "Bash(git diff:*)",
      "Bash(ls:*)",
      "Bash(mkdir:*)",
      "Bash(grep:*)",
      "Bash(find:*)",
      "Bash(timeout:*)",
      "Bash(curl:*)"
    ],
    "deny": [
      "Bash(dd:*)",
      "Bash(mkfs:*)",
      "Bash(shutdown:*)",
      "Bash(reboot:*)",
      "Bash(sudo:*)",
      "Bash(su:*)",
      "Bash(chown:*)",
      "Bash(vim:*)",
      "Bash(nano:*)",
      "Bash(top:*)",
      "Bash(htop:*)",
      "Bash(less:*)",
      "Bash(man:*)"
    ],
    "ask": [
      "Bash(rm:*)",
      "Bash(git clean:*)",
      "Bash(git reset:*)",
      "Bash(git restore:*)",
      "Bash(git push:*)"
    ]
  },
  "enabledPlugins": {
    "feature-dev@mirror-claude-plugins-official": true,
    "ralph-loop@mirror-claude-plugins-official": true
  },
  "extraKnownMarketplaces": {
    "mirror-claude-plugins-official": {
      "source": {
        "source": "git",
        "url": "https://codehub-dg-y.huawei.com/w00699598/mirror-claude-plugins-official.git"
      }
    }
  },
  "defaultMode": "acceptEdits"
}
```

> Do **not** include `/v1` in `ANTHROPIC_BASE_URL`. Claude Code appends
> `/v1/messages` automatically.

---

## Step 5: Run Claude Code

```bash
claude
```

Or non-interactively:

```bash
claude -p "Explain the main function in main.py"
```

---

## Switching between backends

Only the `env` section of `settings.json` changes. Everything else
(permissions, plugins, marketplace, defaultMode) stays the same.

### Switch to BooM Gateway

```json
"env": {
  "ANTHROPIC_AUTH_TOKEN": "sk-boom-master",
  "ANTHROPIC_BASE_URL": "http://7.216.57.215:30401",
  "ANTHROPIC_DEFAULT_HAIKU_MODEL": "served-model",
  "ANTHROPIC_DEFAULT_OPUS_MODEL": "served-model",
  "ANTHROPIC_DEFAULT_SONNET_MODEL": "served-model",
  "ANTHROPIC_MODEL": "served-model",
  "ANTHROPIC_REASONING_MODEL": "served-model"
}
```

### Switch back to LiteLLM

```json
"env": {
  "ANTHROPIC_AUTH_TOKEN": "YOUR_LITELLM_API_KEY",
  "ANTHROPIC_BASE_URL": "http://7.242.99.159:8888",
  "ANTHROPIC_DEFAULT_HAIKU_MODEL": "MiniMax-M2.5",
  "ANTHROPIC_DEFAULT_OPUS_MODEL": "MiniMax-M2.5",
  "ANTHROPIC_DEFAULT_SONNET_MODEL": "MiniMax-M2.5",
  "ANTHROPIC_MODEL": "MiniMax-M2.5",
  "ANTHROPIC_REASONING_MODEL": "MiniMax-M2.5"
}
```

---

## Configuration reference

### Deployment config (`configs/boom-claude.yaml`)

Same as `configs/boom.yaml` but with `helm.boom_claude_aliases: true` and a
small validation pass (8 requests). The load test uses `backend: boom` (OpenAI
format to BooM) to verify the stack is healthy. Claude Code connects
separately via `/v1/messages`.

### Master configs

The boom-claude entry is included (commented out) in `configs/1-master_config.yaml`.
Uncomment it to include in sweeps:

```yaml
# boom-claude:
#   - pull
```

### Helm values added

| Value | Default | Purpose |
|---|---|---|
| `boom.claudeCodeAliases` | `false` | When `true`, adds model aliases in BooM config |
| `boom.extraModelAliases` | `[]` | Additional custom aliases (list of `{alias, target}`) |

---

## Debug checklist

| Check | Command |
|---|---|
| BooM pod status | `kubectl get pods -n vllm -l app=boom-proxy` |
| BooM health | `curl http://7.216.57.215:30401/health` |
| BooM models list | `curl http://7.216.57.215:30401/v1/models -H "x-api-key: sk-boom-master"` |
| Test /v1/messages | `curl http://7.216.57.215:30401/v1/messages -H "x-api-key: sk-boom-master" -H "Content-Type: application/json" -H "anthropic-version: 2023-06-01" -d '{"model":"served-model","max_tokens":64,"messages":[{"role":"user","content":"Hello"}]}'` |
| Claude config | `cat ~/.claude/settings.json` |
| Claude env vars | `env \| grep ANTHROPIC` |

---

## Common issues

| Symptom | Fix |
|---|---|
| `Connection refused` on 30401 | BooM not deployed — run `helm upgrade` with `boom.enabled=true` |
| `401 Unauthorized` | API key mismatch — `boom.masterKey` in Helm must match `ANTHROPIC_AUTH_TOKEN` in settings.json |
| Model not found | Aliases missing — deploy with `boom.claudeCodeAliases=true` or use `served-model` as model name |
| Streaming errors | Rebuild BooM Gateway from latest source (needs `AnthropicStreamTranscoder`) |
| Slow responses | Expected — responses come from your vLLM backend (Qwen3-8B), not Anthropic's cloud |
| Claude still hitting LiteLLM | `settings.json` env block still has port 8888 — update to 30401 |

---

## Files added/modified for this integration

| File | Change |
|---|---|
| `configs/boom-claude.yaml` | Deployment config (`backend: boom` + `boom_claude_aliases: true`) |
| `configs/1-master_config.yaml` | Boom + boom-claude entries (boom active, boom-claude commented) |
| `config.py` | `helm.boom_claude_aliases` field on `HelmConfig` |
| `sweep_methods.py` | Passes `boom.claudeCodeAliases` to Helm when `backend=boom` |
| `vllm-kv-stack/templates/75-boom.yaml` | Conditional `router_settings.model_group_alias` block |
| `vllm-kv-stack/values.yaml` | `boom.claudeCodeAliases` and `boom.extraModelAliases` values |
| `docs/boom_claude.md` | This document |
| `docs/boom_claude_changelog.md` | Detailed changelog with per-file diffs |
| `apply_boom_claude_patch.sh` | Patch script (reads from `boom_claude_patch.tar.gz`) |
| `boom_claude_patch.tar.gz` | All files bundled for transfer to another repo |

For detailed per-file diffs and rollback instructions, see
[boom_claude_changelog.md](boom_claude_changelog.md).

## Applying to another repo

```bash
./apply_boom_claude_patch.sh ~/microservice ~/external-microservice
```

The target repo must already have the base BooM patch applied. The script
copies files from source to target, backs up overwritten files, and prints
next steps.

A `boom_claude_patch.tar.gz` is also provided for transporting the source
files to another machine.

See [boom_claude_changelog.md](boom_claude_changelog.md) for per-file diffs
and rollback instructions.
