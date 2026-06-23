# Setting Up a Claude-Powered Coding Agent on Huawei Cloud Engine (HCE) with LiteLLM

A complete guide for deploying Claude Code as an AI coding agent on Huawei Cloud Engine (HCE), routed through a LiteLLM proxy to your internal model backend.

> Scope: this guide targets a **standalone external LiteLLM deployment** (example host `7.242.99.159:8888`, serving MiniMax-M2.5). That is different from the **in-cluster LiteLLM** deployed by the `vllm-kv-stack` chart, which is exposed on **NodePort 30400** (container port 4000). For the chart's native Anthropic path, prefer [BooM Gateway Claude Code](../boom/claude-code.md), which serves `/v1/messages` directly.

> 📄 **Original internal documentation:** [link to original doc](https://wiki.huawei.com/domains/12565/wiki/179215/WIKI202510158581388?title=%E7%BD%91%E7%BB%9C%E8%BF%9E%E6%8E%A5%E8%BF%9E%E6%8E%A5%E4%B8%8D%E4%B8%8A)

---

## What Is This Stack?

**Claude Code** is Anthropic's official AI coding assistant that runs in your terminal. It can read your codebase, write and edit files, run commands, and reason about multi-file projects — all through a conversational interface. It natively targets Anthropic's API, but its base URL and model can be redirected to any OpenAI-compatible endpoint.

→ [Claude Code official docs](https://docs.anthropic.com/en/docs/claude-code/overview)  
→ [Claude Code on GitHub](https://github.com/anthropics/claude-code)

**LiteLLM** is an open-source proxy that translates between different LLM provider APIs. It exposes a single OpenAI-compatible `/v1/chat/completions` endpoint and routes requests to any backend model (Anthropic, OpenAI, Azure, MiniMax, local models, etc.). This lets you point Claude Code at any model your team has access to — without changing Claude Code's internal code.

→ [LiteLLM official docs](https://docs.litellm.ai)  
→ [LiteLLM on GitHub](https://github.com/BerriAI/litellm)

---

## Ways to Use Claude Code + LiteLLM

Once this stack is set up, you can use it in several ways:

**1. Terminal AI coding assistant**  
Run `claude` in any project directory. Claude Code reads your files, understands your codebase, and can write code, fix bugs, refactor, and explain logic interactively.  
→ [Claude Code usage guide](https://docs.anthropic.com/en/docs/claude-code/usage)

**2. Automated code review and generation in CI/CD**  
Claude Code can be invoked non-interactively with `claude -p "..."` for scripted tasks — e.g., auto-generating tests, reviewing diffs, or summarizing changes in a pipeline.  
→ [Non-interactive / scripted usage](https://docs.anthropic.com/en/docs/claude-code/cli-reference)

**3. Direct API calls via LiteLLM (Python, curl, any HTTP client)**  
Any code that speaks OpenAI's `/v1/chat/completions` format can call LiteLLM directly. This means your existing Python scripts, Jupyter notebooks, or services can use the same endpoint and model without any SDK changes.  
→ [LiteLLM Python SDK](https://docs.litellm.ai/docs/completion/input)  
→ [OpenAI-compatible usage](https://docs.litellm.ai/docs/proxy/quick_start)

**4. Multi-model routing and load balancing via LiteLLM**  
LiteLLM supports routing across multiple model backends, fallback on failure, rate limit handling, and cost tracking. You can configure it to route heavy requests to a powerful model and light ones to a cheaper model automatically.  
→ [LiteLLM router docs](https://docs.litellm.ai/docs/routing)

**5. Integration with editors and other tools**  
Claude Code's terminal interface can be paired with editors like VS Code (via the terminal panel) or used in tmux/screen sessions for persistent coding sessions.  
→ [Editor integrations](https://docs.anthropic.com/en/docs/claude-code/overview#editors)

---

## Table of Contents

1. [Fix OS Check (Installer Patch)](#1-fix-os-check-installer-patch)
2. [Run Installer](#2-run-installer)
3. [Fix Node.js Version](#3-fix-nodejs-version-critical)
4. [Verify LiteLLM Endpoint](#4-verify-litellm-endpoint)
5. [Fix Claude Config](#5-fix-claude-config-most-important)
6. [Remove Proxy (If Needed)](#6-remove-proxy-if-needed)
7. [Reload Environment](#7-reload-environment)
8. [Run Claude](#8-run-claude)
9. [Python Examples](#9-python-examples)
10. [Debug Checklist](#10-debug-checklist)
11. [Root Causes](#root-causes-encountered)
12. [Key Insight](#key-insight)

---

## 1. Fix OS Check (Installer Patch)

The installer only supports `euleros` and `ubuntu` by default. HCE uses `ID="hce"`, which causes the installer to fail.

**Find this check in the installer:**

```bash
if [[ "$ID" != "euleros" && "$ID" != "ubuntu" ]]; then
```

**Replace with:**

```bash
if [[ "$ID" != "euleros" && "$ID" != "ubuntu" && "$ID" != "hce" ]]; then
```

> Apply this fix everywhere it appears in the installer script.

---

## 2. Run Installer

```bash
claude install
```

**Expected output:**

```
✔ Claude Code successfully installed!
Location: ~/.local/bin/claude
```

---

## 3. Fix Node.js Version (Critical)

Claude Code requires Node.js 18 or higher.

```bash
node -v
```

If the version is below 18, ensure Claude is using a newer Node.js installation before proceeding.

---

## 4. Verify LiteLLM Endpoint

**Working endpoint:** `http://7.242.99.159:8888`  
**Working model:** `MiniMax-M2.5`

Test with curl:

```bash
curl http://7.242.99.159:8888/v1/chat/completions \
  -H "Authorization: Bearer <API_KEY>" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "MiniMax-M2.5",
    "messages": [
      {"role": "user", "content": "Hello"}
    ]
  }'
```

A successful response confirms the LiteLLM endpoint and API key are valid.

---

## 5. Fix Claude Config (Most Important)

Claude Code reads `~/.claude/settings.json` and this file **overrides** shell environment variables (including those set in `~/.bashrc`).

Update `~/.claude/settings.json` to:

```json
{
  "env": {
    "ANTHROPIC_AUTH_TOKEN": "YOUR_API_KEY",
    "ANTHROPIC_BASE_URL": "http://7.242.99.159:8888",
    "ANTHROPIC_DEFAULT_HAIKU_MODEL": "MiniMax-M2.5",
    "ANTHROPIC_DEFAULT_OPUS_MODEL": "MiniMax-M2.5",
    "ANTHROPIC_DEFAULT_SONNET_MODEL": "MiniMax-M2.5",
    "ANTHROPIC_MODEL": "MiniMax-M2.5",
    "ANTHROPIC_REASONING_MODEL": "MiniMax-M2.5"
  }
}
```

> **Important notes:**
> - Use port `8888`, not `8050`
> - Do **not** include `/v1` in `ANTHROPIC_BASE_URL`
> - The model name must exactly match a model available on your LiteLLM instance

---

## 6. Remove Proxy (If Needed)

If proxy settings are interfering with requests:

```bash
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY
```

---

## 7. Reload Environment

```bash
source ~/.bashrc
hash -r
```

---

## 8. Run Claude

```bash
claude
```

If configured correctly:
- No `RTOS-flash` errors
- Requests route successfully to the LiteLLM endpoint

---

## 9. Python Examples

### Simple Request

```python
import requests

url = "http://7.242.99.159:8888/v1/chat/completions"
api_key = "YOUR_API_KEY"

payload = {
    "model": "MiniMax-M2.5",
    "messages": [
        {"role": "user", "content": "Hello"}
    ]
}

headers = {
    "Authorization": f"Bearer {api_key}",
    "Content-Type": "application/json",
}

resp = requests.post(url, headers=headers, json=payload, timeout=60)
print("status:", resp.status_code)
print(resp.text)
```

### Using Environment Variables

```python
import os
import requests

base_url = os.environ.get("LITELLM_BASE_URL", "http://7.242.99.159:8888")
api_key = os.environ["LITELLM_API_KEY"]
model = os.environ.get("LITELLM_MODEL", "MiniMax-M2.5")

url = f"{base_url}/v1/chat/completions"

payload = {
    "model": model,
    "messages": [
        {"role": "user", "content": "Hello"}
    ]
}

headers = {
    "Authorization": f"Bearer {api_key}",
    "Content-Type": "application/json",
}

resp = requests.post(url, headers=headers, json=payload, timeout=60)
resp.raise_for_status()
data = resp.json()
print(data["choices"][0]["message"]["content"])
```

**Set environment variables before running:**

```bash
export LITELLM_BASE_URL="http://7.242.99.159:8888"
export LITELLM_API_KEY="YOUR_API_KEY"
export LITELLM_MODEL="MiniMax-M2.5"
python test_litellm.py
```

---

## 10. Debug Checklist

| Check | Command |
|---|---|
| Shell environment | `env \| grep ANTHROPIC` |
| Claude config | `sed -n '1,200p' ~/.claude/settings.json` |
| Node version | `node -v` |
| LiteLLM connectivity | `curl http://7.242.99.159:8888/v1/chat/completions -H "Authorization: Bearer <API_KEY>" -H "Content-Type: application/json" -d '{"model": "MiniMax-M2.5", "messages": [{"role": "user", "content": "Hello"}]}'` |

---

## Root Causes Encountered

| Symptom | Root Cause |
|---|---|
| Installer fails | OS check did not allow `hce` |
| Wrong model used | `~/.claude/settings.json` overrode shell variables |
| 401 Unauthorized | Claude was pointing to port `8050` instead of `8888` |
| Node.js error | Claude requires Node.js 18 or higher |

---

## Final Working State

- Installer patched to allow `hce`
- `~/.claude/settings.json` points to the correct LiteLLM endpoint
- Model set to `MiniMax-M2.5`
- Valid API key in Claude config
- Node.js 18 or higher installed
- LiteLLM verified with curl and Python

---

## Key Insight

> **Claude Code prioritizes `~/.claude/settings.json` over shell environment variables.**  
> Fixing `~/.bashrc` alone is not enough if `settings.json` already contains an `env` block — that block takes precedence.