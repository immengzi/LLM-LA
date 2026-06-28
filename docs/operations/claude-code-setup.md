# Claude Code Setup Record

Date: 2026-06-24  
Host workspace: `/home/haiting/llm-la`  
Requested pinned version: `@anthropic-ai/claude-code@2.1.150`

## Node and npm

Existing Node installation was already sufficient:

```text
node v20.18.2
npm 10.8.2
```

Node is >= 18, so I did not run `yum install -y nodejs`.

## npm Registry Settings

Applied settings:

```bash
npm config set registry https://registry.npmmirror.com
npm config set strict-ssl false
```

Verified current values:

```text
registry = https://registry.npmmirror.com
strict-ssl = false
```

## Claude Code Version

Before pinning, the installed Claude Code was:

```text
2.1.173 (Claude Code)
```

This is newer than the requested maximum, so I pinned it:

```bash
npm install -g @anthropic-ai/claude-code@2.1.150
```

Verified after install:

```text
2.1.150 (Claude Code)
```

Do not run an unpinned global update; message format changes can affect Boom Gateway translation and KV hash alignment.

## Settings File

Created:

```text
/home/haiting/.claude/settings.json
```

The file did not previously exist, so no user settings were overwritten.

Current content uses placeholders for secret/API routing values:

```json
{
  "env": {
    "DISABLE_TELEMETRY": "1",
    "DISABLE_ERROR_REPORTING": "1",
    "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
    "MCP_TIMEOUT": "60000",
    "ANTHROPIC_API_KEY": "<<邮件里的 key，待填>>",
    "ANTHROPIC_AUTH_TOKEN": "<<邮件里的 token，待填>>",
    "ANTHROPIC_BASE_URL": "<<邮件里的 URL，待填>>",
    "ANTHROPIC_MODEL": "MiniMax-M2.7",
    "ANTHROPIC_DEFAULT_HAIKU_MODEL": "MiniMax-M2.7",
    "ANTHROPIC_DEFAULT_SONNET_MODEL": "Qwen3.5-122B-A10B",
    "ANTHROPIC_DEFAULT_OPUS_MODEL": "Qwen3.5-122B-A10B"
  },
  "enabledPlugins": {
    "cc-demo-plugin@rtos-cc-marketplace": true
  },
  "outputStyle": "engineer-professional"
}
```

## Pending User Inputs

The following must be filled from email before running real requests:

- `ANTHROPIC_API_KEY`
- `ANTHROPIC_AUTH_TOKEN`
- `ANTHROPIC_BASE_URL`

`ANTHROPIC_BASE_URL` should ultimately point to the Boom Gateway entrypoint for the full chain:

```text
Claude Code -> Boom Gateway -> LLM-LA router -> KV sidecar -> vLLM
```

For KV-alignment experiments, the old docs also mention `CLAUDE_CODE_ATTRIBUTION_HEADER=0` as a client-side defense against changing `cch=<hex>` attribution counters. The router-side `STRIP_CCH=1` remains the server-side defense.
