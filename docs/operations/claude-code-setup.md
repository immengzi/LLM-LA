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

## Router-side `STRIP_CCH`: whole-block fix (`remove-cc-header` build)

### The gap

Claude Code prepends a standalone **system** text block that begins with:

```text
x-anthropic-billing-header: cc_version=<ver>; cch=<random>; cc_entrypoint=<...>;
```

Both the rotating `cc_version` tail **and** the per-request `cch=<hex>` counter change
on every request. The original `STRIP_CCH` implementation only matched the narrow
fragment `; cch=<hex>` (`_CCH_RE = re.compile(r"; cch=[0-9a-f]+")`), so it left the
rotating `cc_version` behind. The first KV block therefore differed on every
request and byte-exact prefix matching missed **100%** of the time — `STRIP_CCH=1`
never actually restored cross-request cache hits.

### The fix

`router/api.py::_strip_cch_inplace` now drops the **entire** attribution block, not
just the `cch=` fragment:

- **List-form** system content: whole text parts whose text starts with
  `x-anthropic-billing-header` are removed (`_is_attribution_text`).
- **String-form** system content: the whole `x-anthropic-billing-header: …` line is
  removed (`_ATTRIBUTION_LINE_RE`).
- The legacy `_CCH_RE` is kept only as a residual-fragment scrub.

This mirrors BooM Gateway's `rewrite::strip_cc_attribution_anthropic` and vLLM
PR #36829. Still gated by `STRIP_CCH=1`; BooM Gateway is not modified.

### Build & rollout

- Image: `reg.local:32000/kv-router:remove-cc-header` (digest `e407aab5…f0a5b`).
- Adopted in `src/client/configs/prod-bz-boom-minmax-lmcache-p2p-hoststaging-affinity-redis.yaml`
  via `helm.values.images.router`.

### Validation (2026-07-07)

- **Strip effective**: block 0 detokenizes to the real system prompt with no
  `x-anthropic-billing-header` present.
- **Cross-conversation alignment restored**: different CC windows now compute
  identical block hashes for the shared system+tools prefix (LCP ≈ 174–184 blocks),
  which the old narrow regex never achieved.
- **Actual reuse confirmed**: vLLM `prefix_cache_hits_total / queries_total`
  ≈ **80%** (GPU tier) on both `vllm-minimax-m2-0` and `-1`.

### Caveat — affinity routing vs. cross-conversation co-location

The current deployment uses `router_strategy: affinity` (HARD mode). Affinity keys
on `sha256(model + system + first user message)`, i.e. per-conversation identity,
not on the shared prefix — so two **different** conversations that share the same
system prompt are pinned independently and are **not** co-located, even though their
prefix hashes now align. Cross-conversation prefix sharing requires
`router_strategy: prefix` or `both` (prefix routing is inert under affinity).

Note also that under affinity mode `KV_AWARE=False`, so the router's per-request
block-owner lookup is skipped and `matched_tokens` / `kv_hit` in `/latency_log` are
**always 0** — a measurement blind spot, not evidence of a cache miss. Use the vLLM
`prefix_cache_*` metrics to observe real reuse.
