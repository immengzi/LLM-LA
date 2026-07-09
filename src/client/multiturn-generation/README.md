# Multiturn Generation Templates

This folder contains template files used by the LLM-LA load generator to inject
a realistic **Claude Code CLI prefix** into multiturn requests sent to the
BooM / LiteLLM gateway via the OpenAI `/v1/chat/completions` wire path.

The goal is to simulate the token-heavy system preamble that a real Claude Code
session carries (system blocks, system reminders, 23 tool definitions) so that
prefix-caching and KV-aware routing can be tested under realistic conditions.

---

## Files

### `system_blocks.json`

A JSON array of Anthropic-format content blocks representing the **system
prompt** of a Claude Code session.

Each element is `{"type": "text", "text": "...", "cache_control": {...}}`.
The first block contains the billing/version header
(`x-anthropic-billing-header: cc_version=X.Y.Z.XXX; ...`); subsequent blocks
contain the full Claude Code system instructions (tool usage guidelines, coding
rules, environment description, etc.).

At injection time, all block texts are joined with `"\n"` and emitted as a
single `{"role": "system", "content": "<joined>"}` message prepended to the
OpenAI messages array.

### `system_reminders.json`

A JSON array of Anthropic-format content blocks representing **system
reminders** (context injected inside the first user turn by the Claude Code
harness).

Contains available-skill listings and `claudeMd` (CLAUDE.md project
instructions). At injection time, all block texts are joined with `"\n"` and
**prepended to the content of the first `role: "user"` message** in the request.

### `tools.json`

A JSON array of **Anthropic-format tool definitions**. Each element has the
shape:

```json
{
  "name": "ToolName",
  "description": "...",
  "input_schema": { "$schema": "...", "type": "object", "properties": {...}, ... }
}
```

Contains 23 tools (Agent, AskUserQuestion, Bash, CronCreate, CronDelete,
CronList, Edit, EnterPlanMode, EnterWorktree, ExitPlanMode, ExitWorktree, Glob,
Grep, NotebookEdit, Read, ScheduleWakeup, Skill, TaskOutput, TaskStop,
TodoWrite, WebFetch, WebSearch, Write).

At injection time, each tool is mechanically converted to OpenAI function-call
format:

```json
{"type": "function", "function": {"name": "...", "description": "...", "parameters": <input_schema>}}
```

and written into the request body's top-level `"tools"` field.

### `request_body_template.json`

A **reference sample** of a full Anthropic `/v1/messages` request body captured
from a live Claude Code session. This file is **not loaded by code** at runtime;
it exists purely for documentation/debugging purposes so developers can see the
complete wire-format context that the injection system is trying to reproduce.

Top-level structure:

| Field | Description |
|-------|-------------|
| `model` | The model ID used in the original capture (e.g. `claude-sonnet-4-20250514`) |
| `messages` | A multi-turn conversation (user/assistant/tool_result turns) |
| `tools` | The full 23-tool array (Anthropic format) |
| `metadata` | Session metadata (device_id, session_id — redacted) |
| `max_tokens` | Token budget for the response |
| `thinking` | Extended-thinking configuration |
| `context_management` | Context-window management directives |
| `stream` | Always `true` |

The `messages` array demonstrates how Claude Code structures a real request:
the first user turn contains system-reminders as content blocks, followed by
the actual user prompt; subsequent turns show tool_use / tool_result exchanges.

---

## How It's Used

### Configuration

The injection is controlled by the `claude_code_injection` section in an
experiment config YAML (e.g.
`microservice/configs/15-2-template-boom-claude-glm-stability-system-prompts.yaml`):

```yaml
claude_code_injection:
  enabled: true
  template_dir: "/home/haiting/llm-la/src/client/multiturn-generation"
  inject_system_blocks: true
  inject_system_reminders: true
  inject_tools: true
  preserve_cache_control: true  # no-op on OpenAI path
```

This dict is loaded as-is by `config.py` (`load_config`, field
`claude_code_injection` at line 651/703) and passed through `main.py` (line 342)
into the load runner.

### Call Path

1. **`microservice/main.py`** passes `claude_code_injection=getattr(cfg, "claude_code_injection", None)` to `run_open_loop_load`.

2. **`microservice/load_runner.py`** (`run_open_loop_load`, line 1620) accepts
   `claude_code_injection: Optional[dict]` and forwards it as `cc_cfg` to each
   conversation worker thread (`_request_thread_conversation`, line 2022).

3. **`microservice/load_runner.py`** (`_request_thread_conversation`, line 984)
   passes `cc_cfg=cc_cfg, turn_idx=turn_idx` to the HTTP send function.

4. **`microservice/http_client.py`** (`send_one_litellm` line 757,
   `send_one_litellm_stream` line 561) calls:
   ```python
   payload = _maybe_inject_claude_code_template(payload, cc_cfg, turn_idx)
   ```

5. **`_maybe_inject_claude_code_template`** (http_client.py line 85) performs
   the injection:
   - Calls `_load_cc_templates(template_dir)` which reads the three JSON files
     from this folder **once** and caches them in `_CC_TEMPLATE_CACHE` (line 47).
   - Deep-copies the messages array to avoid mutating the shared template cache.
   - Conditionally applies each injection based on the `inject_*` flags.
   - Always strips `cache_control` keys from the final payload (OpenAI/vLLM
     does not use them).

### What Gets Injected Per Request

Given `inject_system_blocks`, `inject_system_reminders`, and `inject_tools` all
set to `true`, each outgoing request body is augmented as:

```
messages = [
  {"role": "system", "content": "<all system_blocks joined>"},  ← from system_blocks.json
  {"role": "user", "content": "<reminders>\n<original user content>"},  ← reminders prepended
  ... rest of conversation history ...
]
tools = [{"type": "function", "function": {...}}, ...]  ← from tools.json
```

### Notable Behavior

- Injection happens on **every turn** of a multiturn conversation, not just the
  first. The `turn_idx` parameter is accepted but not used as a gate. This means
  the system prefix is byte-identical across turns, which is favorable for prefix
  caching.
- The `cch` and `cc_version` values in `system_blocks.json` are left as
  placeholder strings (`XXXXX`, `X.Y.Z.XXX`). Per-request drift is handled
  router-side by the `STRIP_CCH` mechanism.
- When `claude_code_injection.enabled` is `false` or the section is absent, all
  injection code is a strict no-op — existing behavior is bit-for-bit unchanged.
