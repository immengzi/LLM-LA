# Multi-Turn Conversation Benchmarking

## Overview

The multi-turn feature allows benchmarking LLM inference with realistic
multi-round conversations from the LMSYS dataset. Instead of sending isolated
single-turn prompts, it replays full conversations where each turn builds on
the actual LLM response from the previous turn.

This is useful for:

- Measuring how performance degrades as context length grows across turns
- Testing prefix caching / KV reuse across turns in a conversation
- Producing realistic traffic patterns that match production chat workloads
- Comparing per-endpoint token distribution in multi-turn scenarios

---

## How It Works

```
Conversation 1 (3 turns)        Conversation 2 (2 turns)
─────────────────────────        ────────────────────────
t=0s  [user1] ──→ LLM           t=3s  [user1] ──→ LLM
      [asst1] ←── (real)              [asst1] ←── (real)
      [user1,asst1,user2] ──→         [user1,asst1,user2] ──→
      [asst2] ←── (real)              [asst2] ←── (real)
      [user1,asst1,user2,asst2,user3] ──→
      [asst3] ←── (real)
```

1. Conversations are loaded from the LMSYS dataset (only conversations with
   2+ user turns are selected).
2. Each conversation is scheduled at the configured arrival rate (one schedule
   slot per conversation).
3. Within a conversation, turns execute **sequentially** — turn N+1 waits for
   turn N to complete. The real LLM response is used as the assistant message
   in subsequent turns.
4. Different conversations overlap and run concurrently according to the
   open-loop arrival rate.
5. Each turn is logged as a separate record in `logs.json` with
   `conversation_id`, `turn_idx`, and `num_turns` fields.

---

## Configuration

Add `multi_turn: true` to any `hf-lmsys` config with `backend: boom` or
`backend: litellm`:

```yaml
backend: "boom"
multi_turn: true
prompt_source: "hf-lmsys"
total_requests: 100          # number of conversations (not turns)

hf_lmsys:
  dataset_name: "/path/to/lmsys_chat_1m"
  tokenizer_name: "/path/to/model"
  min_input_tokens: 64
  max_input_tokens: 4096

generation:
  max_tokens: 4096
  temperature: 0.0
  use_dataset_output_len: false    # true = use dataset assistant lengths
  # replay_output_lengths_from: "350"  # uncomment for replay mode

boom:
  base_url: "http://host:30401"
  model: "served-model"
  api_key: "sk-boom-master"
  timeout_s: 10000.0
  stream: false                    # streaming also supported
```

### Key Fields

| Field | Description |
|-------|-------------|
| `multi_turn: true` | Enables multi-turn mode. Default `false` — all existing configs unchanged. |
| `total_requests` | Number of **conversations** to run (not individual turns). |
| `use_dataset_output_len` | When `true`, each turn's `max_tokens`/`min_tokens` is set to the dataset's assistant reply length for that turn. |
| `replay_output_lengths_from` | Experiment ID or path to replay per-turn output lengths from a prior run. |
| `stream: true/false` | Both streaming and non-streaming modes are supported. |

### Filters

The standard `hf_lmsys` token filters apply per-turn:

- `min_input_tokens` / `max_input_tokens` — filter user turns by token length
- `min_output_tokens` / `max_output_tokens` — filter assistant turns by token length

Conversations where any turn fails the filter are excluded.

---

## Output Format

Each turn produces a separate record in `logs.json`:

```json
{
  "idx": "c0t0",
  "conversation_id": 0,
  "turn_idx": 0,
  "num_turns": 3,
  "req_id": "chatcmpl-abc123",
  "prompt": "What is machine learning?",
  "end_to_end_s": 2.451,
  "model_latency_s": 2.300,
  "finish_reason": "stop",
  "endpoint_id": "vllm-glm5-chat-0",
  "prompt_tokens": 42,
  "completion_tokens": 156,
  "total_tokens": 198
}
```

### Fields Added for Multi-Turn

| Field | Type | Description |
|-------|------|-------------|
| `conversation_id` | `int` | Which conversation this turn belongs to |
| `turn_idx` | `int` | 0-based turn index within the conversation |
| `num_turns` | `int` | Total number of user turns in this conversation |
| `idx` | `str` | Composite key `c{conv_id}t{turn_idx}` |

You can group by `conversation_id` for conversation-level metrics, or treat
each turn as an independent request for standard per-request analysis.

---

## Replay Mode

Multi-turn replay works the same as single-turn replay but tracks per-turn
output lengths:

1. **First run** — free generation (no `replay_output_lengths_from`):

```yaml
generation:
  use_dataset_output_len: false
```

2. **Replay run** — read per-turn `completion_tokens` from the first run:

```yaml
generation:
  use_dataset_output_len: true
  replay_output_lengths_from: "350"    # experiment ID
```

The replay loader reads `(conversation_id, turn_idx) → completion_tokens`
from the prior `logs.json` and overrides each turn's output length.

---

## Backward Compatibility

- `multi_turn` defaults to `false` — existing configs work unchanged
- All existing single-turn code paths are untouched
- The `send_one_litellm` / `send_one_litellm_stream` functions accept an
  optional `messages` parameter; when omitted, they behave exactly as before
- Multi-turn only activates when all of: `multi_turn: true`,
  `prompt_source: hf-lmsys`, and `backend: litellm` or `boom`

---

## Files Changed

| File | Change |
|------|--------|
| `prompts.py` | `Conversation`/`ConversationTurn` dataclasses, `build_conversations_from_lmsys()`, `load_replay_conversation_lengths()` |
| `config.py` | `multi_turn: bool = False` on `ClientConfig` |
| `http_client.py` | Optional `messages` parameter on `send_one_litellm` and `send_one_litellm_stream` |
| `load_runner.py` | `ConversationTask` dataclass, `_request_thread_conversation()`, multi-turn dispatch |
| `main.py` | Multi-turn loading, scheduling, and replay wiring |
| `configs/6-1-template-boom-direct-multiturn.yaml` | Sample config |
