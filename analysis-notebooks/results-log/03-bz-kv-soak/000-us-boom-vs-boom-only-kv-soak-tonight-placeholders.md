---
id: c03-000
date: 2026-07-15
campaign: 03-bz-kv-soak
series: 1-2
exp_ids: []
methods: [pull, key_affinity]
status: draft
---

# Series 1-2 — us-boom vs boom-only KV soak (tonight placeholders)

## Notebook
[`prefix-kv-drop-root-cause.ipynb`](../../prefix-kv-drop-root-cause.ipynb) — section **BZ tonight KV soak** (fill exp ids after runs).

## Setup
- servers: bz (this cluster)
- model: MiniMax-M2.7-w8a8-QuaRot / LMCache P2P host-staging
- backend: boom
- dataset: hf-lmsys uncapped input, max_tokens 8192, ~50k @ ~1 rps
- total_requests: 50000
- rate_rps: 1.0
- methods:
  - Series 1: `benchmark-bz-us-boom-lmsys-kv-soak` → `pull` (BooM → LLM-LA router, `router_strategy=both`, hard affinity 800s)
  - Series 2: `benchmark-bz-boom-only-lmsys-kv-soak` → `key_affinity` (BooM → vLLM direct, sk-bench rotation)

## Purpose
Reproduce YZ-style GPU-KV fill / prefix collapse / pin timeouts on BZ and compare us+BooM (33/218 path) vs Boom-only (142 path) metrics.

## Findings
(pending run)

## Observations

| Signal | Series 1 (us+boom) | Series 2 (boom-only) | Notes |
|---|---|---|---|
| exp_id | TBD | TBD | fill after sweep |
| GPU KV max |  |  | aim ~97–100% |
| Pin timeouts |  |  | vllm.log "Pin timeout … Forcing unpin" |
| prefix_cache_hit_rate |  |  | collapse under saturation? |
| decode tok/s |  |  | decays while running≈batchSize? |
| running / waiting |  |  | |
| TTFT / TPOT / e2e |  |  | stacked overview panels |

## Success criteria (from configs)
- GPU KV → ~97–100% and stays there
- Pin timeouts in vllm.log
- Decode tok/s decays while `num_requests_running` ≈ batchSize
- Prefix cache hit rate collapses
