---
id: c02-001
date:
campaign: 02-boom-reproduce
series: "2"
exp_ids: []
methods: []
status: migrated
source: logs
---
# Series 2 — check the multi turn feature

Series 2:
Setup:
- 4 replicas (2x data-parallel, TP8 each)
- Model: GLM-5-w4a8-mtp-QuaRot
- Backend: boom (route via router — pull mode)
- Batch size: 8 (helm) / 4 (model spec)
- Sidecar prefetch: 2
- No cap on input tokens
- Request response length: 512
- Total requests: 10
- Pattern: "det"
- Rate_rps: 0.3
- Dataset: lmsys_chat_1m (multi-turn)
- Multi-turn: true
- Streaming: false
- Replay output lengths from: turn 1
- KV-aware routing: true
- Len-aware routing: true (short_first)
- Output length predictor: simple
- Max inflight: 1000

Purpose: check the multi turn feature
Findings: it works
