---
id: c02-000
date:
campaign: 02-boom-reproduce
series: "1"
exp_ids: []
methods: []
status: migrated
source: logs
---
# Series 1 — Check the multi turn feature

Series 1:
Setup:
- 4 replicas (2x data-parallel, TP8 each)
- Model: GLM-5-w4a8-mtp-QuaRot
- Backend: boom (direct routing, round_robin)
- Batch size: 8 (helm) / 4 (model spec)
- No cap on input tokens
- Request response length: 512
- Total requests: 10
- Pattern: "det"
- Rate_rps: 0.3
- Dataset: lmsys_chat_1m (multi-turn)
- Multi-turn: true
- Streaming: false
- KV-aware routing: true
- Len-aware routing: true (short_first)
- Output length predictor: simple

Purpose: Check the multi turn feature
Findings: it works
