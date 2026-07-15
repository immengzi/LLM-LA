---
id: c02-002
date:
campaign: 02-boom-reproduce
series: "3"
exp_ids: []
methods: []
status: migrated
source: logs
---
# Series 3 — stability test with single turn

Series 3:
Setup:
- 4 replicas (2x data-parallel, TP8 each), model replicas: 4
- Model: GLM-5-w4a8-mtp-QuaRot
- Backend: boom (route via router — pull mode)
- Batch size: 8 (helm) / 4 (model spec)
- Sidecar prefetch: 2
- No cap on input tokens
- Request response length: 8192 (use_dataset_output_len: true)
- Total requests: 40000
- Pattern: "det"
- Rate_rps: 0.2
- Dataset: lmsys_chat_1m (single-turn)
- Multi-turn: false
- Streaming: false
- KV-aware routing: true
- Len-aware routing: true (short_first)
- Output length predictor: simple
- Max inflight: 1000
- Key affinity keys: 0

Purpose: stability test with single turn
Findings: stable
