---
id: c02-003
date:
campaign: 02-boom-reproduce
series: "4"
exp_ids: []
methods: []
status: migrated
source: logs
---
# Series 4 — Stability test

Series 4:
Setup:
- 4 replicas (2x data-parallel, TP8 each), model replicas: 4
- Model: GLM-5-w4a8-mtp-QuaRot
- Backend: boom (route via router — pull mode)
- Batch size: 8 (helm) / 4 (model spec)
- Sidecar prefetch: 2
- No cap on input tokens (max_input_tokens 3000 via dataset)
- Request response length: use_dataset_output_len: true (max_tokens 1024)
- Total requests: 2000
- Pattern: "det"
- Rate_rps: 0.1
- Dataset: CodeFlowBench-2505 (single-turn)
- Multi-turn: false
- Streaming: false
- KV-aware routing: true
- Len-aware routing: true (short_first)
- Output length predictor: simple
- Max inflight: 1000
- Key affinity keys: 0
- Mooncake: disabled

Purpose: Stability test
Findings: it is stable
