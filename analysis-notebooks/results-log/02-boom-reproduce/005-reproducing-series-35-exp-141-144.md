---
id: c02-005
date:
campaign: 02-boom-reproduce
series: "9-12"
exp_ids: [9, 10, 11, 12]
methods: []
status: migrated
source: logs
---
# Series 9-12 — reproducing series 35 (exp 141-144)

Series 9-12:
Setup:
- 8 replicas, Qwen3-8B (dense, TP1 each)
- Backend: router (pull mode)
- Batch size: 8 (helm) / 8 (model spec)
- Sidecar prefetch: 2
- No cap on input tokens (min 256)
- Request response length: 8192 (use_dataset_output_len: false)
- Total requests: 10000
- Pattern: "det"
- Rate_rps: 4
- Dataset: lmsys_chat_1m (single-turn)
- Multi-turn: false
- Streaming: false
- KV-aware routing: false
- Len-aware routing: true (short_first)
- Output length predictor: simple
- Max inflight: unlimited
- Key affinity keys: 0
- Mooncake: disabled

Purpose: reproducing series 35 (exp 141-144)
Findings: Good imporvement but not impressevie

Metric / stat            | exp9 (pull)|exp10 (rr) |exp11(rand)|exp12 (lq)
-------------------------|------------|-----------|-----------|----------
End-to-end latency (s)   |            |           |           |
  count                  |     10,000 |    10,000 |    10,000 |    10,000
  mean                   |     105.33 |    118.84 |    112.74 |    118.90
  std                    |      71.51 |     84.00 |    101.97 |     85.27
  min                    |      0.143 |     0.142 |     0.138 |     0.146
  p50                    |      97.54 |    109.57 |     86.84 |    103.11
  p90                    |     186.21 |    244.79 |    267.91 |    238.53
  p99                    |     293.46 |    326.42 |    379.47 |    301.03
  max                    |   1962.55  |    486.30 |    559.72 |    469.21
-------------------------|------------|-----------|-----------|----------
TTFT (s)                 |            |           |           |
  mean                   |       4.23 |      3.93 |      3.63 |      3.81
  p50                    |       4.13 |      3.96 |      3.58 |      3.92
  p90                    |       5.60 |      5.28 |      4.69 |      5.12
  p99                    |       7.61 |      7.50 |      7.32 |      6.77
  max                    |      17.27 |      8.52 |     12.41 |      7.82
-------------------------|------------|-----------|-----------|----------
TPOT (s)                 |            |           |           |
  mean                   |     0.0240 |    0.0238 |    0.0237 |    0.0237
  p50                    |     0.0242 |    0.0240 |    0.0239 |    0.0239
  p90                    |     0.0250 |    0.0249 |    0.0248 |    0.0248
  p99                    |     0.0254 |    0.0253 |    0.0257 |    0.0258
  max                    |     0.0270 |    0.0269 |    0.0267 |    0.0319
-------------------------|------------|-----------|-----------|----------
Gen tokens/sec           |            |           |           |
  mean                   |    2448.19 |   2349.51 |   2289.51 |   2372.56
  p50                    |    2600.52 |   2575.31 |   2537.66 |   2564.88
  p90                    |    2698.50 |   2672.36 |   2634.52 |   2680.51
-------------------------|------------|-----------|-----------|----------
Router queue length mean |     374.11 |      0.00 |         — |      0.00
Router admission RPS mean|       4.25 |      4.09 |         — |      4.18
Router outgoing RPS mean |       0.58 |      0.51 |      0.51 |      0.52
Sidecar queue length mean|      73.55 |    486.16 |    459.15 |    496.42
Sidecar received RPS mean|       4.25 |      4.09 |      4.07 |      4.18
Sidecar completed RPS mean|      4.25 |      4.09 |      4.07 |      4.17
