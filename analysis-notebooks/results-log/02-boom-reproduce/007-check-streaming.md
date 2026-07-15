---
id: c02-007
date:
campaign: 02-boom-reproduce
series: "17-20"
exp_ids: [17, 18, 19, 20]
methods: []
status: migrated
source: logs
---
# Series 17-20 — Check streaming

Series 17-20:
Setup:
- 8 replicas, Qwen3-8B (dense, TP1 each)
- Backend: router (pull mode)
- Batch size: 8 (helm) / 8 (model spec)
- Sidecar prefetch: 2
- No cap on input tokens (min 256)
- Request response length: 8192 (use_dataset_output_len: false)
- Total requests: 10000
- Pattern: "det"
- Rate_rps: 5
- Dataset: lmsys_chat_1m (single-turn)
- Multi-turn: false
- Streaming: false
- KV-aware routing: false
- Len-aware routing: true (short_first)
- Output length predictor: simple
- Max inflight: unlimited
- Key affinity keys: 0
- Mooncake: disabled

Purpose: Check streaming
Findings: Seems not stremaing

Metric / stat            |exp17(pull) |exp18 (rr) |exp19(rand)|exp20 (lq)
-------------------------|------------|-----------|-----------|----------
End-to-end latency (s)   |            |           |           |
  count                  |     10,000 |     9,999 |     9,998 |     9,999
  mean                   |      85.35 |    111.88 |    142.38 |    128.39
  std                    |      60.09 |     86.08 |    123.07 |     91.58
  min                    |      0.145 |     0.144 |     0.143 |     0.144
  p50                    |      71.82 |     90.10 |    109.71 |    118.72
  p90                    |     149.15 |    233.49 |    315.32 |    282.51
  p99                    |     260.18 |    311.62 |    523.82 |    349.36
  max                    |   1926.88  |    482.93 |    706.32 |    523.04
-------------------------|------------|-----------|-----------|----------
TTFT (s)                 |            |           |           |
  mean                   |       3.96 |      3.73 |      3.96 |      3.90
  p50                    |       4.03 |      3.86 |      4.02 |      3.96
  p90                    |       5.21 |      4.80 |      5.56 |      5.10
  p99                    |       6.32 |      5.82 |      7.38 |      6.70
  max                    |       8.44 |      6.39 |      9.39 |      8.20
-------------------------|------------|-----------|-----------|----------
TPOT (s)                 |            |           |           |
  mean                   |     0.0238 |    0.0238 |    0.0240 |    0.0239
  p50                    |     0.0240 |    0.0240 |    0.0242 |    0.0242
  p90                    |     0.0249 |    0.0249 |    0.0253 |    0.0250
  p99                    |     0.0253 |    0.0255 |    0.0271 |    0.0256
  max                    |     0.0255 |    0.0259 |    0.0281 |    0.0272
-------------------------|------------|-----------|-----------|----------
Gen tokens/sec           |            |           |           |
  mean                   |    2456.08 |   2348.95 |   2162.59 |   2322.37
  p50                    |    2624.62 |   2568.33 |   2526.66 |   2556.71
  p90                    |    2719.89 |   2656.21 |   2657.30 |   2655.74
-------------------------|------------|-----------|-----------|----------
Router queue length mean |     296.45 |      0.00 |         — |      0.00
Router admission RPS mean|       4.33 |      4.12 |         — |      4.04
Router outgoing RPS mean |       0.60 |      0.52 |      0.49 |      0.50
Sidecar queue length mean|      73.44 |    461.39 |    529.65 |    517.90
Sidecar received RPS mean|       4.33 |      4.12 |      3.72 |      4.03
Sidecar completed RPS mean|      4.33 |      4.12 |      3.72 |      4.03
