---
id: c02-008
date:
campaign: 02-boom-reproduce
series: "21-24"
exp_ids: [21, 22, 23, 24]
methods: []
status: migrated
source: logs
---
# Series 21-24 — Check streaming

Series 21-24:
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

Purpose: Check streaming
Findings: Seems not to be streaming

Metric / stat            |exp21(pull) |exp22 (rr) |exp23(rand)|exp24 (lq)
-------------------------|------------|-----------|-----------|----------
End-to-end latency (s)   |            |           |           |
  count                  |      9,999 |    10,000 |    10,000 |    10,000
  mean                   |      14.48 |     33.43 |     36.90 |     25.40
  std                    |      27.36 |     42.09 |     64.18 |     35.03
  min                    |      0.143 |     0.131 |     0.137 |     0.138
  p50                    |       6.47 |     18.39 |     14.25 |     13.97
  p90                    |      31.07 |     85.65 |     86.33 |     59.04
  p99                    |     195.33 |    209.37 |    293.45 |    201.29
  max                    |     221.06 |    325.55 |    505.22 |    291.20
-------------------------|------------|-----------|-----------|----------
TTFT (s)                 |            |           |           |
  mean                   |       1.67 |      2.56 |      2.45 |      2.20
  p50                    |       1.51 |      2.48 |      2.31 |      2.15
  p90                    |       2.60 |      4.13 |      3.79 |      3.54
  p99                    |       3.73 |      5.44 |      7.83 |      4.77
  max                    |      32.96 |      5.74 |      9.65 |      6.00
-------------------------|------------|-----------|-----------|----------
TPOT (s)                 |            |           |           |
  mean                   |     0.0230 |    0.0233 |    0.0233 |    0.0231
  p50                    |     0.0232 |    0.0235 |    0.0234 |    0.0232
  p90                    |     0.0239 |    0.0244 |    0.0243 |    0.0243
  p99                    |     0.0243 |    0.0255 |    0.0256 |    0.0254
  max                    |     0.0266 |    0.0260 |    0.0272 |    0.0259
-------------------------|------------|-----------|-----------|----------
Gen tokens/sec           |            |           |           |
  mean                   |    2096.00 |   2130.33 |   2010.69 |   2094.06
  p50                    |    2228.10 |   2302.66 |   2226.07 |   2232.48
  p90                    |    2441.70 |   2451.64 |   2423.66 |   2417.19
-------------------------|------------|-----------|-----------|----------
Router queue length mean |       0.00 |      0.00 |         — |      0.00
Router admission RPS mean|       3.73 |      3.66 |         — |      3.69
Router outgoing RPS mean |       0.55 |      0.46 |      0.44 |      0.46
Sidecar queue length mean|      53.91 |    122.89 |    129.78 |     93.97
Sidecar received RPS mean|       3.73 |      3.66 |      3.52 |      3.69
Sidecar completed RPS mean|      3.73 |      3.66 |      3.51 |      3.69
