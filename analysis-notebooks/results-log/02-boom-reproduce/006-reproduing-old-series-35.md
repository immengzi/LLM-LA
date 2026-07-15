---
id: c02-006
date:
campaign: 02-boom-reproduce
series: "13-16"
exp_ids: [13, 14, 15, 16]
methods: []
status: migrated
source: logs
---
# Series 13-16 — reproduing old series 35

Series 13-16:
Setup:
- 8 replicas, Qwen3-8B (dense, TP1 each)
- Backend: router (pull mode)
- Batch size: 8 (helm) / 8 (model spec)
- Sidecar prefetch: 2
- No cap on input tokens (min 256)
- Request response length: 8192 (use_dataset_output_len: false)
- Total requests: 25000
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

Purpose: reproduing old series 35
Findings: Good Results ***

Metric / stat            |exp13(pull) |exp14 (rr) |exp15(rand)|exp16 (lq)
-------------------------|------------|-----------|-----------|----------
End-to-end latency (s)   |            |           |           |
  count                  |     25,000 |    24,999 |    24,999 |    24,999
  mean                   |      14.82 |     23.66 |     31.65 |     26.22
  std                    |      27.98 |     33.68 |     43.71 |     37.89
  min                    |      0.137 |     0.133 |     0.032 |     0.129
  p50                    |       6.70 |     12.48 |     15.41 |     12.34
  p90                    |      31.42 |     56.29 |     82.69 |     68.10
  p99                    |     197.74 |    198.73 |    205.06 |    198.92
  max                    |     233.42 |    294.03 |    369.82 |    354.09
-------------------------|------------|-----------|-----------|----------
TTFT (s)                 |            |           |           |
  mean                   |       1.82 |      2.10 |      2.27 |      2.06
  p50                    |       1.57 |      2.00 |      2.14 |      1.89
  p90                    |       3.28 |      3.25 |      3.45 |      3.35
  p99                    |       5.22 |      7.46 |      5.15 |      6.55
  max                    |       9.23 |     10.23 |     11.05 |      8.95
-------------------------|------------|-----------|-----------|----------
TPOT (s)                 |            |           |           |
  mean                   |     0.0233 |    0.0231 |    0.0232 |    0.0230
  p50                    |     0.0233 |    0.0231 |    0.0232 |    0.0231
  p90                    |     0.0243 |    0.0241 |    0.0242 |    0.0241
  p99                    |     0.0252 |    0.0255 |    0.0251 |    0.0252
  max                    |     0.0280 |    0.0268 |    0.0259 |    0.0279
-------------------------|------------|-----------|-----------|----------
Gen tokens/sec           |            |           |           |
  mean                   |    2199.04 |   2164.72 |   2143.83 |   2168.21
  p50                    |    2243.36 |   2230.86 |   2226.97 |   2238.69
  p90                    |    2509.58 |   2415.72 |   2416.81 |   2400.82
-------------------------|------------|-----------|-----------|----------
Router queue length mean |       0.14 |      0.00 |         — |      0.00
Router admission RPS mean|       3.89 |      3.86 |         — |      3.88
Router outgoing RPS mean |       0.52 |      0.48 |      0.48 |      0.48
Sidecar queue length mean|      57.46 |     91.19 |    120.63 |    101.36
Sidecar received RPS mean|       3.89 |      3.86 |      3.82 |      3.88
Sidecar completed RPS mean|      3.90 |      3.86 |      3.82 |      3.88
