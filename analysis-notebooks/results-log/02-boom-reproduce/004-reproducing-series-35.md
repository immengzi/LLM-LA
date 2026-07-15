---
id: c02-004
date:
campaign: 02-boom-reproduce
series: "5-8"
exp_ids: [5, 6, 7, 8]
methods: []
status: migrated
source: logs
---
# Series 5-8 — reproducing series 35 ***

Series 5-8:
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
- Max inflight: 0 (unlimited)
- Key affinity keys: 0
- Mooncake: disabled

Purpose: reproducing series 35 ***
Findings: reproducable with very good results

Metric / stat            |exp5 (pull)| exp6 (rr) |exp7(rand) | exp8 (lq)
-------------------------|-----------|-----------|-----------|----------
End-to-end latency (s)   |           |           |           |
  count                  |     9,999 |    10,000 |    10,000 |     9,999
  mean                   |     15.31 |     27.43 |     37.30 |     28.21
  std                    |     29.16 |     39.84 |     53.90 |     37.29
  min                    |     0.137 |     0.131 |     0.131 |     0.139
  p50                    |      6.93 |     12.60 |     16.00 |     15.62
  p90                    |     31.74 |     71.34 |    118.20 |     69.86
  p99                    |    200.51 |    201.02 |    217.13 |    205.22
  max                    |    248.79 |    326.47 |    393.29 |    301.71
-------------------------|-----------|-----------|-----------|----------
Router queue length      |           |           |           |
  mean                   |      0.22 |      0.00 |         — |      0.00
  p99                    |      7.88 |      0.00 |         — |      0.00
  max                    |     17.00 |      0.00 |         — |      0.00
-------------------------|-----------|-----------|-----------|----------
Router admission RPS     |           |           |           |
  mean                   |      3.71 |      3.66 |         — |      3.71
  p50                    |      4.00 |      4.00 |         — |      4.00
-------------------------|-----------|-----------|-----------|----------
Router outgoing RPS      |           |           |           |
  mean                   |      0.41 |      0.46 |      0.42 |      0.46
-------------------------|-----------|-----------|-----------|----------
Sidecar queue length     |           |           |           |
  mean                   |     56.32 |    100.63 |    131.87 |    104.79
  p50                    |     59.00 |    108.00 |    143.00 |    106.00
  p90                    |     74.00 |    145.00 |    186.00 |    166.20
  p99                    |     80.00 |    180.56 |    233.00 |    206.00
  max                    |     80.00 |    185.00 |    238.00 |    213.00
-------------------------|-----------|-----------|-----------|----------
Sidecar received RPS     |           |           |           |
  mean                   |      3.70 |      3.66 |      3.54 |      3.71
-------------------------|-----------|-----------|-----------|----------
Sidecar completed RPS    |           |           |           |
  mean                   |      3.70 |      3.66 |      3.54 |      3.71
