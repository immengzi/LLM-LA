---
id: c01-014
date:
campaign: 01-push-vs-pull-strategy
series: "14"
exp_ids: [57, 58, 59, 60]
methods: []
status: migrated
source: logs
---
# Series 14 — check the pull method under none-capped input and capped 8192 output token on rps 4, repeat of 13 and 14 on lower load so we don't get requests losts

Series 14:
Setup:
- 8 servers
- No cap on input tokens
- Request response length: 8192
- Total requests: 10000
- pattern: "det"
- rate_rps: 2
- Methods:
  57) Pull
  58) Push-RR
  59) Push-Random
  60) Push-Least-Queue

Purpose: check the pull method under none-capped input and capped 8192 output token on rps 4, repeat of 13 and 14 on lower load so we don't get requests losts
Findings: pull slightly better but since it isn't under saturation point like 13 and 14 which seems to be the place pull excels we cannot have a clear conclusion.

Metric (stat)               | Pull (exp57) | Push-RR (exp58)           | Push-Random (exp59)        | Push-LQ (exp60)           | Winner
----------------------------|--------------|---------------------------|----------------------------|---------------------------|--------
↓ Latency mean (s)          | 21.40*       | 24.27 (+13.4%)            | 28.19 (+31.7%)             | 23.87 (+11.5%)            | Pull
↓ Latency p99 (s)           | 266.66*      | 269.41 (+1.0%)            | 269.72 (+1.1%)             | 269.02 (+0.9%)            | Pull

↑ Decode TPS mean           | 1139.36*     | 1135.62 (−0.3%)           | 1132.29 (−0.6%)            | 1133.33 (−0.5%)           | Pull
↑ Prefill TPS mean          | 161.53       | 161.78 (+0.2%)            | 160.04 (−0.9%)             | 163.45* (+1.2%)           | Push-LQ

↑ Requests running mean     | 37.31        | 37.79 (+1.3%)*            | 37.78 (+1.3%)              | 37.42 (+0.3%)             | Push-RR

↓ TTFT mean (s)             | 0.467*       | 0.523 (+12.2%)            | 0.496 (+6.4%)              | 0.510 (+9.3%)             | Pull
↓ TTFT p99 (s)              | 2.142*       | 2.499 (+16.7%)            | 2.422 (+13.1%)             | 2.478 (+15.7%)            | Pull

↓ TPOT mean (s)             | 0.256        | 0.258 (+0.5%)             | 0.255 (−0.5%)*             | 0.257 (+0.2%)             | Push-Random
↓ TPOT p99 (s)              | 0.504        | 0.514 (+2.0%)             | 0.492 (−2.3%)              | 0.463 (−8.1%)*            | Push-LQ
