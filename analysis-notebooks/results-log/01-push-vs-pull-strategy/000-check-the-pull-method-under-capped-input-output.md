---
id: c01-000
date:
campaign: 01-push-vs-pull-strategy
series: "0"
exp_ids: [1, 2, 3, 4]
methods: []
status: migrated
source: logs
---
# Series 0 — check the pull method under capped input/output token (prefill visible)

Series 0:
Setup:
- 8 servers
- Request response size limit: 2048 tokens
- Input tokens: 256–512
- pattern: "dump"
- Total requests: 200
- Methods:
  1) Pull
  2) Push-RR
  3) Push-Random
  4) Push-Least-Queue

Purpose: check the pull method under capped input/output token (prefill visible)
Findings: With bounded prefill and decode, batching efficiency dominates. Pull consistently wins latency and tail metrics because admission is aligned with GPU availability, while push methods over-admit and inflate queues without throughput benefit.


Observations:
Metric (stat)               | Pull (exp1) | Push-RR (exp2)           | Push-Random (exp3)       | Push-LQ (exp4)           | Winner
----------------------------|-------------|--------------------------|--------------------------|--------------------------|--------
↓ Latency mean (s)          | 57.80*      | 66.02 (+14.2%)           | 69.20 (+19.7%)           | 67.46 (+16.7%)           | Pull
↓ Latency p99 (s)           | 112.46*     | 117.09 (+4.1%)           | 117.07 (+4.1%)           | 113.79 (+1.2%)           | Pull

↑ Decode TPS mean           | 739.23      | 644.20 (−12.9%)          | 633.36 (−14.3%)          | 741.91* (+0.4%)          | Push-LQ
↑ Prefill TPS mean          | 615.16*     | 462.53 (−24.8%)          | 494.12 (−19.7%)          | 552.89 (−10.1%)          | Pull

↑ Requests running mean     | 35.32       | 32.08 (−9.2%)            | 31.12 (−11.9%)           | 36.03* (+2.0%)           | Push-LQ

↓ TTFT mean (s)             | 6.30        | 6.20 (−1.6%)             | 5.33* (−15.4%)           | 6.32 (+0.2%)             | Push-Random
↓ TTFT p99 (s)              | 25.35*      | 35.12 (+38.6%)           | 36.98 (+45.9%)           | 38.35 (+51.3%)           | Pull

↓ TPOT mean (s)             | 0.462       | 0.426* (−8.0%)           | 0.449 (−3.0%)            | 0.480 (+3.8%)            | Push-RR
↓ TPOT p99 (s)              | 1.946*      | 2.129 (+9.4%)            | 2.810 (+44.4%)           | 2.008 (+3.2%)            | Pull



------------------------------------------------
