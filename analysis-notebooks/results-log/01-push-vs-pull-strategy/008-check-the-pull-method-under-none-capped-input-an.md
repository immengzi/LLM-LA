---
id: c01-008
date:
campaign: 01-push-vs-pull-strategy
series: "8"
exp_ids: [33, 34, 35, 36]
methods: []
status: migrated
source: logs
---
# Series 8 — check the pull method under none-capped input and output token (natural prefill and decode scenario) on rps 6

Series 8:
Setup:
- 8 servers
- No cap on input tokens
- Request response length: unlimited
- Total requests: 200
- pattern: "det"
- rate_rps: 6
- Methods:
  33) Pull
  34) Push-RR
  35) Push-Random
  36) Push-Least-Queue

Purpose: check the pull method under none-capped input and output token (natural prefill and decode scenario) on rps 6
Findings: At extreme load, push optimizes throughput and responsiveness but amplifies tail risk. Pull remains the safer policy for latency-critical workloads.

Observations:
Metric (stat)               | Pull (exp33) | Push-RR (exp34)          | Push-Random (exp35)      | Push-LQ (exp36)          | Winner
----------------------------|--------------|--------------------------|--------------------------|--------------------------|--------
↓ Latency mean (s)          | 26.93*       | 28.40 (+5.4%)            | 33.09 (+22.9%)           | 38.91 (+44.5%)           | Pull
↓ Latency p99 (s)           | 107.03       | 105.47 (−1.5%)*          | 119.70 (+11.8%)          | 123.91 (+15.8%)          | Push-RR

↑ Decode TPS mean           | 444.00       | 443.04 (−0.2%)           | 452.73 (+2.0%)*          | 443.46 (−0.1%)           | Push-Random
↑ Prefill TPS mean          | 60.83        | 66.72 (+9.7%)*           | 61.88 (+1.7%)            | 63.60 (+4.6%)            | Push-RR

↑ Requests running mean     | 14.44        | 14.60 (+1.1%)            | 14.80 (+2.5%)*           | 14.24 (−1.4%)            | Push-Random

↓ TTFT mean (s)             | 1.25         | 0.98 (−21.4%)            | 0.81 (−35.4%)            | 0.80 (−35.8%)*           | Push-LQ
↓ TTFT p99 (s)              | 4.46         | 3.97 (−11.0%)            | 3.00 (−32.8%)*           | 3.31 (−25.8%)            | Push-Random

↓ TPOT mean (s)             | 0.128        | 0.128 (+0.5%)            | 0.141 (+10.2%)           | 0.125 (−2.1%)*           | Push-LQ
↓ TPOT p99 (s)              | 0.455*       | 0.511 (+12.3%)           | 0.574 (+26.0%)           | 0.443 (−2.7%)            | Pull


------------------------------------------------
