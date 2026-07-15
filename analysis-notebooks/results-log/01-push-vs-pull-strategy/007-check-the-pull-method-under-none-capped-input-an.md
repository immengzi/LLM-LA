---
id: c01-007
date:
campaign: 01-push-vs-pull-strategy
series: "7"
exp_ids: [29, 30, 31, 32]
methods: []
status: migrated
source: logs
---
# Series 7 — check the pull method under none-capped input and output token (natural prefill and decode scenario) on rps 5

Series 7:
Setup:
- 8 servers
- No cap on input tokens
- Request response length: unlimited
- Total requests: 200
- pattern: "det"
- rate_rps: 5
- Methods:
  29) Pull
  30) Push-RR
  31) Push-Random
  32) Push-Least-Queue

Purpose: check the pull method under none-capped input and output token (natural prefill and decode scenario) on rps 5
Findings: Under heavy decode pressure, push maximizes utilization and TTFT but worsens TPOT stability. Pull sacrifices responsiveness to maintain predictable per-token latency.

Observations:
Metric (stat)               | Pull (exp29) | Push-RR (exp30)          | Push-Random (exp31)      | Push-LQ (exp32)          | Winner
----------------------------|--------------|--------------------------|--------------------------|--------------------------|--------
↓ Latency mean (s)          | 25.62*       | 26.65 (+4.0%)            | 31.85 (+24.3%)           | 26.65 (+4.0%)            | Pull
↓ Latency p99 (s)           | 110.15       | 105.17 (−4.5%)*          | 125.07 (+13.6%)          | 113.35 (+2.9%)           | Push-RR

↑ Decode TPS mean           | 418.68       | 439.78 (+5.0%)           | 437.76 (+4.6%)           | 447.61 (+6.9%)*          | Push-LQ
↑ Prefill TPS mean          | 57.25        | 62.53 (+9.2%)*           | 58.59 (+2.3%)            | 56.38 (−1.5%)            | Push-RR

↑ Requests running mean     | 13.72        | 14.16 (+3.2%)            | 14.31 (+4.3%)*           | 14.19 (+3.5%)            | Push-Random

↓ TTFT mean (s)             | 1.08         | 1.02 (−5.1%)             | 0.95 (−11.9%)            | 0.93 (−13.5%)*           | Push-LQ
↓ TTFT p99 (s)              | 3.72*        | 4.06 (+9.2%)             | 6.16 (+65.3%)            | 3.89 (+4.6%)             | Pull

↓ TPOT mean (s)             | 0.104*       | 0.137 (+31.8%)           | 0.131 (+26.2%)           | 0.133 (+27.8%)           | Pull
↓ TPOT p99 (s)              | 0.396*       | 0.643 (+62.4%)           | 0.742 (+87.6%)           | 0.490 (+23.7%)           | Pull



------------------------------------------------
