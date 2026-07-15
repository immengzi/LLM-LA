---
id: c01-029
date:
campaign: 01-push-vs-pull-strategy
series: "29"
exp_ids: [117, 118, 119, 120]
methods: []
status: migrated
source: logs
---
# Series 29 — 200 on rps 6 for debugging the new experriments

Series 29:
Setup:
- 8 servers
- No cap on input tokens
- Request response length: 8192
- Total requests: 10000
- pattern: "det"
- rate_rps: 6
- Methods:
  117) Pull
  118) Push-RR
  119) Push-Random
  120) Push-Least-Queue

Purpose: 200 on rps 6 for debugging the new experriments
Findings: Not important

Metric (mean / p99)            | Pull (exp117) | Push-RR (exp118)        | Push-Random (exp119)     | Push-LQ (exp120)         | Winner
-------------------------------|---------------|-------------------------|--------------------------|--------------------------|--------
↓ End-to-end latency mean (s)  | 19.98*        | 21.92 (+9.7%)           | 25.63 (+28.3%)           | 22.02 (+10.2%)           | Pull
↓ End-to-end latency p99 (s)   | 93.82*        | 100.24 (+6.8%)          | 116.79 (+24.5%)          | 99.89 (+6.5%)            | Pull

↑ Decode TPS mean              | 664.65        | 681.07 (+2.5%)*         | 679.67 (+2.3%)           | 678.79 (+2.1%)           | Push-RR
↑ Prefill TPS mean             | 105.92*       | 105.54 (−0.4%)          | 105.58 (−0.3%)           | 105.00 (−0.9%)           | Pull

↑ Requests running mean        | 21.67         | 22.10 (+2.0%)           | 21.99 (+1.5%)            | 22.15 (+2.2%)*           | Push-LQ

↓ TTFT mean (s)                | 3.41          | 2.21 (−35.2%)           | 2.03 (−40.4%)*           | 2.27 (−33.4%)            | Push-Random
↓ TPOT mean (s)                | 0.207         | 0.204 (−1.5%)           | 0.208 (+0.5%)            | 0.204 (−1.2%)*           | Push-LQ
