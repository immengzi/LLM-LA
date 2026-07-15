---
id: c01-003
date:
campaign: 01-push-vs-pull-strategy
series: "3"
exp_ids: [13, 14, 15, 16]
methods: []
status: migrated
source: logs
---
# Series 3 — check the pull method under none-capped input and output token (natural prefill and decode scenario) on rps 1

Series 3:
Setup:
- 8 servers
- No cap on input tokens
- Request response length: unlimited
- Total requests: 10000
- pattern: "det"
- rate_rps: 1
- Methods:
  13) Pull
  14) Push-RR
  15) Push-Random
  16) Push-Least-Queue

Purpose: check the pull method under none-capped input and output token (natural prefill and decode scenario) on rps 1
Findings: At very low load, scheduling policy is irrelevant. GPUs are idle, queues are empty, and all methods converge, confirming no structural advantage in the underutilized regime.

Observations:
Metric (stat)               | Pull (exp13) | Push-RR (exp14)          | Push-Random (exp15)      | Push-LQ (exp16)          | Winner
----------------------------|--------------|--------------------------|--------------------------|--------------------------|--------
↓ Latency mean (s)          | 18.07*       | 18.18 (+0.6%)            | 18.18 (+0.6%)            | 18.16 (+0.5%)            | Pull
↓ Latency p99 (s)           | 247.33       | 247.43 (+0.0%)           | 246.83* (−0.2%)          | 248.31 (+0.4%)           | Push-Random

↑ Decode TPS mean           | 581.30       | 581.39* (+0.0%)          | 578.40 (−0.5%)           | 580.92 (−0.1%)           | Push-RR
↑ Prefill TPS mean          | 82.02        | 81.94 (−0.1%)            | 83.37* (+1.6%)           | 82.75 (+0.9%)            | Push-Random

↑ Requests running mean     | 17.53*       | 17.52 (−0.1%)            | 17.46 (−0.4%)            | 17.46 (−0.4%)            | Pull

↓ TTFT mean (s)             | 0.263*       | 0.266 (+1.2%)            | 0.273 (+4.1%)            | 0.289 (+9.9%)            | Pull
↓ TTFT p99 (s)              | 1.413        | 1.564 (+10.7%)           | 1.507 (+6.7%)            | 1.567 (+10.9%)           | Pull

↓ TPOT mean (s)             | 0.211        | 0.216 (+2.4%)            | 0.207* (−1.7%)           | 0.216 (+2.2%)            | Push-Random
↓ TPOT p99 (s)              | 0.284        | 0.304 (+7.4%)            | 0.297 (+4.9%)            | 0.311 (+9.8%)            | Pull


------------------------------------------------
