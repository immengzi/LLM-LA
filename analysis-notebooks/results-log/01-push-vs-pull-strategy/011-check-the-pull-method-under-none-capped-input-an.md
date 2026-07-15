---
id: c01-011
date:
campaign: 01-push-vs-pull-strategy
series: "11"
exp_ids: [45, 46, 47, 48]
methods: []
status: migrated
source: logs
---
# Series 11 — check the pull method under none-capped input and output token (natural prefill and decode scenario) on rps 3 repeat of series 6  and 10

Series 11:
Setup:
- 8 servers
- No cap on input tokens
- Request response length: unlimited
- Total requests: 200
- pattern: "det"
- rate_rps: 3
- Methods:
  45) Pull
  46) Push-RR
  47) Push-Random
  48) Push-Least-Queue

Purpose: check the pull method under none-capped input and output token (natural prefill and decode scenario) on rps 3 repeat of series 6  and 10
Findings: Results are consistent, Pull is latency-stable, push is responsiveness-biased, and Least-Queue is brittle under heterogeneous service times.

Metric (stat)               | Pull (exp45) | Push-RR (exp46)          | Push-Random (exp47)      | Push-LQ (exp48)          | Winner
----------------------------|--------------|--------------------------|--------------------------|--------------------------|--------
↓ Latency mean (s)          | 21.47*       | 21.97 (+2.4%)            | 26.18 (+22.0%)           | 30.64 (+42.7%)           | Pull
↓ Latency p99 (s)           | 100.22*      | 113.54 (+13.3%)          | 110.34 (+10.1%)          | 140.16 (+39.8%)          | Pull

↑ Decode TPS mean           | 416.09       | 417.02* (+0.2%)          | 414.96 (−0.3%)           | 410.30 (−1.4%)           | Push-RR
↑ Prefill TPS mean          | 55.41        | 62.15* (+12.2%)          | 60.49 (+9.2%)            | 61.45 (+10.9%)           | Push-RR

↑ Requests running mean     | 13.11        | 13.06 (−0.4%)            | 13.43* (+2.5%)           | 13.16 (+0.4%)            | Push-Random

↓ TTFT mean (s)             | 0.836        | 0.860 (+2.9%)            | 0.814* (−2.6%)           | 0.943 (+12.8%)           | Push-Random
↓ TTFT p99 (s)              | 3.53*        | 4.31 (+22.0%)            | 4.71 (+33.4%)            | 6.27 (+77.5%)            | Pull

↓ TPOT mean (s)             | 0.133        | 0.132 (−0.7%)            | 0.130 (−2.5%)            | 0.121* (−9.3%)           | Push-LQ
↓ TPOT p99 (s)              | 0.465        | 0.582 (+25.1%)           | 0.478 (+2.8%)            | 0.468 (+0.7%)            | Pull
