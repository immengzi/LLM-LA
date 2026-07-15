---
id: c01-002
date:
campaign: 01-push-vs-pull-strategy
series: "2"
exp_ids: [9, 10, 11, 12]
methods: []
status: migrated
source: logs
---
# Series 2 — check the pull method under none-capped input and output token (natural prefill and decode scenario)

Series 2:
Setup:
- 8 servers
- No cap on input tokens
- Request response length: unlimited
- Total requests: 200
- pattern: "dump"
- Methods:
  9) Pull
  10) Push-RR
  11) Push-Random
  12) Push-Least-Queue

Purpose: check the pull method under none-capped input and output token (natural prefill and decode scenario)
Findings: Unbounded prefills expose scheduling trade-offs, Pull stabilizes mean latency and TPOT by pacing heavy requests, while push reduces TTFT but increases variance from uncontrolled overlap.

Observations:
Metric (stat)               | Pull (exp9) | Push-RR (exp10)          | Push-Random (exp11)      | Push-LQ (exp12)          | Winner
----------------------------|-------------|--------------------------|--------------------------|--------------------------|--------
↓ Latency mean (s)          | 40.01*      | 45.14 (+12.8%)           | 43.37 (+8.4%)            | 41.52 (+3.8%)            | Pull
↓ Latency p99 (s)           | 132.31      | 119.69* (−9.5%)          | 144.27 (+9.0%)           | 137.95 (+4.3%)           | Push-RR

↑ Decode TPS mean           | 458.23*     | 452.54 (−1.2%)           | 451.19 (−1.5%)           | 432.11 (−5.7%)           | Pull
↑ Prefill TPS mean          | 63.09       | 63.72* (+1.0%)           | 55.49 (−12.1%)           | 60.78 (−3.7%)            | Push-RR

↑ Requests running mean     | 15.02*      | 14.82 (−1.4%)            | 14.73 (−2.0%)            | 14.31 (−4.8%)            | Pull

↓ TTFT mean (s)             | 1.76        | 1.34 (−23.7%)            | 1.14* (−35.5%)           | 1.63 (−7.6%)             | Push-Random
↓ TTFT p99 (s)              | 9.85        | 8.90 (−9.7%)             | 7.40* (−24.9%)           | 10.86 (+10.2%)           | Push-Random

↓ TPOT mean (s)             | 0.126       | 0.124* (−1.1%)           | 0.132 (+4.8%)            | 0.128 (+2.0%)            | Push-RR
↓ TPOT p99 (s)              | 0.430*      | 0.486 (+13.2%)           | 0.623 (+45.1%)           | 0.515 (+19.8%)           | Pull


------------------------------------------------
