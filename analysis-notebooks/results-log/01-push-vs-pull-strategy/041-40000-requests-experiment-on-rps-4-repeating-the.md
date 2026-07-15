---
id: c01-041
date:
campaign: 01-push-vs-pull-strategy
series: "41"
exp_ids: [165, 166, 167, 168]
methods: []
status: migrated
source: logs
---
# Series 41 — 40000 requests experiment on rps 4, repeating the

Series 41:
Setup:
- 8 servers
- No cap on input tokens
- Request response length: 8192
- Total requests: 40000
- pattern: "det"
- rate_rps: 4
- Methods:
  165) Pull
  166) Push-RR
  167) Push-Random
  168) Push-Least-Queue

Purpose: 40000 requests experiment on rps 4, repeating the
Findings: Since the servers became over saturated then the queuing latency is now so high that the difference between methdos become marginal


Metric (mean / p99)            | Pull (exp165) | Push-RR (exp166)        | Push-Random (exp167)     | Push-LQ (exp168)         | Winner
-------------------------------|---------------|-------------------------|--------------------------|--------------------------|--------
↓ End-to-end latency mean (s)  | 1677.90*      | 1725.53 (+2.8%)         | 1724.90 (+2.8%)          | 1724.97 (+2.8%)          | Pull
↓ End-to-end latency p99 (s)   | 3374.82*      | 3885.56 (+15.1%)        | 3667.20 (+8.7%)          | 3933.40 (+16.6%)         | Pull

↑ Decode TPS mean              | 1753.52*      | 1668.98 (−4.8%)         | 1691.86 (−3.5%)          | 1664.31 (−5.1%)          | Pull
↑ Prefill TPS mean             | 248.03*       | 235.72 (−5.0%)          | 239.14 (−3.6%)           | 234.85 (−5.3%)           | Pull

↑ Requests running mean        | 61.96*        | 59.09 (−4.6%)           | 59.85 (−3.4%)            | 58.89 (−4.9%)            | Pull

↓ TTFT mean (s)                | 2.394         | 2.342* (−2.2%)          | 2.352 (−1.8%)            | 2.330 (−2.7%)            | Push-LQ
↓ TPOT mean (s)                | 0.2826        | 0.2702* (−4.4%)         | 0.2732 (−3.3%)           | 0.2691 (−4.8%)           | Push-LQ


Metric                         | Pull (exp165) | Push-RR (exp166)        | Push-Random (exp167)     | Push-LQ (exp168)         | Winner
-------------------------------|---------------|-------------------------|--------------------------|--------------------------|--------
↑ Successful requests          | 39835*        | 39635 (−0.5%)           | 39743 (−0.2%)            | 39786 (−0.1%)            | Pull
↓ Connection failures          | 165*          | 365 (+121.2%)           | 257 (+55.8%)             | 214 (+29.7%)             | Pull

↑ Success rate (%)             | 99.5875*      | 99.0875 (−0.5%)         | 99.3575 (−0.2%)          | 99.4650 (−0.1%)          | Pull
↓ Failure rate (%)             | 0.4125*       | 0.9125 (+121.2%)        | 0.6425 (+55.8%)          | 0.5350 (+29.7%)          | Pull
