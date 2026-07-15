---
id: c01-034
date:
campaign: 01-push-vs-pull-strategy
series: "34"
exp_ids: [137, 138, 139, 140]
methods: []
status: migrated
source: logs
---
# Series 34 — 10000 requests experiment on rps 2

Series 34:
Setup:
- 8 servers
- No cap on input tokens
- Request response length: 8192
- Total requests: 10000
- pattern: "det"
- rate_rps: 2
- Methods:
  137) Pull
  138) Push-RR
  139) Push-Random
  140) Push-Least-Queue

Purpose: 10000 requests experiment on rps 2
Findings: under low and not saturated load there is almost no difference in the methods

Metric                         | Pull (exp137) | Push-RR (exp138)        | Push-Random (exp139)     | Push-LQ (exp140)         | Winner
-------------------------------|---------------|-------------------------|--------------------------|--------------------------|--------
↓ End-to-end latency mean (s)  | 19.69*        | 21.30 (+8.2%)           | 23.42 (+19.0%)           | 21.50 (+9.2%)            | Pull
↓ End-to-end latency p99 (s)   | 268.28*       | 268.48 (+0.1%)          | 269.04 (+0.3%)           | 268.84 (+0.2%)           | Pull

↑ Decode TPS mean              | 1148.89*      | 1146.09 (−0.2%)         | 1141.40 (−0.7%)          | 1146.51 (−0.2%)          | Pull
↑ Prefill TPS mean             | 163.35*       | 163.03 (−0.2%)          | 162.63 (−0.4%)           | 163.01 (−0.2%)           | Pull

↑ Requests running mean        | 37.09         | 37.50 (+1.1%)           | 37.56 (+1.3%)            | 37.58* (+1.3%)           | Push-LQ

↓ TTFT mean (s)                | 1.989         | 1.988* (−0.0%)          | 2.102 (+5.7%)            | 1.999 (+0.5%)            | Push-RR
↓ TPOT mean (s)                | 0.25339       | 0.25343 (+0.0%)         | 0.25279* (−0.2%)         | 0.25353 (+0.1%)          | Push-Random


Metric                         | Pull (exp137) | Push-RR (exp138)        | Push-Random (exp139)     | Push-LQ (exp140)         | Winner
-------------------------------|---------------|--------------------------|---------------------------|---------------------------|--------
↑ Successful requests          | 9998*         | 9992 (−0.06%)           | 9982 (−0.16%)            | 9980 (−0.18%)            | Pull
↓ Connection failures          | 2*            | 8 (+300.0%)             | 18 (+800.0%)             | 20 (+900.0%)             | Pull

↑ Success rate (%)             | 99.98*        | 99.92 (−0.06%)          | 99.82 (−0.16%)           | 99.80 (−0.18%)           | Pull
↓ Failure rate (%)             | 0.02*         | 0.08 (+300.0%)          | 0.18 (+800.0%)           | 0.20 (+900.0%)           | Pull
