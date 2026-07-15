---
id: c01-040
date:
campaign: 01-push-vs-pull-strategy
series: "40"
exp_ids: [161, 162, 163, 164]
methods: []
status: migrated
source: logs
---
# Series 40 — 40000 requests experiment on rps 3, repeating the

Series 40:
Setup:
- 8 servers
- No cap on input tokens
- Request response length: 8192
- Total requests: 40000
- pattern: "det"
- rate_rps: 3
- Methods:
  161) Pull
  162) Push-RR
  163) Push-Random
  164) Push-Least-Queue

Purpose: 40000 requests experiment on rps 3, repeating the
Findings: 3 seems to be a magic number that shows the best performance

Metric (mean / p99)            | Pull (exp161) | Push-RR (exp162)        | Push-Random (exp163)     | Push-LQ (exp164)         | Winner
-------------------------------|---------------|-------------------------|--------------------------|--------------------------|--------
↓ End-to-end latency mean (s)  | 76.62*        | 230.35 (+200.7%)        | 197.35 (+157.5%)         | 240.03 (+213.3%)         | Pull
↓ End-to-end latency p99 (s)   | 325.53*       | 764.80 (+135.0%)        | 633.73 (+94.7%)          | 762.11 (+134.1%)         | Pull

↑ Decode TPS mean              | 1744.18*      | 1674.61 (−4.0%)         | 1681.89 (−3.6%)          | 1679.19 (−3.7%)          | Pull
↑ Prefill TPS mean             | 246.72*       | 236.70 (−4.1%)          | 238.02 (−3.5%)           | 237.22 (−3.9%)           | Pull

↑ Requests running mean        | 61.56*        | 58.89 (−4.3%)           | 59.20 (−3.8%)            | 59.19 (−3.8%)            | Pull

↓ TTFT mean (s)                | 2.414         | 2.316* (−4.1%)          | 2.367 (−1.9%)            | 2.362 (−2.1%)            | Push-RR
↓ TPOT mean (s)                | 0.2818        | 0.2737* (−2.9%)         | 0.2749 (−2.5%)           | 0.2754 (−2.3%)           | Push-RR



Metric                         | Pull (exp161) | Push-RR (exp162)        | Push-Random (exp163)     | Push-LQ (exp164)         | Winner
-------------------------------|---------------|-------------------------|--------------------------|--------------------------|--------
↑ Successful requests          | 39884*        | 39846 (−0.10%)          | 39799 (−0.21%)           | 39791 (−0.23%)           | Pull
↓ Connection failures          | 116*          | 154 (+32.8%)            | 201 (+73.3%)             | 209 (+80.2%)             | Pull

↑ Success rate (%)             | 99.71*        | 99.62 (−0.10%)          | 99.50 (−0.21%)           | 99.48 (−0.23%)           | Pull
↓ Failure rate (%)             | 0.29*         | 0.39 (+32.8%)           | 0.50 (+73.3%)            | 0.52 (+80.2%)            | Pull
