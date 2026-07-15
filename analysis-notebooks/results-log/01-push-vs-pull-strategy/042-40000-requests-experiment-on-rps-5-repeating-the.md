---
id: c01-042
date:
campaign: 01-push-vs-pull-strategy
series: "42"
exp_ids: [169, 170, 171, 172]
methods: []
status: migrated
source: logs
---
# Series 42 — 40000 requests experiment on rps 5, repeating the

Series 42:
Setup:
- 8 servers
- No cap on input tokens
- Request response length: 8192
- Total requests: 40000
- pattern: "det"
- rate_rps: 5
- Methods:
  169) Pull
  170) Push-RR
  171) Push-Random
  172) Push-Least-Queue

Purpose: 40000 requests experiment on rps 5, repeating the
Findings: Since the servers became over saturated then the queuing latency is now so high that the difference between methdos become marginal


Metric (mean / p99)            | Pull (exp169) | Push-RR (exp170)        | Push-Random (exp171)     | Push-LQ (exp172)         | Winner
-------------------------------|---------------|-------------------------|--------------------------|--------------------------|--------
↓ End-to-end latency mean (s)  | 2679.19       | 2712.38 (+1.2%)         | 2676.70* (−0.1%)         | 2720.64 (+1.5%)          | Push-Random
↓ End-to-end latency p99 (s)   | 5357.46*      | 5794.28 (+8.2%)         | 5569.32 (+4.0%)          | 5763.50 (+7.6%)          | Pull

↑ Decode TPS mean              | 1747.66*      | 1676.36 (−4.1%)         | 1709.97 (−2.2%)          | 1671.81 (−4.3%)          | Pull
↑ Prefill TPS mean             | 246.99*       | 236.75 (−4.1%)          | 242.19 (−1.9%)           | 236.01 (−4.4%)           | Pull

↑ Requests running mean        | 61.89*        | 59.21 (−4.3%)           | 60.31 (−2.6%)            | 59.19 (−4.4%)            | Pull

↓ TTFT mean (s)                | 2.463         | 2.277* (−7.6%)          | 2.334 (−5.2%)            | 2.340 (−5.0%)            | Push-RR
↓ TPOT mean (s)                | 0.2828        | 0.2700* (−4.5%)         | 0.2744 (−3.0%)           | 0.2706 (−4.3%)           | Push-RR


Metric                         | Pull (exp169) | Push-RR (exp170)         | Push-Random (exp171)      | Push-LQ (exp172)          | Winner
-------------------------------|---------------|--------------------------|---------------------------|---------------------------|--------
↑ Successful requests          | 39661         | 39830* (+0.43%)          | 39820 (+0.40%)            | 39822 (+0.41%)            | Push-RR
↓ Connection failures          | 339           | 170* (−49.85%)           | 180 (−46.90%)             | 178 (−47.49%)             | Push-RR

↑ Success rate (%)             | 99.1525       | 99.5750* (+0.43%)        | 99.5500 (+0.40%)          | 99.5550 (+0.41%)          | Push-RR
↓ Failure rate (%)             | 0.8475        | 0.4250* (−49.85%)        | 0.4500 (−46.90%)          | 0.4450 (−47.49%)          | Push-RR
