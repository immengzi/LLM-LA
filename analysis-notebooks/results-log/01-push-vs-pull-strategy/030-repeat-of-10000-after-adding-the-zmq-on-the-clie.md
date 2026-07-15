---
id: c01-030
date:
campaign: 01-push-vs-pull-strategy
series: "30"
exp_ids: [121, 122, 123, 124]
methods: []
status: migrated
source: logs
---
# Series 30 — repeat of 10000 after adding the zmq on the client side

Series 30:
Setup:
- 8 servers
- No cap on input tokens
- Request response length: 8192
- Total requests: 10000
- pattern: "det"
- rate_rps: 6
- Methods:
  121) Pull
  122) Push-RR
  123) Push-Random
  124) Push-Least-Queue

Purpose: repeat of 10000 after adding the zmq on the client side
Findings: lots of lost requests, no deduction

Metric (mean / p99)            | Pull (exp121) | Push-RR (exp122)        | Push-Random (exp123)     | Push-LQ (exp124)         | Winner
-------------------------------|---------------|-------------------------|--------------------------|--------------------------|--------
↓ End-to-end latency mean (s)  | 278.33*       | 583.94 (+109.8%)        | 588.89 (+111.6%)         | 588.60 (+111.5%)         | Pull
↓ End-to-end latency p99 (s)   | 725.52*       | 1328.94 (+83.2%)        | 1262.81 (+74.0%)         | 1359.81 (+87.4%)         | Pull

↑ Decode TPS mean              | 1513.58       | 1767.10 (+16.8%)        | 1771.62 (+17.0%)*        | 1763.21 (+16.5%)         | Push-Random
↑ Prefill TPS mean             | 211.58        | 252.61 (+19.4%)         | 256.60 (+21.3%)*         | 251.60 (+18.9%)          | Push-Random

↑ Requests running mean        | 53.08         | 62.73 (+18.2%)          | 62.68 (+18.1%)           | 62.76 (+18.3%)*          | Push-LQ

↓ TTFT mean (s)                | 2.440         | 2.391 (−2.0%)*          | 2.434 (−0.3%)            | 2.407 (−1.4%)            | Push-RR
↓ TPOT mean (s)                | 0.2709*       | 0.2840 (+4.8%)          | 0.2830 (+4.5%)           | 0.2847 (+5.1%)           | Pull


Metric                         | Pull (exp121) | Push-RR (exp122)        | Push-Random (exp123)     | Push-LQ (exp124)         | Winner
-------------------------------|---------------|--------------------------|---------------------------|---------------------------|--------
↑ Successful requests          | 5648*         | 7020 (+24.3%)           | 7113 (+26.0%)            | 6991 (+23.8%)             | Push-Random
↓ Connection failures          | 102*          | 2980 (+2821.6%)         | 2887 (+2731.4%)          | 3009 (+2850.0%)           | Pull

↑ Success rate (%)             | 98.23*        | 70.20 (−28.6%)          | 71.13 (−27.6%)           | 69.91 (−28.8%)            | Pull
↓ Failure rate (%)             | 1.77*         | 29.80 (+1580.9%)        | 28.87 (+1527.9%)         | 30.09 (+1597.2%)          | Pull
