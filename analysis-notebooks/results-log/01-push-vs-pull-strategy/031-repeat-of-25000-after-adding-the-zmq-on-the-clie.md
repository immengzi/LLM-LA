---
id: c01-031
date:
campaign: 01-push-vs-pull-strategy
series: "31"
exp_ids: [125, 126, 127, 128]
methods: []
status: migrated
source: logs
---
# Series 31 — repeat of 25000 after adding the zmq on the client side

Series 31:
Setup:
- 8 servers
- No cap on input tokens
- Request response length: 8192
- Total requests: 25000
- pattern: "det"
- rate_rps: 6
- Methods:
  125) Pull
  126) Push-RR
  127) Push-Random
  128) Push-Least-Queue

Purpose: repeat of 25000 after adding the zmq on the client side
Findings: lots of lost requests, no deduction

Metric (mean / p99)            | Pull (exp125) | Push-RR (exp126)        | Push-Random (exp127)     | Push-LQ (exp128)         | Winner
-------------------------------|---------------|-------------------------|--------------------------|--------------------------|--------
↓ End-to-end latency mean (s)  | 1285.30*      | 1297.07 (+0.9%)         | 1289.29 (+0.3%)          | 1286.79 (+0.1%)          | Pull
↓ End-to-end latency p99 (s)   | 2565.06*      | 2754.08 (+7.4%)         | 2683.31 (+4.6%)          | 2732.42 (+6.5%)          | Pull

↑ Decode TPS mean              | 1777.19*      | 1774.58 (−0.1%)         | 1774.41 (−0.2%)          | 1773.55 (−0.2%)          | Pull
↑ Prefill TPS mean             | 250.94        | 250.81 (−0.1%)          | 251.72* (+0.3%)          | 251.17 (+0.1%)           | Push-Random

↑ Requests running mean        | 62.89         | 62.92* (+0.05%)         | 62.85 (−0.06%)           | 62.88 (−0.02%)           | Push-RR

↓ TTFT mean (s)                | 2.333*        | 2.370 (+1.6%)           | 2.422 (+3.8%)            | 2.387 (+2.3%)            | Pull
↓ TPOT mean (s)                | 0.2839*       | 0.2845 (+0.2%)          | 0.2843 (+0.2%)           | 0.2844 (+0.2%)           | Pull


Metric                         | Pull (exp125) | Push-RR (exp126)        | Push-Random (exp127)     | Push-LQ (exp128)         | Winner
-------------------------------|---------------|--------------------------|---------------------------|---------------------------|--------
↑ Successful requests          | 15203         | 15162 (−0.27%)          | 15222* (+0.12%)          | 15148 (−0.36%)           | Push-Random
↓ Connection failures          | 9797          | 9838 (+0.42%)           | 9778* (−0.19%)           | 9852 (+0.56%)            | Push-Random

↑ Success rate (%)             | 60.812        | 60.648 (−0.27%)         | 60.888* (+0.13%)         | 60.592 (−0.36%)          | Push-Random
↓ Failure rate (%)             | 39.188        | 39.352 (+0.42%)         | 39.112* (−0.19%)         | 39.408 (+0.56%)          | Push-Random
