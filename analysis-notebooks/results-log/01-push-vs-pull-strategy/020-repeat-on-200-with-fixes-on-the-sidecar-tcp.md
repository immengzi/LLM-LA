---
id: c01-020
date:
campaign: 01-push-vs-pull-strategy
series: "20"
exp_ids: [81, 82, 83, 84]
methods: []
status: migrated
source: logs
---
# Series 20 — repeat on 200 with fixes on the sidecar tcp

Series 20:
Setup:
- 8 servers
- No cap on input tokens
- Request response length: 8192
- Total requests: 200
- pattern: "det"
- rate_rps: 3
- Methods:
  77) Pull
  78) Push-RR
  79) Push-Random
  80) Push-Least-Queue

Purpose: repeat on 200 with fixes on the sidecar tcp
Findings: sanity check, good but not representative due to short experiment

Metric                         | Pull (exp81) | Push-RR (exp82)        | Push-Random (exp83)     | Push-LQ (exp84)         | Winner
-------------------------------|--------------|------------------------|-------------------------|-------------------------|--------
↓ End-to-end latency mean (s)  | 21.23*       | 21.85 (+2.9%)          | 33.72 (+58.8%)          | 21.87 (+3.0%)           | Pull
↓ End-to-end latency p99 (s)   | 103.60*      | 113.09 (+9.2%)         | 117.30 (+13.2%)         | 113.08 (+9.2%)          | Pull

↑ Decode TPS mean              | 417.98*      | 411.87 (−1.5%)         | 400.06 (−4.3%)          | 412.04 (−1.4%)          | Pull
↑ Prefill TPS mean             | 63.34*       | 63.05 (−0.5%)          | 57.24 (−9.6%)           | 60.67 (−4.2%)           | Push-LQ

↑ Requests running mean        | 13.12*       | 13.04 (−0.6%)          | 12.81 (−2.4%)           | 13.01 (−0.8%)           | Pull

↓ TTFT mean (s)                | 0.80         | 0.91 (+12.8%)          | 0.80* (best)            | 0.82 (+2.0%)            | Push-Random
↓ TPOT mean (s)                | 0.134        | 0.131                  | 0.125*                  | 0.128                   | Push-Random
