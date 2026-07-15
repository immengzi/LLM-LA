---
id: c01-001
date:
campaign: 01-push-vs-pull-strategy
series: "1"
exp_ids: [5, 6, 7, 8]
methods: []
status: migrated
source: logs
---
# Series 1 — check the pull method under none-capped on input and output token (prefill visible but less heavy since the decode is dominant)

Series 1:
Setup:
- 8 servers
- Request response length: unlimited (until EOS)
- Input tokens: 256–512
- Total requests: 200
- pattern: "dump"
- Methods:
  5) Pull
  6) Push-RR
  7) Push-Random
  8) Push-Least-Queue

Purpose: check the pull method under none-capped on input and output token (prefill visible but less heavy since the decode is dominant)
Findings: Once decode dominates, early admission helps. Push methods improve TTFT and TPOT by starting work sooner, while Pull remains competitive on throughput but slightly slower in responsiveness due to conservative gating.

Observations:
Metric (stat)               | Pull (exp5) | Push-RR (exp6)           | Push-Random (exp7)       | Push-LQ (exp8)           | Winner
----------------------------|-------------|--------------------------|--------------------------|--------------------------|--------
↓ Latency mean (s)          | 70.27       | 70.21* (−0.1%)           | 70.44 (+0.2%)            | 73.80 (+5.0%)            | Push-RR
↓ Latency p99 (s)           | 287.42      | 285.26* (−0.8%)          | 290.30 (+1.0%)           | 307.23 (+6.9%)           | Push-RR

↑ Decode TPS mean           | 370.91*     | 337.30 (−9.1%)           | 363.17 (−2.1%)           | 323.80 (−12.7%)          | Pull
↑ Prefill TPS mean          | 246.10*     | 180.76 (−26.6%)          | 222.36 (−9.7%)           | 206.14 (−16.2%)          | Pull

↑ Requests running mean     | 17.28*      | 15.59 (−9.8%)            | 16.34 (−5.5%)            | 14.39 (−16.8%)           | Pull

↓ TTFT mean (s)             | 6.83        | 6.14 (−10.2%)            | 5.50 (−19.6%)            | 4.90* (−28.3%)           | Push-LQ
↓ TTFT p99 (s)              | 40.62       | 31.90* (−21.5%)          | 36.02 (−11.3%)           | 37.11 (−8.6%)            | Push-RR

↓ TPOT mean (s)             | 0.235       | 0.203 (−13.7%)           | 0.214 (−9.0%)            | 0.179* (−24.0%)          | Push-LQ
↓ TPOT p99 (s)              | 1.695       | 1.467 (−13.4%)           | 1.437* (−15.2%)          | 1.493 (−12.0%)           | Push-Random


------------------------------------------------
