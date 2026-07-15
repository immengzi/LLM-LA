---
id: c01-009
date:
campaign: 01-push-vs-pull-strategy
series: "9"
exp_ids: [37, 38, 39, 40]
methods: []
status: migrated
source: logs
---
# Series 9 — check the pull method under none-capped input and output token (natural prefill and decode scenario) on dump

Series 9:
Setup:
- 8 servers
- No cap on input tokens
- Request response length: unlimited
- Total requests: 200
- pattern: "dump"
- Methods:
  37) Pull
  38) Push-RR
  39) Push-Random
  40) Push-Least-Queue

Purpose: check the pull method under none-capped input and output token (natural prefill and decode scenario) on dump
Findings: Burst arrivals favor controlled batching. Pull absorbs bursts cleanly, while push reacts faster but destabilizes queues and tail latency.

Observations:
Metric (stat)               | Pull (exp37) | Push-RR (exp38)          | Push-Random (exp39)      | Push-LQ (exp40)          | Winner
----------------------------|--------------|--------------------------|--------------------------|--------------------------|--------
↓ Latency mean (s)          | 40.48*       | 42.53 (+5.1%)            | 57.03 (+40.8%)           | 61.63 (+52.2%)           | Pull
↓ Latency p99 (s)           | 130.79*      | 141.09 (+7.9%)           | 143.76 (+9.9%)           | 158.97 (+21.6%)          | Pull

↑ Decode TPS mean           | 443.51*      | 422.56 (−4.7%)           | 392.44 (−11.5%)          | 416.49 (−6.1%)           | Pull
↑ Prefill TPS mean          | 62.03*       | 59.52 (−4.0%)            | 49.03 (−21.0%)           | 48.17 (−22.3%)           | Pull

↑ Requests running mean     | 14.26*       | 13.89 (−2.6%)            | 12.67 (−11.2%)           | 13.30 (−6.7%)            | Pull

↓ TTFT mean (s)             | 1.46         | 1.45 (−1.1%)             | 0.72 (−50.6%)*           | 0.93 (−36.4%)            | Push-Random
↓ TTFT p99 (s)              | 6.53         | 7.42 (+13.6%)            | 3.93 (−39.9%)*           | 5.69 (−12.8%)            | Push-Random

↓ TPOT mean (s)             | 0.123        | 0.120 (−2.3%)            | 0.114 (−7.3%)            | 0.112 (−8.8%)*           | Push-LQ
↓ TPOT p99 (s)              | 0.510        | 0.472 (−7.4%)            | 0.533 (+4.6%)            | 0.356 (−30.2%)*          | Push-LQ

------------------------------------------------
