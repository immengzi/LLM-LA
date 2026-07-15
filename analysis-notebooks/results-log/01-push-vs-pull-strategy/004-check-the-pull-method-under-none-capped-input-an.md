---
id: c01-004
date:
campaign: 01-push-vs-pull-strategy
series: "4"
exp_ids: [17, 18, 19, 20]
methods: []
status: migrated
source: logs
---
# Series 4 — check the pull method under none-capped input and output token (natural prefill and decode scenario) on rps 2

Series 4:
Setup:
- 8 servers
- No cap on input tokens
- Request response length: unlimited
- Total requests: 200
- pattern: "det"
- rate_rps: 2
- Methods:
  17) Pull
  18) Push-RR
  19) Push-Random
  20) Push-Least-Queue

Purpose: check the pull method under none-capped input and output token (natural prefill and decode scenario) on rps 2
Findings: Light load favors responsiveness: push improves TTFT, while Pull already begins to reduce tail amplification from occasional long requests.

Observations:
Metric (stat)               | Pull (exp17) | Push-RR (exp18)          | Push-Random (exp19)      | Push-LQ (exp20)          | Winner
----------------------------|--------------|--------------------------|--------------------------|--------------------------|--------
↓ Latency mean (s)          | 18.97*       | 22.28 (+17.4%)           | 19.32 (+1.8%)            | 19.65 (+3.6%)            | Pull
↓ Latency p99 (s)           | 108.79*      | 121.54 (+11.7%)          | 113.17 (+4.0%)           | 118.11 (+8.6%)           | Pull

↑ Decode TPS mean           | 380.90       | 383.37 (+0.6%)           | 387.13 (+1.6%)*          | 382.52 (+0.4%)           | Push-Random
↑ Prefill TPS mean          | 50.37        | 48.30 (−4.1%)            | 56.25 (+11.7%)           | 57.21 (+13.6%)*          | Push-LQ

↑ Requests running mean     | 11.72        | 11.81 (+0.7%)            | 11.87 (+1.3%)*           | 11.70 (−0.2%)            | Push-Random

↓ TTFT mean (s)             | 0.53         | 0.60 (+12.4%)            | 0.52 (−1.8%)*            | 0.62 (+16.3%)            | Push-Random
↓ TTFT p99 (s)              | 2.54*        | 3.31 (+30.5%)            | 2.61 (+3.1%)             | 2.96 (+16.7%)            | Pull

↓ TPOT mean (s)             | 0.126        | 0.116 (−7.8%)*           | 0.127 (+0.8%)            | 0.125 (−0.8%)            | Push-RR
↓ TPOT p99 (s)              | 0.340        | 0.300 (−11.8%)*          | 0.325 (−4.4%)            | 0.334 (−1.7%)            | Push-RR




------------------------------------------------
