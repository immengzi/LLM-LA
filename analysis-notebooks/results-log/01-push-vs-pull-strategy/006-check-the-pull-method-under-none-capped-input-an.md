---
id: c01-006
date:
campaign: 01-push-vs-pull-strategy
series: "6"
exp_ids: [25, 26, 27, 28]
methods: []
status: migrated
source: logs
---
# Series 6 — check the pull method under none-capped input and output token (natural prefill and decode scenario) on rps 4

Series 6:
Setup:
- 8 servers
- No cap on input tokens
- Request response length: unlimited
- Total requests: 200
- pattern: "det"
- rate_rps: 4
- Methods:
  25) Pull
  26) Push-RR
  27) Push-Random
  28) Push-Least-Queue

Purpose: check the pull method under none-capped input and output token (natural prefill and decode scenario) on rps 4
Findings: At sustained high load, speculative admission breaks down. Pull clearly dominates latency and tails by preventing overload, while push suffers from queue inflation.

Observations:
Metric (stat)               | Pull (exp25) | Push-RR (exp26)          | Push-Random (exp27)      | Push-LQ (exp28)          | Winner
----------------------------|--------------|--------------------------|--------------------------|--------------------------|--------
↓ Latency mean (s)          | 23.46*       | 39.33 (+67.6%)           | 38.76 (+65.2%)           | 24.72 (+5.4%)            | Pull
↓ Latency p99 (s)           | 111.08*      | 140.54 (+26.5%)          | 114.79 (+3.3%)           | 112.92 (+1.7%)           | Pull

↑ Decode TPS mean           | 442.06*      | 432.18 (−2.2%)           | 422.11 (−4.5%)           | 429.90 (−2.8%)           | Pull
↑ Prefill TPS mean          | 64.24*       | 58.56 (−8.8%)            | 56.41 (−12.2%)           | 63.62 (−1.0%)            | Pull

↑ Requests running mean     | 14.13*       | 13.85 (−2.0%)            | 13.77 (−2.6%)            | 13.70 (−3.1%)            | Pull

↓ TTFT mean (s)             | 0.98         | 0.83* (−15.2%)           | 0.84 (−13.7%)            | 0.97 (−1.0%)             | Push-RR
↓ TTFT p99 (s)              | 3.78*        | 6.02 (+59.3%)            | 7.00 (+85.2%)            | 3.89 (+2.9%)             | Pull

↓ TPOT mean (s)             | 0.129        | 0.120 (−7.5%)            | 0.115* (−11.1%)          | 0.126 (−2.7%)            | Push-Random
↓ TPOT p99 (s)              | 0.486        | 0.425 (−12.6%)           | 0.412 (−15.3%)           | 0.374* (−23.0%)          | Push-LQ




------------------------------------------------
