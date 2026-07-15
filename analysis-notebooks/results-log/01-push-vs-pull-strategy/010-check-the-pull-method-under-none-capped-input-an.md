---
id: c01-010
date:
campaign: 01-push-vs-pull-strategy
series: "10"
exp_ids: [41, 42, 43, 44]
methods: []
status: migrated
source: logs
---
# Series 10 — check the pull method under none-capped input and output token (natural prefill and decode scenario) on rps 3 repeat of series 6  and 10

Series 10:
Setup:
- 8 servers
- No cap on input tokens
- Request response length: unlimited
- Total requests: 200
- pattern: "det"
- rate_rps: 3
- Methods:
  41) Pull
  42) Push-RR
  43) Push-Random
  44) Push-Least-Queue

Purpose: check the pull method under none-capped input and output token (natural prefill and decode scenario) on rps 3 repeat of series 6  and 10
Findings: Results are consistent, Pull is latency-stable, push is responsiveness-biased, and Least-Queue is brittle under heterogeneous service times.

Metric (stat)               | Pull (exp41) | Push-RR (exp42)          | Push-Random (exp43)      | Push-LQ (exp44)          | Winner
----------------------------|--------------|--------------------------|--------------------------|--------------------------|--------
↓ Latency mean (s)          | 20.85*       | 22.07 (+5.9%)            | 29.39 (+41.0%)           | 29.12 (+39.7%)           | Pull
↓ Latency p99 (s)           | 100.54*      | 112.86 (+12.3%)          | 125.56 (+24.9%)          | 136.77 (+36.0%)          | Pull

↑ Decode TPS mean           | 421.66*      | 413.66 (−1.9%)           | 411.05 (−2.5%)           | 416.34 (−1.3%)           | Pull
↑ Prefill TPS mean          | 61.51*       | 57.60 (−6.4%)            | 60.33 (−1.9%)            | 50.20 (−18.4%)           | Pull

↑ Requests running mean     | 13.04        | 13.04 (−0.1%)            | 13.19 (+1.1%)            | 13.26 (+1.6%)*           | Push-LQ

↓ TTFT mean (s)             | 0.77         | 0.79 (+2.9%)             | 0.76 (−0.6%)*            | 0.89 (+16.1%)            | Push-Random
↓ TTFT p99 (s)              | 3.26         | 3.07 (−5.9%)*            | 3.86 (+18.5%)            | 7.22 (+121.4%)           | Push-RR

↓ TPOT mean (s)             | 0.129        | 0.126 (−2.7%)            | 0.114 (−12.1%)*          | 0.120 (−7.2%)            | Push-Random
↓ TPOT p99 (s)              | 0.345*       | 0.464 (+34.8%)           | 0.357 (+3.5%)            | 0.488 (+41.6%)           | Pull


------------------------------------------------
