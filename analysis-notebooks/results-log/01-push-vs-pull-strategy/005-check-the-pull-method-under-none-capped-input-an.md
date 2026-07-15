---
id: c01-005
date:
campaign: 01-push-vs-pull-strategy
series: "5"
exp_ids: [21, 22, 23, 24]
methods: []
status: migrated
source: logs
---
# Series 5 — check the pull method under none-capped input and output token (natural prefill and decode scenario) on rps 3

Series 5:
Setup:
- 8 servers
- No cap on input tokens
- Request response length: unlimited
- Total requests: 200
- pattern: "det"
- rate_rps: 3
- Methods:
  21) Pull
  22) Push-RR
  23) Push-Random
  24) Push-Least-Queue

Purpose: check the pull method under none-capped input and output token (natural prefill and decode scenario) on rps 3
Findings: Near saturation, the core trade-off appears: Pull protects tail latency via admission control, while push improves start time by admitting more work at the cost of queue growth.

Observations:
Metric (stat)               | Pull (exp21) | Push-RR (exp22)          | Push-Random (exp23)      | Push-LQ (exp24)          | Winner
----------------------------|--------------|--------------------------|--------------------------|--------------------------|--------
↓ Latency mean (s)          | 20.90*       | 31.57 (+51.1%)           | 27.12 (+29.8%)           | 21.95 (+5.0%)            | Pull
↓ Latency p99 (s)           | 100.87*      | 159.22 (+57.9%)          | 114.34 (+13.4%)          | 113.58 (+12.6%)          | Pull

↑ Decode TPS mean           | 414.96       | 414.06 (−0.2%)           | 415.18* (+0.05%)         | 413.15 (−0.4%)           | Push-Random
↑ Prefill TPS mean          | 55.35        | 47.54 (−14.1%)           | 55.83 (+0.9%)            | 60.12* (+8.6%)           | Push-LQ

↑ Requests running mean     | 12.86        | 13.27* (+3.2%)           | 13.08 (+1.7%)            | 13.08 (+1.7%)            | Push-RR

↓ TTFT mean (s)             | 0.652        | 0.876 (+34.5%)           | 0.611* (−6.2%)           | 0.806 (+23.7%)           | Push-Random
↓ TTFT p99 (s)              | 3.247        | 6.907 (+112.8%)          | 2.623* (−19.2%)          | 2.982 (−8.1%)            | Push-Random

↓ TPOT mean (s)             | 0.126        | 0.116* (−8.7%)           | 0.118 (−6.7%)            | 0.133 (+4.8%)            | Push-RR
↓ TPOT p99 (s)              | 0.336        | 0.304* (−9.5%)           | 0.382 (+13.8%)           | 0.571 (+70.0%)           | Push-RR




------------------------------------------------
