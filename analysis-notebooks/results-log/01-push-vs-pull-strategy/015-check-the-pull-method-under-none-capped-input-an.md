---
id: c01-015
date:
campaign: 01-push-vs-pull-strategy
series: "15"
exp_ids: [61, 62, 63, 64]
methods: []
status: migrated
source: logs
---
# Series 15 — check the pull method under none-capped input and capped 8192 output token on rps 4, repeat of 13 and 14 on lower load so we don't get requests losts

Series 15:
Setup:
- 8 servers
- No cap on input tokens
- Request response length: 8192
- Total requests: 10000
- pattern: "det"
- rate_rps: 2.5
- Methods:
  61) Pull
  62) Push-RR
  63) Push-Random
  64) Push-Least-Queue

Purpose: check the pull method under none-capped input and capped 8192 output token on rps 4, repeat of 13 and 14 on lower load so we don't get requests losts
Findings: pull slightly better but since it isn't under saturation point like 13 and 14 which seems to be the place pull excels we cannot have a clear conclusion.

  Metric (stat)             | Pull (exp61) | Push-RR (exp62)             | Push-Random (exp63)        | Push-LQ (exp64)            | Winner
----------------------------|--------------|-----------------------------|----------------------------|----------------------------|--------
↓ Latency mean (s)          | 21.39*       | 24.43 (+14.2%)              | 26.52 (+24.0%)             | 24.03 (+12.4%)             | Pull
↓ Latency p99 (s)           | 266.95       | 268.90 (+0.7%)              | 270.73 (+1.4%)             | 268.86 (+0.7%)*            | Pull

↑ Decode TPS mean           | 1132.78      | 1133.38 (+0.05%)            | 1131.04 (−0.15%)           | 1136.31* (+0.31%)          | Push-LQ
↑ Prefill TPS mean          | 161.31       | 162.91* (+1.0%)             | 160.63 (−0.4%)             | 161.67 (+0.2%)             | Push-RR

↑ Requests running mean     | 37.07        | 37.43 (+1.0%)               | 37.69* (+1.7%)             | 37.70 (+1.7%)              | Push-Random

↓ TTFT mean (s)             | 0.482*       | 0.516 (+7.1%)               | 0.488 (+1.2%)              | 0.523 (+8.4%)              | Pull
↓ TTFT p99 (s)              | 2.205        | 2.570 (+16.5%)              | 2.444 (+10.8%)             | 2.596 (+17.7%)*            | Pull

↓ TPOT mean (s)             | 0.258        | 0.257 (−0.3%)*              | 0.254 (−1.6%)              | 0.257 (−0.3%)              | Push-Random
↓ TPOT p99 (s)              | 0.511        | 0.447 (−12.5%)*             | 0.483 (−5.5%)              | 0.529 (+3.4%)              | Push-RR
