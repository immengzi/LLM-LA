---
id: c01-035
date:
campaign: 01-push-vs-pull-strategy
series: "35"
exp_ids: [141, 142, 143, 144]
methods: []
status: migrated
source: logs
---
# Series 35 — 10000 requests experiment on rps 3

Series 35:
Setup:
- 8 servers
- No cap on input tokens
- Request response length: 8192
- Total requests: 10000
- pattern: "det"
- rate_rps: 3
- Methods:
  141) Pull
  142) Push-RR
  143) Push-Random
  144) Push-Least-Queue

Purpose: 10000 requests experiment on rps 3
Findings: 3 seems to be a magic number that shows the best performance

Metric (mean / p99)            | Pull (exp141) | Push-RR (exp142)        | Push-Random (exp143)     | Push-LQ (exp144)         | Winner
-------------------------------|---------------|-------------------------|--------------------------|--------------------------|--------
↓ End-to-end latency mean (s)  | 64.89*        | 116.18 (+79.1%)         | 102.12 (+57.4%)          | 117.59 (+81.2%)          | Pull
↓ End-to-end latency p99 (s)   | 319.23*       | 426.48 (+33.6%)         | 519.51 (+62.7%)          | 420.15 (+31.6%)          | Pull

↑ Decode TPS mean              | 1638.82*      | 1525.46 (−6.9%)         | 1488.80 (−9.1%)          | 1534.61 (−6.4%)          | Pull
↑ Prefill TPS mean             | 232.54*       | 216.72 (−6.8%)          | 212.13 (−8.8%)           | 217.50 (−6.5%)           | Pull

↑ Requests running mean        | 57.50*        | 53.30 (−7.3%)           | 52.02 (−9.5%)            | 53.69 (−6.6%)            | Pull

↓ TTFT mean (s)                | 2.322         | 2.202 (−5.2%)           | 2.098* (−9.6%)           | 2.206 (−5.0%)            | Push-Random
↓ TPOT mean (s)                | 0.2737        | 0.2585 (−5.6%)          | 0.2517* (−8.0%)          | 0.2603 (−4.9%)           | Push-Random


Metric                         | Pull (exp141) | Push-RR (exp142)         | Push-Random (exp143)     | Push-LQ (exp144)         | Winner
-------------------------------|---------------|--------------------------|--------------------------|--------------------------|--------
↑ Successful requests          | 9976*         | 9964 (−0.12%)            | 9974 (−0.02%)            | 9962 (−0.14%)            | Pull
↓ Connection failures          | 24*           | 36 (+50.0%)              | 26 (+8.3%)               | 38 (+58.3%)              | Pull

↑ Success rate (%)             | 99.76*        | 99.64 (−0.12%)           | 99.74 (−0.02%)           | 99.62 (−0.14%)           | Pull
↓ Failure rate (%)             | 0.24*         | 0.36 (+50.0%)            | 0.26 (+8.3%)             | 0.38 (+58.3%)            | Pull
