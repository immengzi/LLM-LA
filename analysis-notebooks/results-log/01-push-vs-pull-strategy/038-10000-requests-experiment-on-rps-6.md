---
id: c01-038
date:
campaign: 01-push-vs-pull-strategy
series: "38"
exp_ids: [153, 154, 155, 156]
methods: []
status: migrated
source: logs
---
# Series 38 — 10000 requests experiment on rps 6

Series 38:
Setup:
- 8 servers
- No cap on input tokens
- Request response length: 8192
- Total requests: 10000
- pattern: "det"
- rate_rps: 6
- Methods:
  153) Pull
  154) Push-RR
  155) Push-Random
  156) Push-Least-Queue

Purpose: 10000 requests experiment on rps 6
Findings: After 3 we seem to starrt getting diminishing return. Almost no improvement in mean and lower improvement in p99 start to appear compared to all rps 3, 4 and 5


Metric (mean / p99)            | Pull (exp153) | Push-RR (exp154)        | Push-Random (exp155)     | Push-LQ (exp156)         | Winner
-------------------------------|---------------|-------------------------|--------------------------|--------------------------|--------
↓ End-to-end latency mean (s)  | 831.97*       | 812.74 (−2.3%)          | 834.25 (+0.3%)           | 835.56 (+0.4%)           | Push-RR
↓ End-to-end latency p99 (s)   | 1657.75*      | 1751.06 (+5.6%)         | 1780.67 (+7.4%)          | 1868.50 (+12.8%)         | Pull

↑ Decode TPS mean              | 1682.73*      | 1568.76 (−6.8%)         | 1620.20 (−3.7%)          | 1557.07 (−7.5%)          | Pull
↑ Prefill TPS mean             | 238.85*       | 222.15 (−7.0%)          | 230.63 (−3.4%)           | 220.83 (−7.5%)           | Pull

↑ Requests running mean        | 59.24*        | 55.25 (−6.7%)           | 56.92 (−3.9%)            | 54.76 (−7.6%)            | Pull

↓ TTFT mean (s)                | 2.377         | 2.182 (−8.2%)           | 2.263 (−4.8%)            | 2.173* (−8.6%)           | Push-LQ
↓ TPOT mean (s)                | 0.2754        | 0.2580 (−6.3%)          | 0.2636 (−4.3%)           | 0.2548* (−7.5%)          | Push-LQ


Metric                         | Pull (exp153) | Push-RR (exp154)        | Push-Random (exp155)     | Push-LQ (exp156)         | Winner
-------------------------------|---------------|--------------------------|---------------------------|---------------------------|--------
↑ Successful requests          | 9960          | 9621 (−3.4%)            | 9942 (−0.2%)             | 9962* (+0.0%)            | Push-LQ
↓ Connection failures          | 40            | 379 (+847.5%)           | 58 (+45.0%)              | 38* (−5.0%)              | Push-LQ

↑ Success rate (%)             | 99.60         | 96.21 (−3.4%)           | 99.42 (−0.2%)            | 99.62* (+0.0%)           | Push-LQ
↓ Failure rate (%)             | 0.40          | 3.79 (+847.5%)          | 0.58 (+45.0%)            | 0.38* (−5.0%)            | Push-LQ
