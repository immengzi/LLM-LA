---
id: c01-039
date:
campaign: 01-push-vs-pull-strategy
series: "39"
exp_ids: [157, 158, 159, 160]
methods: []
status: migrated
source: logs
---
# Series 39 — 25000 requests experiment on rps 3, repeating the

Series 39:
Setup:
- 8 servers
- No cap on input tokens
- Request response length: 8192
- Total requests: 25000
- pattern: "det"
- rate_rps: 3
- Methods:
  157) Pull
  158) Push-RR
  159) Push-Random
  160) Push-Least-Queue

Purpose: 25000 requests experiment on rps 3, repeating the
Findings: 3 seems to be a magic number that shows the best performance

Metric (mean / p99)            | Pull (exp157) | Push-RR (exp158)        | Push-Random (exp159)     | Push-LQ (exp160)         | Winner
-------------------------------|---------------|-------------------------|--------------------------|--------------------------|--------
↓ End-to-end latency mean (s)  | 55.88*        | 183.66 (+228.6%)        | 189.31 (+238.8%)         | 182.44 (+226.5%)         | Pull
↓ End-to-end latency p99 (s)   | 302.28*       | 552.80 (+82.9%)         | 775.06 (+156.4%)         | 528.59 (+74.8%)          | Pull

↑ Decode TPS mean              | 1724.92*      | 1633.00 (−5.3%)         | 1648.59 (−4.4%)          | 1644.31 (−4.7%)          | Pull
↑ Prefill TPS mean             | 243.91*       | 230.18 (−5.6%)          | 232.40 (−4.7%)           | 231.84 (−5.0%)           | Pull

↑ Requests running mean        | 60.56*        | 57.44 (−5.1%)           | 58.07 (−4.1%)            | 57.80 (−4.6%)            | Pull

↓ TTFT mean (s)                | 2.309         | 2.282 (−1.2%)           | 2.312 (+0.1%)            | 2.266* (−1.9%)           | Push-LQ
↓ TPOT mean (s)                | 0.2787        | 0.2692* (−3.4%)         | 0.2723 (−2.3%)           | 0.2707 (−2.9%)           | Push-RR


Metric                         | Pull (exp157) | Push-RR (exp158)        | Push-Random (exp159)     | Push-LQ (exp160)         | Winner
-------------------------------|---------------|-------------------------|--------------------------|--------------------------|--------
↑ Successful requests          | 24894*        | 24879 (−0.06%)          | 24826 (−0.27%)           | 24876 (−0.07%)           | Pull
↓ Connection failures          | 106*          | 121 (+14.2%)            | 174 (+64.2%)             | 124 (+17.0%)             | Pull

↑ Success rate (%)             | 99.576*       | 99.516 (−0.06%)         | 99.304 (−0.27%)          | 99.504 (−0.07%)          | Pull
↓ Failure rate (%)             | 0.424*        | 0.484 (+14.2%)          | 0.696 (+64.2%)           | 0.496 (+17.0%)           | Pull
