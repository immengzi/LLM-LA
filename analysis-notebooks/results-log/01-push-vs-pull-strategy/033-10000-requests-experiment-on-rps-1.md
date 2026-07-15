---
id: c01-033
date:
campaign: 01-push-vs-pull-strategy
series: "33"
exp_ids: [133, 134, 135, 136]
methods: []
status: migrated
source: logs
---
# Series 33 — 10000 requests experiment on rps 1

Series 33:
Setup:
- 8 servers
- No cap on input tokens
- Request response length: 8192
- Total requests: 10000
- pattern: "det"
- rate_rps: 1
- Methods:
  133) Pull
  134) Push-RR
  135) Push-Random
  136) Push-Least-Queue

Purpose: 10000 requests experiment on rps 1
Findings: under low and not saturated load there is almost no difference in the methods

Metric (mean / p99)            | Pull (exp133) | Push-RR (exp134)        | Push-Random (exp135)     | Push-LQ (exp136)         | Winner
-------------------------------|---------------|-------------------------|--------------------------|--------------------------|--------
↓ End-to-end latency mean (s)  | 18.23         | 18.15* (−0.42%)         | 18.30 (+0.38%)           | 18.19 (−0.21%)           | Push-RR
↓ End-to-end latency p99 (s)   | 246.98        | 247.43 (+0.18%)         | 247.32 (+0.14%)          | 247.61 (+0.26%)          | Pull

↑ Decode TPS mean              | 588.65*       | 587.61 (−0.18%)         | 587.08 (−0.27%)          | 588.51 (−0.03%)          | Pull
↑ Prefill TPS mean             | 83.53         | 83.58 (+0.06%)          | 83.45 (−0.10%)           | 83.58* (+0.06%)          | Push-RR / Push-LQ

↑ Requests running mean        | 17.62*        | 17.49 (−0.75%)          | 17.60 (−0.07%)           | 17.58 (−0.21%)           | Pull

↓ TTFT mean (s)                | 1.753         | 1.821 (+3.88%)          | 1.757 (+0.24%)           | 1.791 (+2.17%)           | Pull
↓ TPOT mean (s)                | 0.2293*       | 0.2311 (+0.81%)         | 0.2307 (+0.61%)          | 0.2314 (+0.93%)          | Pull


Metric                         | Pull (exp133) | Push-RR (exp134)        | Push-Random (exp135)     | Push-LQ (exp136)         | Winner
-------------------------------|---------------|--------------------------|---------------------------|---------------------------|--------
↑ Successful requests          | 9993          | 9996 (+0.03%)           | 9999* (+0.06%)           | 9998 (+0.05%)            | Push-Random
↓ Connection failures          | 7             | 4 (−42.9%)              | 1* (−85.7%)              | 2 (−71.4%)               | Push-Random

↑ Success rate (%)             | 99.93         | 99.96 (+0.03%)          | 99.99* (+0.06%)          | 99.98 (+0.05%)           | Push-Random
↓ Failure rate (%)             | 0.07          | 0.04 (−42.9%)           | 0.01* (−85.7%)           | 0.02 (−71.4%)            | Push-Random
