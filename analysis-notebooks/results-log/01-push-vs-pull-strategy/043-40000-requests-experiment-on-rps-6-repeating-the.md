---
id: c01-043
date:
campaign: 01-push-vs-pull-strategy
series: "43"
exp_ids: [173, 174, 175, 176]
methods: []
status: migrated
source: logs
---
# Series 43 — 40000 requests experiment on rps 6, repeating the

Series 43:
Setup:
- 8 servers
- No cap on input tokens
- Request response length: 8192
- Total requests: 40000
- pattern: "det"
- rate_rps: 6
- Methods:
  173) Pull
  174) Push-RR
  175) Push-Random
  176) Push-Least-Queue

Purpose: 40000 requests experiment on rps 6, repeating the
Findings: Since the servers became over saturated then the queuing latency is now so high that the difference between methdos become marginal


Metric (mean / p99)            | Pull (exp173) | Push-RR (exp174)         | Push-Random (exp175)      | Push-LQ (exp176)          | Winner
-------------------------------|---------------|--------------------------|---------------------------|---------------------------|--------
↓ End-to-end latency mean (s)  | 3350.92*      | 3357.41 (+0.19%)         | 3356.91 (+0.18%)          | 3379.79 (+0.86%)          | Pull
↓ End-to-end latency p99 (s)   | 6685.72*      | 7016.50 (+4.95%)         | 6822.28 (+2.04%)          | 7098.09 (+6.17%)          | Pull

↑ Decode TPS mean              | 1750.09*      | 1676.98 (−4.18%)         | 1710.37 (−2.27%)          | 1679.74 (−4.02%)          | Pull
↑ Prefill TPS mean             | 247.96*       | 237.17 (−4.35%)          | 241.80 (−2.48%)           | 237.15 (−4.36%)           | Pull

↑ Requests running mean        | 62.01*        | 59.22 (−4.49%)           | 60.44 (−2.52%)            | 59.46 (−4.10%)            | Pull

↓ TTFT mean (s)                | 2.466         | 2.311* (−6.29%)          | 2.353 (−4.59%)            | 2.351 (−4.68%)            | Push-RR
↓ TPOT mean (s)                | 0.2835        | 0.2699* (−4.82%)         | 0.2756 (−2.81%)           | 0.2717 (−4.18%)           | Push-RR


Metric                         | Pull (exp173) | Push-RR (exp174)        | Push-Random (exp175)      | Push-LQ (exp176)         | Winner
-------------------------------|---------------|-------------------------|---------------------------|--------------------------|--------
↑ Successful requests          | 39813         | 39781 (−0.08%)          | 39845* (+0.08%)           | 39801 (−0.03%)           | Push-Random
↓ Connection failures          | 187           | 219 (+17.11%)           | 155* (−17.11%)            | 199 (+6.42%)             | Push-Random

↑ Success rate (%)             | 99.5325       | 99.4525 (−0.08%)        | 99.6125* (+0.08%)         | 99.5025 (−0.03%)         | Push-Random
↓ Failure rate (%)             | 0.4675        | 0.5475 (+17.11%)        | 0.3875* (−17.11%)         | 0.4975 (+6.42%)          | Push-Random
