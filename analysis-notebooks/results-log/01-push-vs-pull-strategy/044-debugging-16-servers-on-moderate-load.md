---
id: c01-044
date:
campaign: 01-push-vs-pull-strategy
series: "44"
exp_ids: [177, 178, 179, 180]
methods: []
status: migrated
source: logs
---
# Series 44 — debugging 16 servers on moderate load

Series 44:
Setup:
- 16 servers
- No cap on input tokens
- Request response length: 8192
- Total requests: 600
- pattern: "det"
- rate_rps: 9
- Methods:
  177) Pull
  178) Push-RR
  179) Push-Random
  180) Push-Least-Queue

Purpose: debugging 16 servers on moderate load
Findings: all good and seems working well


Metric (mean / p99)            | Pull (exp177) | Push-RR (exp178)        | Push-Random (exp179)     | Push-LQ (exp180)        | Winner
-------------------------------|---------------|-------------------------|--------------------------|-------------------------|--------
↓ End-to-end latency mean (s)  | 38.49         | 36.34* (−5.60%)         | 36.95 (−4.01%)           | 39.57 (+2.79%)          | Push-RR
↓ End-to-end latency p99 (s)   | 130.60        | 126.10* (−3.45%)        | 131.89 (+0.99%)          | 145.51 (+11.42%)        | Push-RR

↑ Decode TPS mean              | 1305.38       | 1338.76* (+2.56%)       | 1289.94 (−1.18%)         | 1272.36 (−2.53%)        | Push-RR
↑ Prefill TPS mean             | 183.55        | 189.32* (+3.15%)        | 182.30 (−0.68%)          | 179.92 (−1.98%)         | Push-RR

↑ Requests running mean        | 50.10         | 50.37* (+0.55%)         | 46.83 (−6.52%)           | 49.92 (−0.37%)          | Push-RR

↓ TTFT mean (s)                | 34.99         | 25.27* (−27.78%)        | 26.35 (−24.69%)          | 27.13 (−22.45%)         | Push-RR
↓ TPOT mean (s)                | 0.4715        | 0.4657 (−1.22%)         | 0.4209* (−10.72%)        | 0.4792 (+1.64%)         | Push-Random


Metric                         | Pull (exp177) | Push-RR (exp178)        | Push-Random (exp179)     | Push-LQ (exp180)        | Winner
-------------------------------|---------------|-------------------------|--------------------------|-------------------------|--------
↑ Successful requests          | 596*          | 578 (−3.02%)            | 583 (−2.18%)             | 564 (−5.37%)            | Pull
↓ Connection failures          | 4*            | 22 (+450.00%)           | 17 (+325.00%)            | 36 (+800.00%)           | Pull

↑ Success rate (%)             | 99.33*        | 96.33 (−3.02%)          | 97.17 (−2.18%)           | 94.00 (−5.37%)          | Pull
↓ Failure rate (%)             | 0.67*         | 3.67 (+450.00%)         | 2.83 (+325.00%)          | 6.00 (+800.00%)         | Pull
