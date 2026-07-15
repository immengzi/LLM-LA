---
id: c01-022
date:
campaign: 01-push-vs-pull-strategy
series: "22"
exp_ids: [89, 90, 91, 92]
methods: []
status: migrated
source: logs
---
# Series 22 — series 22

Series 22:
Setup:
- 8 servers
- No cap on input tokens
- Request response length: 8192
- Total requests: 25000
- pattern: "det"
- rate_rps: 3
- Methods:
  89) Pull
  90) Push-RR
  91) Push-Random
  92) Push-Least-Queue


Metric (mean / p99)            | Pull (exp89) | Push-RR (exp90)        | Push-Random (exp91)     | Push-LQ (exp92)         | Winner
-------------------------------|--------------|------------------------|-------------------------|-------------------------|--------
↓ End-to-end latency mean (s)  | 64.33*       | 142.28 (+121.1%)       | 146.66 (+128.0%)        | 144.20 (+124.2%)        | Pull
↓ End-to-end latency p99 (s)   | 315.58*      | 572.78 (+81.5%)        | 463.62 (+46.9%)         | 513.71 (+62.8%)         | Pull

↑ Decode TPS mean              | 1706.42*     | 1603.73 (−6.0%)        | 1638.77 (−3.9%)         | 1637.88 (−4.0%)         | Pull
↑ Prefill TPS mean             | 241.58*      | 228.91 (−5.2%)         | 233.19 (−3.5%)          | 231.53 (−4.1%)          | Pull

↑ Requests running mean        | 60.72*       | 56.82 (−6.4%)          | 58.35 (−3.9%)           | 58.20 (−4.1%)           | Pull

↓ TTFT mean (s)                | 0.767*       | 0.761 (−0.8%)          | 0.785 (+2.3%)           | 0.777 (+1.3%)           | Push-RR
↓ TPOT mean (s)                | 0.2967       | 0.2871*                | 0.2968 (+3.4%)          | 0.2946 (+2.6%)          | Push-RR



Metric                         | Pull (exp89) | Push-RR (exp90)        | Push-Random (exp91)     | Push-LQ (exp92)         | Winner
-------------------------------|--------------|------------------------|-------------------------|-------------------------|--------
↑ Successful requests          | 24995*       | 24564 (−1.72%)         | 24488 (−2.03%)          | 24554 (−1.76%)          | Pull
↓ Connection failures          | 5*           | 436 (+8620.0%)         | 512 (+10140.0%)         | 446 (+8820.0%)          | Pull

↑ Success rate (%)             | 99.98*       | 98.26 (−1.72%)         | 97.95 (−2.03%)          | 98.22 (−1.76%)          | Pull
↓ Failure rate (%)             | 0.02*        | 1.74 (+8620.0%)        | 2.05 (+10140.0%)        | 1.78 (+8820.0%)         | Pull
