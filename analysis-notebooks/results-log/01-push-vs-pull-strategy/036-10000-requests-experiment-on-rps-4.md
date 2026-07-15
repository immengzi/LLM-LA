---
id: c01-036
date:
campaign: 01-push-vs-pull-strategy
series: "36"
exp_ids: [145, 146, 147, 148]
methods: []
status: migrated
source: logs
---
# Series 36 — 10000 requests experiment on rps 4

Series 36:
Setup:
- 8 servers
- No cap on input tokens
- Request response length: 8192
- Total requests: 10000
- pattern: "det"
- rate_rps: 4
- Methods:
  145) Pull
  146) Push-RR
  147) Push-Random
  148) Push-Least-Queue

Purpose: 10000 requests experiment on rps 4
Findings: After 3 we seem to starrt getting diminishing return. Almost no improvement in mean and lower improvement in p99 start to appear compared to rps 3

Metric (mean / p99)            | Pull (exp145) | Push-RR (exp146)        | Push-Random (exp147)     | Push-LQ (exp148)         | Winner
-------------------------------|---------------|-------------------------|-------------------------|------------------------|--------
↓ End-to-end latency mean (s)  | 423.45*       | 434.98 (+2.7%)          | 430.99 (+1.8%)           | 431.84 (+2.0%)           | Pull
↓ End-to-end latency p99 (s)   | 856.58*       | 1114.19 (+30.1%)        | 1025.67 (+19.8%)         | 1123.86 (+31.2%)         | Pull

↑ Decode TPS mean              | 1678.16*      | 1546.96 (−7.8%)         | 1574.67 (−6.2%)          | 1545.19 (−7.9%)          | Pull
↑ Prefill TPS mean             | 238.71*       | 219.26 (−8.1%)          | 223.38 (−6.4%)           | 219.08 (−8.2%)           | Pull

↑ Requests running mean        | 59.15*        | 54.55 (−7.8%)           | 55.54 (−6.1%)            | 54.44 (−8.0%)            | Pull

↓ TTFT mean (s)                | 2.364         | 2.200* (−7.0%)          | 2.205 (−6.7%)            | 2.179 (−7.8%)            | Push-LQ
↓ TPOT mean (s)                | 0.2744        | 0.2551 (−7.0%)          | 0.2609 (−5.0%)           | 0.2542* (−7.4%)          | Push-LQ


Metric                         | Pull (exp145) | Push-RR (exp146)        | Push-Random (exp147)     | Push-LQ (exp148)         | Winner
-------------------------------|---------------|--------------------------|---------------------------|---------------------------|--------
↑ Successful requests          | 9986*         | 9953 (−0.33%)           | 9964 (−0.22%)            | 9967 (−0.19%)            | Pull
↓ Connection failures          | 14*           | 47 (+235.7%)            | 36 (+157.1%)             | 33 (+135.7%)             | Pull

↑ Success rate (%)             | 99.86*        | 99.53 (−0.33%)          | 99.64 (−0.22%)           | 99.67 (−0.19%)           | Pull
↓ Failure rate (%)             | 0.14*         | 0.47 (+235.7%)          | 0.36 (+157.1%)           | 0.33 (+135.7%)           | Pull
