---
id: c01-045
date:
campaign: 01-push-vs-pull-strategy
series: "45"
exp_ids: [181, 182, 183, 184]
methods: []
status: migrated
source: logs
---
# Series 45 — series 45

Series 45:
Setup:
- 24 servers
- No cap on input tokens
- Request response length: 8192
- Total requests: 40000
- pattern: "det"
- rate_rps: 8
- Methods:
  181) Pull
  182) Push-RR
  183) Push-Random
  184) Push-Least-Queue

Purpose: TODO
Findings: TODO


Metric (mean / p99)            | Pull (exp181) | Push-RR (exp182)        | Push-Random (exp183)     | Push-LQ (exp184)        | Winner
-------------------------------|---------------|--------------------------|---------------------------|--------------------------|--------
↓ End-to-end latency mean (s)  | 21.05         | 19.31 (−8.29%)          | 20.72 (−1.59%)           | 18.32* (−12.98%)        | Push-LQ
↓ End-to-end latency p99 (s)   | 254.07        | 231.12 (−9.04%)         | 216.44 (−14.82%)         | 220.14* (−13.35%)       | Push-Random

↑ Decode TPS mean              | 3516.20*      | 2526.03 (−28.16%)       | 2559.40 (−27.22%)        | 2626.61 (−25.30%)       | Pull
↑ Prefill TPS mean             | 498.39*       | 357.53 (−28.25%)        | 364.14 (−26.93%)         | 373.18 (−25.12%)        | Pull

↑ Requests running mean        | 116.51*       | 75.80 (−34.94%)         | 79.85 (−31.46%)          | 75.07 (−35.56%)         | Pull

↓ TTFT mean (s)                | 28.44         | 20.98 (−26.25%)         | 25.11 (−11.71%)          | 17.21* (−39.51%)        | Push-LQ
↓ TPOT mean (s)                | 0.8147        | 0.7156 (−12.17%)        | 0.7523 (−7.67%)          | 0.6770* (−16.90%)       | Push-LQ



Metric                         | Pull (exp181) | Push-RR (exp182)        | Push-Random (exp183)       | Push-LQ (exp184)          | Winner
-------------------------------|---------------|--------------------------|-----------------------------|----------------------------|--------
↑ Successful requests          | 38850*        | 37475 (−3.54%)          | 29892 (−23.06%)            | 19210 (−50.55%)           | Pull
↓ Connection failures          | 1150*         | 2525 (+119.57%)         | 10108 (+778.96%)           | 20790 (+1707.83%)         | Pull

↑ Success rate (%)             | 97.125*       | 93.6875 (−3.54%)        | 74.73 (−23.06%)            | 48.025 (−50.55%)          | Pull
↓ Failure rate (%)             | 2.875*        | 6.3125 (+119.57%)       | 25.27 (+778.96%)           | 51.975 (+1707.83%)        | Pull
