---
id: c01-046
date:
campaign: 01-push-vs-pull-strategy
series: "46"
exp_ids: [185, 186, 187, 188]
methods: []
status: migrated
source: logs
---
# Series 46 — series 46

Series 46:
Setup:
- 24 servers
- No cap on input tokens
- Request response length: 8192
- Total requests: 40000
- pattern: "det"
- rate_rps: 10
- Methods:
  185) Pull
  186) Push-RR
  187) Push-Random
  188) Push-Least-Queue

Purpose: TODO
Findings: TODO


Metric (mean / p99)            | Pull (exp185) | Push-RR (exp186)         | Push-Random (exp187)      | Push-LQ (exp188)          | Winner
-------------------------------|---------------|---------------------------|----------------------------|----------------------------|--------
↓ End-to-end latency mean (s)  | 21.37         | 19.28* (−9.76%)          | 17.80* (−16.69%)          | 19.41 (−9.16%)            | Push-Random
↓ End-to-end latency p99 (s)   | 253.38        | 231.69 (−8.56%)          | 107.55* (−57.55%)         | 229.54 (−9.41%)           | Push-Random

↑ Decode TPS mean              | 3609.08*      | 2547.98 (−29.40%)        | 2496.03 (−30.83%)         | 2622.45 (−27.33%)         | Pull
↑ Prefill TPS mean             | 509.93*       | 360.81 (−29.25%)         | 353.98 (−30.60%)          | 372.55 (−26.94%)          | Pull

↑ Requests running mean        | 121.46*       | 76.77 (−36.80%)          | 78.32 (−35.53%)           | 79.04 (−34.92%)           | Pull

↓ TTFT mean (s)                | 29.56         | 21.40 (−27.61%)          | 26.11 (−11.67%)           | 20.21* (−31.64%)          | Push-LQ
↓ TPOT mean (s)                | 0.8215        | 0.7107 (−13.49%)         | 0.7556 (−8.02%)           | 0.7174* (−12.68%)         | Push-RR


Metric                         | Pull (exp185) | Push-RR (exp186)        | Push-Random (exp187)        | Push-LQ (exp188)         | Winner
-------------------------------|---------------|--------------------------|-----------------------------|---------------------------|--------
↑ Successful requests          | 38576*        | 36790 (−4.63%)           | 1035 (−97.32%)              | 36869 (−4.42%)           | Pull
↓ Connection failures          | 1424*         | 3210 (+125.42%)          | 38965 (+2636.87%)           | 3131 (+119.88%)          | Pull

↑ Success rate (%)             | 96.44*        | 91.98 (−4.63%)           | 2.59 (−97.32%)              | 92.17 (−4.42%)           | Pull
↓ Failure rate (%)             | 3.56*         | 8.03 (+125.42%)          | 97.41 (+2636.87%)           | 7.83 (+119.88%)          | Pull
