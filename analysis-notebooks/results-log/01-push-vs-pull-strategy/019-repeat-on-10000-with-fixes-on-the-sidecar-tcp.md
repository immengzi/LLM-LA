---
id: c01-019
date:
campaign: 01-push-vs-pull-strategy
series: "19"
exp_ids: [77, 78, 79, 80]
methods: []
status: migrated
source: logs
---
# Series 19 — repeat on 10000 with fixes on the sidecar tcp

Series 19:
Setup:
- 8 servers
- No cap on input tokens
- Request response length: 8192
- Total requests: 10000
- pattern: "det"
- rate_rps: 3
- Methods:
  77) Pull
  78) Push-RR
  79) Push-Random
  80) Push-Least-Queue

Purpose: repeat on 10000 with fixes on the sidecar tcp
Findings: good results but still many lost requests

Metric (stat)               | Pull (exp77) | Push-RR (exp78)            | Push-Random (exp79)        | Push-LQ (exp80)            | Winner
----------------------------|--------------|----------------------------|----------------------------|----------------------------|--------
Latency mean (s)            | 85.93*       | 133.43 (+55.3%)            | 132.04 (+53.7%)            | 154.65 (+80.0%)            | Pull
Latency p99 (s)             | 327.77       | 332.07 (+1.3%)             | 311.28* (−5.0%)            | 316.88 (−3.3%)             | Push-Random

Decode TPS mean              | 1619.99*     | 1461.86 (−9.8%)            | 1541.20 (−4.9%)            | 1465.80 (−9.5%)            | Pull
Prefill TPS mean             | 231.94*      | 213.43 (−8.0%)             | 227.28 (−2.0%)             | 218.92 (−5.6%)             | Pull

Requests running mean        | 58.16*       | 51.79 (−11.0%)             | 54.74 (−5.9%)              | 51.55 (−11.4%)             | Pull

TTFT mean (s)                | 0.813        | 0.763* (−6.2%)             | 0.794 (−2.4%)              | 0.770 (−5.3%)              | Push-RR
TTFT p99 (s)                 | 3.72        | 3.62 (−2.8%)               | 3.52* (−5.4%)              | 3.48 (−6.6%)               | Push-LQ

TPOT mean (s)                | 0.298        | 0.275* (−7.9%)             | 0.289 (−3.0%)              | 0.282 (−5.5%)              | Push-RR
TPOT p99 (s)                 | 0.849        | 0.698* (−17.8%)            | 0.779 (−8.2%)              | 0.805 (−5.2%)              | Push-RR



Metric                         | Pull (exp77) | Push-RR (exp78)        | Push-Random (exp79)     | Push-LQ (exp80)         | Winner
-------------------------------|--------------|------------------------|-------------------------|-------------------------|--------
↑ Successful requests          | 9997*        | 9161 (−8.36%)          | 9385 (−6.12%)           | 8839 (−11.58%)          | Pull
↓ Connection failures          | 3*           | 839 (+27 867%)         | 615 (+20 400%)          | 1161 (+38 600%)         | Pull

↑ Success rate (%)             | 99.97*       | 91.61 (−8.36%)         | 93.85 (−6.12%)          | 88.39 (−11.58%)         | Pull
↓ Failure rate (%)             | 0.03*        | 8.39 (+27 867%)        | 6.15 (+20 400%)         | 11.61 (+38 600%)        | Pull
