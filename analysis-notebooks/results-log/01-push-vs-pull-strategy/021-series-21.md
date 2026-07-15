---
id: c01-021
date:
campaign: 01-push-vs-pull-strategy
series: "21"
exp_ids: [85, 86, 87, 88]
methods: []
status: migrated
source: logs
---
# Series 21 — series 21

Series 21:
Setup:
- 8 servers
- No cap on input tokens
- Request response length: 8192
- Total requests: 10000
- pattern: "det"
- rate_rps: 3
- Methods:
  85) Pull
  86) Push-RR
  87) Push-Random
  88) Push-Least-Queue


Metric                         | Pull (exp85) | Push-RR (exp86)        | Push-Random (exp87)     | Push-LQ (exp88)         | Winner
-------------------------------|--------------|------------------------|-------------------------|-------------------------|--------
↓ End-to-end latency mean (s)  | 63.10*       | 116.51 (+84.7%)        | 112.35 (+78.1%)         | 115.69 (+83.4%)         | Pull
↓ End-to-end latency p99 (s)   | 312.27*      | 422.79 (+35.4%)        | 474.91 (+52.1%)         | 435.63 (+39.5%)         | Pull

↑ Decode TPS mean              | 1638.37*     | 1570.51 (−4.1%)        | 1497.68 (−8.6%)         | 1546.91 (−5.6%)         | Pull
↑ Prefill TPS mean             | 233.25*      | 223.19 (−4.3%)         | 213.15 (−8.6%)          | 218.60 (−6.3%)          | Pull

↑ Requests running mean        | 58.19*       | 55.45 (−4.7%)          | 52.84 (−9.2%)           | 54.77 (−5.9%)           | Pull

↓ TTFT mean (s)                | 0.772        | 0.753                  | 0.725*                  | 0.764                   | Push-Random
↓ TPOT mean (s)                | 0.292        | 0.285                  | 0.269*                  | 0.281                   | Push-Random


Metric                         | Pull (exp85) | Push-RR (exp86)        | Push-Random (exp87)     | Push-LQ (exp88)         | Winner
-------------------------------|--------------|------------------------|-------------------------|-------------------------|--------
↑ Successful requests          | 9997         | 9961 (−0.36%)          | 9985 (−0.12%)           | 9950 (−0.47%)           | Pull
↓ Connection failures          | 3*           | 39 (+1200.0%)          | 15 (+400.0%)            | 50 (+1566.7%)           | Pull

↑ Success rate (%)             | 99.97*       | 99.61 (−0.36%)         | 99.85 (−0.12%)          | 99.50 (−0.47%)          | Pull
↓ Failure rate (%)             | 0.03*        | 0.39 (+1200.0%)        | 0.15 (+400.0%)          | 0.50 (+1566.7%)         | Pull
