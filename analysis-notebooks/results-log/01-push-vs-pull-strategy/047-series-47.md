---
id: c01-047
date:
campaign: 01-push-vs-pull-strategy
series: "47"
exp_ids: [189, 190, 191, 192]
methods: []
status: migrated
source: logs
---
# Series 47 — series 47

Series 47:
Setup:
- 24 servers
- No cap on input tokens
- Request response length: 8192
- Total requests: 40000
- pattern: "det"
- rate_rps: 12
- Methods:
  189) Pull
  190) Push-RR
  191) Push-Random
  192) Push-Least-Queue

Purpose: TODO
Findings: TODO


Metric (mean / p99)            | Pull (exp189) | Push-RR (exp190)        | Push-Random (exp191)      | Push-LQ (exp192)         | Winner
-------------------------------|---------------|--------------------------|---------------------------|---------------------------|--------
↓ End-to-end latency mean (s)  | 21.12         | 20.00 (−5.31%)           | 21.10 (−0.11%)            | 18.93* (−10.37%)         | Push-LQ
↓ End-to-end latency p99 (s)   | 254.58        | 231.64* (−9.01%)         | 239.12 (−6.07%)           | 231.17 (−9.19%)          | Push-LQ

↑ Decode TPS mean              | 3481.91*      | 2537.18 (−27.13%)        | 2605.84 (−25.16%)         | 2586.39 (−25.72%)        | Pull
↑ Prefill TPS mean             | 493.84*       | 359.35 (−27.23%)         | 369.23 (−25.23%)          | 364.88 (−26.11%)         | Pull

↑ Requests running mean        | 116.01*       | 78.43 (−32.39%)          | 82.02 (−29.30%)           | 76.00 (−34.49%)          | Pull

↓ TTFT mean (s)                | 27.97         | 22.93 (−18.01%)          | 25.60 (−8.47%)            | 18.90* (−32.44%)         | Push-LQ
↓ TPOT mean (s)                | 0.8120        | 0.7441 (−8.37%)          | 0.7424 (−8.58%)           | 0.6928* (−14.67%)        | Push-LQ


Metric                         | Pull (exp189) | Push-RR (exp190)        | Push-Random (exp191)     | Push-LQ (exp192)         | Winner
-------------------------------|---------------|--------------------------|---------------------------|---------------------------|--------
↑ Successful requests          | 38911*        | 37063 (−4.75%)           | 37002 (−4.91%)            | 36502 (−6.19%)            | Pull
↓ Connection failures          | 1089*         | 2937 (+169.70%)          | 2998 (+175.32%)           | 3498 (+221.31%)           | Pull

↑ Success rate (%)             | 97.2775*      | 92.6575 (−4.75%)         | 92.5050 (−4.91%)          | 91.2550 (−6.19%)          | Pull
↓ Failure rate (%)             | 2.7225*       | 7.3425 (+169.70%)        | 7.4950 (+175.32%)         | 8.7450 (+221.31%)         | Pull
