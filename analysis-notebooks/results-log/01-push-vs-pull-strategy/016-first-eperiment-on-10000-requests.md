---
id: c01-016
date:
campaign: 01-push-vs-pull-strategy
series: "16"
exp_ids: [65, 66, 67, 68]
methods: []
status: migrated
source: logs
---
# Series 16 — First eperiment on 10000 requests

Series 16:
Setup:
- 8 servers
- No cap on input tokens
- Request response length: 8192
- Total requests: 10000
- pattern: "det"
- rate_rps: 3
- Methods:
  65) Pull
  66) Push-RR
  67) Push-Random
  68) Push-Least-Queue

Purpose: First eperiment on 10000 requests
Findings: very good resutls in terms of the tail and mean latency of the requests but many lost requests and post results latency problem

Metric (stat)               | Pull (exp65) | Push-RR (exp66)            | Push-Random (exp67)         | Push-LQ (exp68)            | Winner
----------------------------|--------------|----------------------------|-----------------------------|----------------------------|--------
↓ Latency mean (s)          | 78.77*       | 134.43 (+70.7%)            | 128.32 (+62.9%)             | 132.06 (+67.7%)            | Pull
↓ Latency p99 (s)           | 316.86       | 312.44 (−1.4%)             | 297.63* (−6.1%)             | 308.23 (−2.7%)             | Push-Random

↑ Decode TPS mean           | 1638.95*     | 1538.05 (−6.2%)            | 1557.76 (−5.0%)             | 1528.06 (−6.8%)            | Pull
↑ Prefill TPS mean          | 233.56*      | 220.32 (−5.7%)             | 227.23 (−2.7%)              | 217.12 (−7.0%)             | Pull

↑ Requests running mean     | 58.17*       | 54.35 (−6.6%)              | 55.11 (−5.3%)               | 53.77 (−7.6%)              | Pull

↓ TTFT mean (s)             | 0.754        | 0.761 (+0.9%)              | 0.739 (−2.0%)               | 0.723* (−4.1%)             | Push-LQ
↓ TTFT p99 (s)              | 3.579        | 3.538 (−1.1%)              | 3.455 (−3.5%)               | 3.425* (−4.3%)             | Push-LQ

↓ TPOT mean (s)             | 0.293        | 0.288 (−1.8%)              | 0.289 (−1.3%)               | 0.280* (−4.2%)             | Push-LQ
↓ TPOT p99 (s)              | 0.743        | 0.739 (−0.6%)              | 0.807 (+8.6%)               | 0.699* (−5.9%)             | Push-LQ


Metric                         | Pull (exp65) | Push-RR (exp66)        | Push-Random (exp67)     | Push-LQ (exp68)         | Winner
-------------------------------|--------------|------------------------|-------------------------|-------------------------|--------
↑ Successful requests          | 9997*        | 9309 (−6.88%)          | 9619 (−3.78%)           | 9409 (−5.88%)           | Pull
↓ Connection failures          | 3*           | 691 (+22 933%)         | 381 (+12 600%)          | 591 (+19 600%)          | Pull

↑ Success rate (%)             | 99.97*       | 93.09 (−6.88%)         | 96.19 (−3.78%)          | 94.09 (−5.88%)          | Pull
↓ Failure rate (%)             | 0.03*        | 6.91 (+22 933%)        | 3.81 (+12 600%)         | 5.91 (+19 600%)         | Pull
