---
id: c01-037
date:
campaign: 01-push-vs-pull-strategy
series: "37"
exp_ids: [149, 150, 151, 152]
methods: []
status: migrated
source: logs
---
# Series 37 — 10000 requests experiment on rps 5

Series 37:
Setup:
- 8 servers
- No cap on input tokens
- Request response length: 8192
- Total requests: 10000
- pattern: "det"
- rate_rps: 5
- Methods:
  149) Pull
  150) Push-RR
  151) Push-Random
  152) Push-Least-Queue

Purpose: 10000 requests experiment on rps 5
Findings: After 3 we seem to starrt getting diminishing return. Almost no improvement in mean and lower improvement in p99 start to appear compared to rps both 3 and 4


Metric (mean / p99)            | Pull (exp149) | Push-RR (exp150)        | Push-Random (exp151)     | Push-LQ (exp152)         | Winner
-------------------------------|---------------|-------------------------|--------------------------|--------------------------|--------
↓ End-to-end latency mean (s)  | 661.45*       | 668.01 (+1.0%)          | 684.86 (+3.5%)           | 677.38 (+2.4%)           | Pull
↓ End-to-end latency p99 (s)   | 1322.67*      | 1566.24 (+18.4%)        | 1469.76 (+11.1%)         | 1582.42 (+19.6%)         | Pull

↑ Decode TPS mean              | 1692.55*      | 1555.18 (−8.1%)         | 1660.88 (−1.9%)          | 1546.01 (−8.7%)          | Pull
↑ Prefill TPS mean             | 240.21*       | 220.46 (−8.2%)          | 235.76 (−1.9%)           | 219.36 (−8.7%)           | Pull

↑ Requests running mean        | 59.46*        | 54.59 (−8.2%)           | 58.58 (−1.5%)            | 54.52 (−8.3%)            | Pull

↓ TTFT mean (s)                | 2.320         | 2.112* (−9.0%)          | 2.248 (−3.1%)            | 2.192 (−5.5%)            | Push-RR
↓ TPOT mean (s)                | 0.2757        | 0.2535* (−8.1%)         | 0.2723 (−1.2%)           | 0.2545 (−7.7%)           | Push-RR


Metric                         | Pull (exp149) | Push-RR (exp150)        | Push-Random (exp151)     | Push-LQ (exp152)         | Winner
-------------------------------|---------------|-------------------------|--------------------------|--------------------------|--------
↑ Successful requests          | 9959*         | 9956 (−0.03%)           | 9955 (−0.04%)            | 9946 (−0.13%)            | Pull
↓ Connection failures          | 41*           | 44 (+7.3%)              | 45 (+9.8%)               | 54 (+31.7%)              | Pull

↑ Success rate (%)             | 99.59*        | 99.56 (−0.03%)          | 99.55 (−0.04%)           | 99.46 (−0.13%)           | Pull
↓ Failure rate (%)             | 0.41*         | 0.44 (+7.3%)            | 0.45 (+9.8%)             | 0.54 (+31.7%)            | Pull
