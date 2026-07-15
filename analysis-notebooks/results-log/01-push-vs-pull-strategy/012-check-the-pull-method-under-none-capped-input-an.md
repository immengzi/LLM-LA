---
id: c01-012
date:
campaign: 01-push-vs-pull-strategy
series: "12"
exp_ids: [49, 50, 51, 52]
methods: []
status: migrated
source: logs
---
# Series 12 — check the pull method under none-capped input and output token (natural prefill and decode scenario) on rps 3 repeat of series 6  and 10

Series 12:
Setup:
- 8 servers
- No cap on input tokens
- Request response length: 8192
- Total requests: 10000
- pattern: "det"
- rate_rps: 3
- Methods:
  49) Pull
  50) Push-RR
  51) Push-Random
  52) Push-Least-Queue

Purpose: check the pull method under none-capped input and output token (natural prefill and decode scenario) on rps 3 repeat of series 6  and 10
Findings: pull is better but as there are many lost requests no conclusion can be infered

Metric (stat)               | Pull (exp49) | Push-RR (exp50)          | Push-Random (exp51)      | Push-LQ (exp52)          | Winner
----------------------------|--------------|--------------------------|--------------------------|--------------------------|--------
↓ Latency mean (s)          | 85.16*       | 134.95 (+58.5%)          | 143.83 (+68.9%)          | 146.76 (+72.3%)          | Pull
↓ Latency p99 (s)           | 325.01*      | 308.98 (−4.9%)           | 342.80 (+5.5%)           | 325.49 (+0.1%)           | Pull

↑ Decode TPS mean           | 1639.70*     | 1498.45 (−8.6%)          | 1483.62 (−9.5%)          | 1469.13 (−10.4%)         | Pull
↑ Prefill TPS mean          | 231.53*      | 216.15 (−6.6%)           | 212.96 (−8.0%)           | 214.24 (−7.5%)           | Pull

↑ Requests running mean     | 58.33*       | 52.67 (−9.7%)            | 52.25 (−10.4%)           | 51.68 (−11.4%)           | Pull

↓ TTFT mean (s)             | 0.772        | 0.750 (−2.8%)            | 0.720* (−6.7%)           | 0.738 (−4.4%)            | Push-Random
↓ TTFT p99 (s)              | 3.68         | 3.41 (−7.3%)             | 3.34* (−9.1%)            | 3.54 (−3.7%)             | Push-Random

↓ TPOT mean (s)             | 0.295        | 0.279 (−5.3%)            | 0.274* (−7.2%)           | 0.278 (−5.8%)            | Push-Random
↓ TPOT p99 (s)              | 0.776        | 0.729 (−6.0%)            | 0.678* (−12.6%)          | 0.862 (+11.1%)           | Push-Random


Metric                         | Pull (exp49) | Push-RR (exp50)        | Push-Random (exp51)     | Push-LQ (exp52)         | Winner
-------------------------------|--------------|------------------------|-------------------------|-------------------------|--------
↑ Successful requests          | 9997*        | 9343 (−6.5%)           | 9078 (−9.2%)            | 9163 (−8.3%)            | Pull
↓ Connection failures          | 3*           | 657 (+21,800%)         | 922 (+30,633%)          | 837 (+27,800%)          | Pull

↑ Success rate (%)             | 99.97*       | 93.43 (−6.5%)          | 90.78 (−9.2%)           | 91.63 (−8.3%)           | Pull
↓ Failure rate (%)             | 0.03*        | 6.57 (+21,800%)        | 9.22 (+30,633%)         | 8.37 (+27,800%)         | Pull
