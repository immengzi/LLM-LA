---
id: c01-017
date:
campaign: 01-push-vs-pull-strategy
series: "17"
exp_ids: [69, 70, 71, 72]
methods: []
status: migrated
source: logs
---
# Series 17 — sanity check on 200 requests

Series 17:
Setup:
- 8 servers
- No cap on input tokens
- Request response length: 8192
- Total requests: 200
- pattern: "det"
- rate_rps: 3
- Methods:
  69) Pull
  70) Push-RR
  71) Push-Random
  72) Push-Least-Queue

Purpose: sanity check on 200 requests
Findings: good results but not as good 10000 experiment, our approach is much better for long running requests on almost saturation points


Metric (stat)               | Pull (exp69) | Push-RR (exp70)            | Push-Random (exp71)        | Push-LQ (exp72)            | Winner
----------------------------|--------------|----------------------------|----------------------------|----------------------------|--------
↓ Latency mean (s)          | 21.36*       | 22.06 (+3.3%)              | 22.56 (+5.6%)              | 29.52 (+38.2%)             | Pull
↓ Latency p99 (s)           | 102.99*      | 113.70 (+10.4%)            | 104.82 (+1.8%)             | 113.45 (+10.2%)            | Pull

↑ Decode TPS mean           | 412.96       | 412.90 (−0.0%)             | 411.56 (−0.3%)             | 415.74* (+0.7%)            | Push-LQ
↑ Prefill TPS mean          | 51.47        | 62.41* (+21.2%)            | 55.88 (+8.5%)              | 56.11 (+9.0%)              | Push-RR

↑ Requests running mean     | 12.91        | 13.02 (+0.9%)              | 13.17 (+2.1%)              | 13.22* (+2.5%)             | Push-LQ

↓ TTFT mean (s)             | 0.766        | 0.853 (+11.3%)             | 0.719* (−6.2%)             | 0.787 (+2.8%)              | Push-Random
↓ TTFT p99 (s)              | 3.349        | 3.927 (+17.3%)             | 2.891* (−13.7%)            | 4.513 (+34.8%)             | Push-Random

↓ TPOT mean (s)             | 0.122        | 0.135 (+10.3%)             | 0.128 (+4.7%)              | 0.121* (−1.3%)             | Push-LQ
↓ TPOT p99 (s)              | 0.405*       | 0.416 (+2.7%)              | 0.464 (+14.6%)             | 0.465 (+14.9%)             | Pull
