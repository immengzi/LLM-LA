---
id: c01-013
date:
campaign: 01-push-vs-pull-strategy
series: "13"
exp_ids: [53, 54, 55, 56]
methods: []
status: migrated
source: logs
---
# Series 13 — check the pull method under none-capped input and capped 8192 output token on rps 4, repeat of 7 with 10000 requests

Series 13:
Setup:
- 8 servers
- No cap on input tokens
- Request response length: 8192
- Total requests: 10000
- pattern: "det"
- rate_rps: 4
- Methods:
  53) Pull
  54) Push-RR
  55) Push-Random
  56) Push-Least-Queue

Purpose: check the pull method under none-capped input and capped 8192 output token on rps 4, repeat of 7 with 10000 requests
Findings: pull is better but as there are many lost requests no conclusion can be infered.

Metric (stat)               | Pull (exp53) | Push-RR (exp54)            | Push-Random (exp55)        | Push-LQ (exp56)           | Winner
----------------------------|--------------|----------------------------|----------------------------|---------------------------|--------
↓ Latency mean (s)          | 146.76       | 144.54* (+1.5%)            | 163.20 (−11.2%)            | 159.16 (−8.4%)            | Push-RR
↓ Latency p99 (s)           | 325.49*      | 390.38 (−19.9%)            | 329.49 (−1.2%)             | 332.61 (−2.2%)            | Pull

↑ Decode TPS mean           | 1469.13      | 1623.93* (+10.5%)          | 1467.00 (−0.1%)            | 1452.07 (−1.2%)           | Push-RR
↑ Prefill TPS mean          | 214.24       | 243.63* (+13.7%)           | 218.01 (+1.8%)             | 216.44 (+1.0%)            | Push-RR

↑ Requests running mean     | 51.68        | 57.66* (+11.6%)            | 52.04 (+0.7%)              | 51.20 (−0.9%)             | Push-RR

↓ TTFT mean (s)             | 0.738*       | 0.810 (−9.8%)              | 0.789 (−6.9%)              | 0.781 (−5.8%)             | Pull
↓ TTFT p99 (s)              | 3.541        | 3.504* (+1.0%)             | 4.044 (−14.2%)             | 3.580 (−1.1%)             | Push-RR

↓ TPOT mean (s)             | 0.278        | 0.292 (−5.0%)              | 0.271 (＋2.6%)             | 0.264* (+5.0%)            | Push-LQ
↓ TPOT p99 (s)              | 0.862        | 0.828 (+3.9%)              | 0.754 (+12.5%)             | 0.735* (+14.7%)           | Push-LQ


Metric                         | Pull (exp53) | Push-RR (exp54)        | Push-Random (exp55)     | Push-LQ (exp56)         | Winner
-------------------------------|--------------|------------------------|-------------------------|-------------------------|--------
↑ Successful requests          | 9163*        | 7981 (−12.9%)          | 7362 (−19.7%)           | 7502 (−18.1%)           | Pull
↓ Connection failures          | 837*         | 2019 (+141.3%)         | 2638 (+215.2%)          | 2498 (+198.5%)          | Pull

↑ Success rate (%)             | 91.63*       | 79.81 (−12.9%)         | 73.62 (−19.7%)          | 75.02 (−18.1%)          | Pull
↓ Failure rate (%)             | 8.37*        | 20.19 (+141.3%)        | 26.38 (+215.2%)         | 24.98 (+198.5%)         | Pull
