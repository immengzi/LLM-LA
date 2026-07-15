---
id: c01-018
date:
campaign: 01-push-vs-pull-strategy
series: "18"
exp_ids: [73, 74, 75, 76]
methods: []
status: migrated
source: logs
---
# Series 18 — sanity check on 200 requests with dump

Series 18:
Setup:
- 8 servers
- No cap on input tokens
- Request response length: 8192
- Total requests: 200
- pattern: "dump"
- rate_rps: 3
- Methods:
  73) Pull
  74) Push-RR
  75) Push-Random
  76) Push-Least-Queue

Purpose: sanity check on 200 requests with dump
Findings: good results but not as good 10000 experiment, our approach is much better for long running requests on almost saturation points

Metric (stat)               | Pull (exp73) | Push-RR (exp74)            | Push-Random (exp75)        | Push-LQ (exp76)            | Winner
----------------------------|--------------|----------------------------|----------------------------|----------------------------|--------
↓ Latency mean (s)          | 41.57        | 63.58 (+53.0%)             | 47.26 (+13.7%)             | 40.97* (−1.5%)             | Push-LQ
↓ Latency p99 (s)           | 140.67       | 161.98 (+15.1%)            | 139.05* (−1.2%)            | 142.12 (+1.0%)             | Push-Random

↑ Decode TPS mean           | 455.77*      | 398.69 (−12.5%)            | 437.23 (−4.1%)             | 438.18 (−3.9%)             | Pull
↑ Prefill TPS mean          | 65.31        | 55.52 (−15.0%)             | 61.13 (−6.4%)              | 65.72* (+0.6%)             | Push-LQ

↑ Requests running mean     | 14.84*       | 13.00 (−12.4%)             | 14.29 (−3.7%)              | 14.54 (−2.0%)              | Pull

↓ TTFT mean (s)             | 1.877        | 1.074* (−42.8%)            | 1.276 (−32.0%)             | 1.801 (−4.0%)              | Push-RR
↓ TTFT p99 (s)              | 14.70        | 3.90* (−73.5%)             | 9.35 (−36.4%)              | 12.58 (−14.4%)             | Push-RR

↓ TPOT mean (s)             | 0.133        | 0.108* (−18.7%)            | 0.133 (−0.2%)              | 0.134 (+1.1%)              | Push-RR
↓ TPOT p99 (s)              | 0.460        | 0.376 (−18.2%)             | 0.365 (−20.6%)             | 0.718 (+56.2%)             | Push-Random
