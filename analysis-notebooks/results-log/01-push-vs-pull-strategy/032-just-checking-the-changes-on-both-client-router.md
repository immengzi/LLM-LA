---
id: c01-032
date:
campaign: 01-push-vs-pull-strategy
series: "32"
exp_ids: [129, 130, 131, 132]
methods: []
status: migrated
source: logs
---
# Series 32 — just checking the changes on both client -> router, router -> sidecar

Series 32:
Setup:
- 8 servers
- No cap on input tokens
- Request response length: 8192
- Total requests: 1000
- pattern: "det"
- rate_rps: 6
- Methods:
  129) Pull
  130) Push-RR
  131) Push-Random
  132) Push-Least-Queue

Purpose: just checking the changes on both client -> router, router -> sidecar
Findings: no conclusion as it is a short experiment but also no lost requests so looks good

Metric (mean / p99)            | Pull (exp129) | Push-RR (exp130)        | Push-Random (exp131)     | Push-LQ (exp132)         | Winner
-------------------------------|---------------|-------------------------|--------------------------|--------------------------|--------
↓ End-to-end latency mean (s)  | 74.07*        | 78.61 (+6.1%)           | 79.16 (+6.9%)            | 78.08 (+5.4%)            | Pull
↓ End-to-end latency p99 (s)   | 240.07        | 226.67* (−5.6%)         | 258.53 (+7.7%)           | 224.93 (−6.3%)           | Push-LQ

↑ Decode TPS mean              | 1060.16       | 1075.55 (+1.5%)         | 1191.45* (+12.4%)        | 1075.84 (+1.5%)          | Push-Random
↑ Prefill TPS mean             | 151.84        | 154.57 (+1.8%)          | 171.23* (+12.8%)         | 154.64 (+1.8%)           | Push-Random

↑ Requests running mean        | 36.11         | 36.61 (+1.4%)           | 40.61* (+12.5%)          | 36.59 (+1.3%)            | Push-Random

↓ TTFT mean (s)                | 2.774         | 2.135 (−23.0%)          | 1.965* (−29.2%)          | 2.084 (−24.9%)           | Push-Random
↓ TPOT mean (s)                | 0.2169*       | 0.2193 (+1.1%)          | 0.2172 (+0.1%)           | 0.2193 (+1.1%)           | Pull


Metric                         | Pull (exp129) | Push-RR (exp130)        | Push-Random (exp131)     | Push-LQ (exp132)         | Winner
-------------------------------|---------------|--------------------------|---------------------------|---------------------------|--------
↑ Successful requests          | 998           | 996 (−0.2%)             | 1000* (+0.2%)            | 984 (−1.4%)              | Push-Random
↓ Connection failures          | 2             | 4 (+100.0%)             | 0* (−100.0%)             | 16 (+700.0%)             | Push-Random

↑ Success rate (%)             | 99.8          | 99.6 (−0.2%)            | 100.0* (+0.2%)           | 98.4 (−1.4%)             | Push-Random
↓ Failure rate (%)             | 0.2           | 0.4 (+100.0%)           | 0.0* (−100.0%)           | 1.6 (+700.0%)            | Push-Random
