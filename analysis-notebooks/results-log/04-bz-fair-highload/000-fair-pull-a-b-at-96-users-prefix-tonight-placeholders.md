---
id: c04-000
date: 2026-07-15
campaign: 04-bz-fair-highload
series: 1-2
exp_ids: []
methods: [pull]
status: draft
---

# Series 1-2 — fair pull A/B at 96 users prefix (tonight placeholders)

## Notebook
[`claude-strategy-comparison.ipynb`](../../claude-strategy-comparison.ipynb) — section **BZ tonight fair high-load** (fill exp ids after runs).

## Setup
- servers: bz (this cluster)
- model: MiniMax-M2.7-w8a8-QuaRot / LMCache P2P host-staging
- backend: boom + LLM-LA router (pull)
- dataset: CodeFlowBench / claude CLI transport, 96 users × 10 convs
- total_requests: 960 (informational; users model controls count)
- rate_rps: n/a (closed-loop users)
- methods:
  - Series 1: `benchmark-bz-pull-prefix-highload` → fair OFF, `router_strategy=prefix`
  - Series 2: `benchmark-bz-pull-fair-prefix-highload` → fair ON (`router_fair_pull`, margin 1.15)

## Purpose
Stress rebalancing under overload (~1.5× capacity: 96 users vs 64-seq pod cap) and compare fair-off vs fair-on on the same prefix workload.

## Findings
(pending run)

## Observations

| Metric | Series 1 (fair OFF) | Series 2 (fair ON) | Winner |
|---|---|---|---|
| exp_id | TBD | TBD | |
| req Jain / load balance |  |  | fair should raise toward 1.0 |
| avg_requests_waiting |  |  | |
| TTFT p50 / p99 |  |  | |
| TPOT p50 / p99 |  |  | |
| e2e p50 / p99 |  |  | |
| timeout / fail rate |  |  | 96-user collapse historically |
| KV hit rate |  |  | |
| stickiness |  |  | |
