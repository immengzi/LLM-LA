---
id: c05-002
date: 2026-07-27
campaign: 05-bz-claude-mine-vs-boom
series: u64
exp_ids: [139, 140, 141, 142, 143, 144, 145]
methods: [us-both-hard, us-aff-hard, us-aff-soft, us-prefix, boom-rr, boom-key_affinity, boom-kvc]
status: done
---
# Series u64 — medium load (64 users): LLM-LA (mine) vs BooM

Notebook: `analysis-notebooks/u64-mine-vs-boom.ipynb`

## Setup
- servers: bz (192.168.0.79); MiniMax-M2, 2 leader pods
- backend: mine = us + LLM-LA router (`pull`); boom = BooM direct (`directRoutingStrategy`)
- dataset: CodeFlowBench; transport: real Claude Code CLI (`transport.mode=claude`)
- KV transport: LMCache P2P host-staging; `strip_claude_code_attribution`: ON (all cells)
- total_requests: 64 users x 2 convs = 128 conversations (same replay); per-turn timeout 10000s
- rate_rps: closed-loop (64 concurrent users)
- methods: us {both+hard, aff-hard, aff-soft, prefix} (pull); boom {round_robin, key_affinity, kvc_aware}

## Purpose
Third point on the concurrency ramp (medium load). 145 (boom kvc_aware) is the re-run after
the original mid-run kill.

## Findings
Ours ahead ~**+8% overall**. Best Ours (aff-hard) beats best BooM (round-robin): E2E-mean
+19% faster, RPS +5%, TPS +2%. BooM key_affinity again pins to 1 pod (worst cell here).

Caveat: engine (Prometheus) TTFT/TPOT for 139-144 are **past retention** (runs Jul 22-23) and
survive only for 145; the E2E/RPS/TPS comparison (workload-log derived) covers all 7 cells.

## Observations
Lower makespan/latency better; higher TPS/kv_hit better. kv_hit is router-side (mine only).

| exp | method | makespan(min) | e2e_p50(s) | e2e_mean(s) | e2e_p95(s) | TPS(tok/s) | kv_hit | pods |
|---|---|---|---|---|---|---|---|---|
| 139 | us both+hard | 217.4 | 112.7 | 366.0 | 1445.8 | **180.0** | 0.435 | 2 |
| 140 | us aff-hard | 148.4 | 105.4 | **223.7** | 776.0 | 179.0 | 0.998 | 2 |
| 141 | us aff-soft | 173.2 | 90.8 | 248.3 | 910.1 | 151.8 | 0.995 | 2 |
| 142 | us prefix | 226.2 | 114.4 | 325.5 | 1088.6 | 138.8 | 0.408 | 2 |
| 143 | boom rr | 155.8 | 100.5 | 266.1 | 1061.9 | 176.0 | — | 2 |
| 144 | boom key_affinity | 202.2 | 132.5 | 350.3 | 1249.6 | 137.4 | — | 1 |
| 145 | boom kvc | 178.5 | 98.4 | 269.0 | 916.9 | 162.1 | — | 2 |

Overall ranking (norm score): us-aff-hard 0.998 > boom-rr 0.924 > boom-kvc 0.913 >
us-aff-soft 0.866 > us-both 0.765 > boom-key_aff 0.711 > us-prefix 0.705.
