---
id: c05-000
date: 2026-07-27
campaign: 05-bz-claude-mine-vs-boom
series: u16
exp_ids: [122, 123, 124, 125, 129, 130, 131]
methods: [us-both-hard, us-aff-hard, us-aff-soft, us-prefix, boom-rr, boom-key_affinity, boom-kvc]
status: done
---
# Series u16 — low load (16 users): LLM-LA (mine) vs BooM

Notebook: `analysis-notebooks/u16-mine-vs-boom.ipynb`

## Setup
- servers: bz (192.168.0.79); MiniMax-M2, 2 leader pods
- backend: mine = us + LLM-LA router (`pull`); boom = BooM direct (`directRoutingStrategy`)
- dataset: CodeFlowBench; transport: real Claude Code CLI (`transport.mode=claude`)
- KV transport: LMCache P2P host-staging; `strip_claude_code_attribution`: ON (all cells)
- total_requests: 16 users x 2 convs = 32 conversations (same replay); per-turn timeout 10000s
- rate_rps: closed-loop (16 concurrent users)
- methods: us {both+hard, aff-hard, aff-soft, prefix} (pull); boom {round_robin, key_affinity, kvc_aware}

## Purpose
First apples-to-apples low-load comparison of our LLM-LA router strategies vs BooM's three
scheduler policies, with BooM's KV-cache prefix matching actually enabled (post strip-fix).

## Findings
Ours wins decisively. Best Ours (prefix) beats best BooM (round-robin) by **+53% overall**
(mean normalized score across TTFT/TPOT/E2E/RPS/TPS). E2E-mean +28% faster, TPS +82%, TTFT
+9%, TPOT +11%; RPS +318% (at this low load RPS is ~inverse makespan on the same 32-conv
replay). BooM key_affinity collapses onto 1 pod (expected under low concurrency).

## Observations
Lower makespan/latency better; higher TPS/kv_hit better. kv_hit is router-side (mine only).

| exp | method | makespan(min) | e2e_p50(s) | e2e_mean(s) | e2e_p95(s) | TPS(tok/s) | kv_hit | pods |
|---|---|---|---|---|---|---|---|---|
| 122 | us both+hard | 42.4 | 71.1 | 169.1 | 803.5 | 142.9 | 0.343 | 2 |
| 123 | us aff-hard | 57.2 | 61.6 | 172.2 | 752.1 | 119.3 | 0.994 | 2 |
| 124 | us aff-soft | 54.5 | 61.2 | 136.5 | 517.9 | 102.0 | 0.456 | 2 |
| 125 | us prefix | **18.4** | **51.8** | **70.7** | 240.0 | 118.8 | 0.988 | 2 |
| 129 | boom rr | 91.1 | 67.9 | 165.5 | 504.8 | 71.8 | — | 2 |
| 130 | boom key_affinity | 76.9 | 78.3 | 208.1 | 676.0 | 95.0 | — | 1 |
| 131 | boom kvc | 126.6 | 66.5 | 226.2 | 846.7 | 67.1 | — | 2 |

Overall ranking (norm score): us-prefix 0.987 > us-both 0.791 > us-aff-hard 0.731 >
us-aff-soft 0.719 > boom-rr 0.645 > boom-key_aff 0.623 > boom-kvc 0.570.
