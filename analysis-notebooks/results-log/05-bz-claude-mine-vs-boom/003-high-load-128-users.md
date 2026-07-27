---
id: c05-003
date: 2026-07-27
campaign: 05-bz-claude-mine-vs-boom
series: u128
exp_ids: [146, 147, 148, 149, 150, 151, 152]
methods: [us-both-hard, us-aff-hard, us-aff-soft, us-prefix, boom-rr, boom-key_affinity, boom-kvc]
status: done
---
# Series u128 — high load (128 users): LLM-LA (mine) vs BooM

Notebook: `analysis-notebooks/u128-mine-vs-boom.ipynb`

## Setup
- servers: bz (192.168.0.79); MiniMax-M2, 2 leader pods
- backend: mine = us + LLM-LA router (`pull`); boom = BooM direct (`directRoutingStrategy`)
- dataset: CodeFlowBench; transport: real Claude Code CLI (`transport.mode=claude`)
- KV transport: LMCache P2P host-staging; `strip_claude_code_attribution`: ON (all cells)
- total_requests: 128 users x 1 conv = 128 conversations (same replay); per-turn timeout 10000s
- rate_rps: closed-loop (128 concurrent users, saturation regime)
- methods: us {both+hard, aff-hard, aff-soft, prefix} (pull); boom {round_robin, key_affinity, kvc_aware}

## Purpose
Top of the concurrency ramp (saturation). Does the low/medium-load advantage hold under
overload?

## Findings
**BooM round-robin wins overall** (best Ours aff-hard is **-12% overall** vs it). At saturation
BooM RR spreads work best and gets much higher throughput: TPS 375 vs Ours-best 254.7 (-32%),
RPS -29%, makespan 84.9 vs 119.3 min, E2E-mean -12%. Ours keeps the per-token latency edge
(TTFT +6%, TPOT +12%). BooM key_affinity degenerates badly (1 pod: e2e-mean 550s, engine TTFT
mean ~77.5s). Net: our win shrinks as load rises and flips to BooM-RR at 128-user saturation
on throughput, while we retain TTFT/TPOT.

## Observations
Lower makespan/latency better; higher TPS/kv_hit better. kv_hit is router-side (mine only).

| exp | method | makespan(min) | e2e_p50(s) | e2e_mean(s) | e2e_p95(s) | TPS(tok/s) | kv_hit | pods |
|---|---|---|---|---|---|---|---|---|
| 146 | us both+hard | 248.5 | 170.9 | 416.7 | 1455.3 | 136.9 | 0.459 | 2 |
| 147 | us aff-hard | 119.3 | 156.8 | 364.4 | 1373.7 | 254.7 | 0.889 | 2 |
| 148 | us aff-soft | 139.9 | 169.1 | 375.9 | 1536.4 | 231.3 | 0.774 | 2 |
| 149 | us prefix | 185.9 | 151.8 | 393.7 | 1185.2 | 153.5 | 0.639 | 2 |
| 150 | boom rr | **84.9** | **144.0** | **320.2** | 1324.9 | **375.0** | — | 2 |
| 151 | boom key_affinity | 167.1 | 317.9 | 550.3 | 1566.7 | 192.9 | — | 1 |
| 152 | boom kvc | 112.9 | 212.8 | 357.0 | 1067.8 | 133.1 | — | 2 |

Overall ranking (norm score): boom-rr 0.966 > us-aff-hard 0.853 > us-aff-soft 0.797 >
us-prefix 0.717 > boom-kvc 0.711 > us-both 0.688 > boom-key_aff 0.478.
