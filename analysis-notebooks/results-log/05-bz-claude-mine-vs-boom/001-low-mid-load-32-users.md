---
id: c05-001
date: 2026-07-27
campaign: 05-bz-claude-mine-vs-boom
series: u32
exp_ids: [132, 133, 134, 135, 136, 137, 138]
methods: [us-both-hard, us-aff-hard, us-aff-soft, us-prefix, boom-rr, boom-key_affinity, boom-kvc]
status: done
---
# Series u32 — low-mid load (32 users): LLM-LA (mine) vs BooM

Notebook: `analysis-notebooks/u32-mine-vs-boom.ipynb`

## Setup
- servers: bz (192.168.0.79); MiniMax-M2, 2 leader pods
- backend: mine = us + LLM-LA router (`pull`); boom = BooM direct (`directRoutingStrategy`)
- dataset: CodeFlowBench; transport: real Claude Code CLI (`transport.mode=claude`)
- KV transport: LMCache P2P host-staging; `strip_claude_code_attribution`: ON (all cells)
- total_requests: 32 users x 4 convs = 128 conversations (same replay); per-turn timeout 10000s
- rate_rps: closed-loop (32 concurrent users)
- methods: us {both+hard, aff-hard, aff-soft, prefix} (pull); boom {round_robin, key_affinity, kvc_aware}

## Purpose
Second point on the 16->32->64->128 concurrency ramp; check whether our low-load win holds
as load rises.

## Findings
Near-parity, Ours edges ahead. Best Ours (aff-hard) beats best BooM (kvc_aware) by only
**+1% overall**. E2E-mean +1%, TPS +15%, TTFT +1%; RPS -4%. BooM kvc_aware is strong at this
load (best BooM). BooM key_affinity still concentrates on 1 pod.

## Observations
Lower makespan/latency better; higher TPS/kv_hit better. kv_hit is router-side (mine only).

| exp | method | makespan(min) | e2e_p50(s) | e2e_mean(s) | e2e_p95(s) | TPS(tok/s) | kv_hit | pods |
|---|---|---|---|---|---|---|---|---|
| 132 | us both+hard | 263.4 | 81.5 | 292.8 | 1189.1 | 128.5 | 0.505 | 2 |
| 133 | us aff-hard | 193.8 | 92.9 | 324.9 | 1337.8 | 169.6 | 0.823 | 2 |
| 134 | us aff-soft | 238.5 | 82.7 | 262.5 | 806.9 | 102.2 | 0.511 | 2 |
| 135 | us prefix | 234.6 | 83.7 | 285.0 | 861.1 | 126.8 | 0.984 | 2 |
| 136 | boom rr | 250.5 | 90.5 | 272.1 | 998.4 | 126.0 | — | 2 |
| 137 | boom key_affinity | 280.9 | 100.1 | 325.2 | 1290.1 | 108.2 | — | 1 |
| 138 | boom kvc | 187.7 | 90.2 | 291.2 | 921.2 | **171.9** | — | 2 |

Overall ranking (norm score): us-aff-hard 0.944 > boom-kvc 0.935 > boom-rr 0.866 >
us-aff-soft 0.864 > us-prefix 0.858 > us-both 0.842 > boom-key_aff 0.773.
