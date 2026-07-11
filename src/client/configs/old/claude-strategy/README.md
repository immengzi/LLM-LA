# `old/claude-strategy/`

Claude-CLI prefix-cache routing-strategy comparison. Each config is a full cold
redeploy so every strategy runs on a clean KV/affinity slate. The **light** (10
users x 2) baselines are superseded by the high-load runs
(`benchmark-bz-pull-*`, 96 users x 3) kept active in the root. The
**batch16** rate-limit variants (both the light `prod-*` and the high-load
`benchmark-*`) are parked here too.

Light-load baselines (10 users x 2):

| Config | Purpose |
| --- | --- |
| `prod-bz-claude-none.yaml` | Baseline, `router_strategy=none` (batch 32 / hold 600). |
| `prod-bz-claude-prefix.yaml` | `router_strategy=prefix`. |
| `prod-bz-claude-affinity-hard.yaml` | Hard conversation affinity. |
| `prod-bz-claude-affinity-soft.yaml` | Soft conversation affinity. |
| `prod-bz-claude-both.yaml` | `router_strategy=both`. |
| `prod-bz-claude-affinity-hard-batch16.yaml` | Rate-limit-to-affined-node scenario: batchSize 16 + longer hard hold 800s. |
| `prod-bz-claude-both-batch16.yaml` | Same batch16/hold-800 scenario with `router_strategy=both`. |

High-load batch16 rate-limit variants (96 users x 3):

| Config | Purpose |
| --- | --- |
| `benchmark-bz-claude-affinity-hard-batch16-highload.yaml` | High-load rate-limit-to-affined-node: batchSize 16 + hard hold 800s. |
| `benchmark-bz-claude-both-batch16-highload.yaml` | Same, with `router_strategy=both`. |
