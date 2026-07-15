# `old/p2p-baselines/`

Non-affinity (`none`) baselines on the current LMCache **P2P host-staging** line,
kept for comparison against the affinity variants. The affinity variants
(`*-p2p-hoststaging-affinity`, `-affinity-fair`) stay active in the root; this
`none` baseline is archived because only the affinity prod configs are active.

| Config | Purpose |
| --- | --- |
| `prod-bz-boom-minmax-lmcache-p2p-hoststaging-none-fair.yaml` | BZ P2P host-staging, `router_strategy=none`, fair-pull enabled — baseline for the affinity-fair run. |
| `benchmark-bz-boom-direct-lmsys-kv-soak.yaml` | First-draft BZ KV soak (BooM direct + `key_affinity`). Superseded by `benchmark-bz-us-boom-lmsys-kv-soak` + `benchmark-bz-boom-only-lmsys-kv-soak` in configs root. |
