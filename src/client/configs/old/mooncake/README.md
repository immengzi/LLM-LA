# `old/mooncake/`

BooM + router deployments using **Mooncake** as the remote KV store, plus the
multi-turn `router_strategy` sweep run on top of Mooncake. Superseded by the
LMCache **P2P host-staging** line (no Mooncake, no NDS), which avoids the
device-registration crash and the peak-load prefix-cache collapse.

| Config | Purpose |
| --- | --- |
| `prod-bz-boom-minmax-mooncake.yaml` | BZ MiniMax-M2.7, BooM + router, DP2 + Mooncake. |
| `prod-yz-boom-minmax-mooncake.yaml` | YZ equivalent of the above. |
| `prod-bz-boom-minmax-mooncake-multiturn.yaml` | Single-config multi-turn run (router_strategy chosen inline), ad-hoc. |
| `prod-bz-boom-minmax-mooncake-multiturn-none.yaml` | Multi-turn sweep, `router_strategy=none` (DP2 + Mooncake). |
| `prod-bz-boom-minmax-mooncake-multiturn-prefix.yaml` | Multi-turn sweep, `router_strategy=prefix`. |
| `prod-bz-boom-minmax-mooncake-multiturn-affinity.yaml` | Multi-turn sweep, `router_strategy=affinity`. |
| `prod-bz-boom-minmax-mooncake-multiturn-both.yaml` | Multi-turn sweep, `router_strategy=both`. |
