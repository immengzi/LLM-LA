# `old/dp-multiturn/`

4-node data-parallel routing / conversation-affinity validation runs (no Mooncake;
external KV not needed to validate routing). Used to confirm k8s-worker2 rejoined
traffic after its networking fix and to check DP1-vs-DP2 placement. One-off cluster
validation, not a standing deployment.

DP1 = single TP=8 pod per replica across 4 nodes; DP2 = 2 DP ranks/replica
(sizeLocal=1, RoCE-paired) x 2 replicas = 4 nodes.

| Config | Purpose |
| --- | --- |
| `prod-bz-boom-minmax-multiturn-none-dp1.yaml` | DP1 x4, `router_strategy=none`. |
| `prod-bz-boom-minmax-multiturn-prefix-dp1.yaml` | DP1 x4, `router_strategy=prefix`. |
| `prod-bz-boom-minmax-multiturn-affinity-dp1.yaml` | DP1 x4, `router_strategy=affinity`. |
| `prod-bz-boom-minmax-multiturn-both-dp1.yaml` | DP1 x4, `router_strategy=both`. |
| `prod-bz-boom-minmax-multiturn-affinity-dp2.yaml` | DP2 x2 (4 nodes), hard conversation affinity. |
