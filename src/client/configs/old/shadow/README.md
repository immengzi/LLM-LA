# `old/shadow/`

YZ **shadow pool** BooM variants (parallel to prod, release/ns `vllm-shadow`) on
the pre-P2P LMCache lineage, including the Go-router (`-go`) shadows. The shadow
**P2P host-staging affinity** config is kept active in the root (it mirrors the
kept primary yz P2P config), so it is not archived here.

| Config | Purpose |
| --- | --- |
| `prod-yz-shadow-boom-minmax-lmcache-hq.yaml` | Shadow, LMCache HQ. |
| `prod-yz-shadow-boom-minmax-lmcache-hq-affinity.yaml` | Shadow, HQ + router key-affinity. |
| `prod-yz-shadow-boom-minmax-lmcache-hq-go.yaml` | Shadow, HQ, Go router. |
| `prod-yz-shadow-boom-minmax-lmcache-hq-go-affinity.yaml` | Shadow, HQ + affinity, Go router. |
