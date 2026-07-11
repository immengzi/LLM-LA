# `old/lmcache-pre-p2p/`

Early LMCache lineage before the P2P host-staging design: plain BooM, the `-hq`
(headquarters) variants, and the `-local-hq` local-cache-first variant (big local
CPU cache, Mooncake remote store off). Superseded by
`*-lmcache-p2p-hoststaging-affinity` (bounded registered host-staging arena,
lmcache_controller instead of Mooncake).

| Config | Purpose |
| --- | --- |
| `prod-yz-boom-minmax.yaml` | Base YZ MiniMax-M2.7 BooM deployment (no external KV store). |
| `prod-yz-boom-minmax-lmcache.yaml` | YZ + LMCache. |
| `prod-yz-boom-minmax-lmcache-affinity.yaml` | YZ + LMCache + router key-affinity (hard) + prefix-hit logging. |
| `prod-yz-boom-minmax-lmcache-hq.yaml` | YZ + LMCache, HQ layout. |
| `prod-yz-boom-minmax-lmcache-hq-affinity.yaml` | HQ + router key-affinity. |
| `prod-yz-boom-minmax-lmcache-local-hq-affinity.yaml` | Local-cache-first (128 GiB/worker), Mooncake off, NDS/P2P + key-affinity; fixes peak-load prefix-cache collapse. |
| `prod-bz-boom-minmax-lmcache-local-hq-affinity.yaml` | BZ port of the local-hq-affinity fix. |
