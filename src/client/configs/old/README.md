# Archived configs (`old/`)

Historical / superseded client sweep configs, moved out of the active `configs/`
root to keep it small. The active deployments are the LMCache **P2P host-staging**
line (`prod-*-lmcache-p2p-hoststaging-affinity*`) plus the `benchmark-*` sweeps;
everything here predates or was subsumed by that line.

This directory holds two kinds of things: the archive **category folders** below
(each a group of superseded prod configs) and the former `configs/non-prod/` tree
(the flat benchmark/template library), now folded in at [`non-prod/`](non-prod/).

These are kept (not deleted) for provenance and so they can be re-run if needed.
`sweep_methods.py` resolves subfolder keys under `configs/`, so any of these still
works by its relative path, e.g.:

```
cd src/client && python sweep_methods.py --config 1-master_config
# with a master entry like:
#   old/mooncake/prod-bz-boom-minmax-mooncake:
#     - pull
```

## Categories

| Folder | What it is | Superseded by |
| --- | --- | --- |
| [`mooncake/`](mooncake/) | BooM + Mooncake remote-KV deployments and the multi-turn strategy sweep on Mooncake. | LMCache P2P host-staging (no Mooncake). |
| [`lmcache-pre-p2p/`](lmcache-pre-p2p/) | Early LMCache lineage: plain, `-hq`, and `-local-hq` (Mooncake-remote / local-cache-first). | `*-lmcache-p2p-hoststaging-affinity`. |
| [`dp-multiturn/`](dp-multiturn/) | 4-node DP1/DP2 routing / affinity validation runs (no Mooncake). | One-off cluster validation; not a standing deployment. |
| [`claude-strategy/`](claude-strategy/) | Light (10-user) + batch16 claude-CLI routing-strategy comparison baselines. | `benchmark-bz-pull-*` (96-user runs). |
| [`autoscaling/`](autoscaling/) | KEDA autoscaling validation (dense Qwen3-8B), prod + shadow. | Isolated validation release; re-enable only for autoscaling tests. |
| [`shadow/`](shadow/) | yz shadow-pool BooM variants (`-hq`, `-hq-go`). | The shadow P2P host-staging config kept active in the root. |
| [`p2p-baselines/`](p2p-baselines/) | Non-affinity P2P host-staging baselines used to compare against the affinity variants. | Affinity variants kept active in the root. |
| [`non-prod/`](non-prod/) | Former `configs/non-prod/`: flat benchmark/template library + its own master plans (`non-prod/1-master_config.yaml`, `non-prod/boom_master.yaml`). Reference by `old/non-prod/<name>`. | n/a (benchmark/dev scratch, not a deployment). |
