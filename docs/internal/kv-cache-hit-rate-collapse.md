# KV / prefix-cache hit-rate collapse under peak load

> Internal investigation report (2026-07-01). Kept for historical reference and as
> the design rationale behind the `*-lmcache-local-hq-affinity` configs. Not a user guide.
> Operational runbook entry: [disaster-recovery.md §11](../operations/disaster-recovery.md).
> Raw evidence bundle: `142/` (pod logs, metrics jsonl, config dumps, collector scripts) and
> `142_data/lmcache-csi.zip` (the reference machines' real launch scripts + startup logs).

> **Correction (2026-07-01, after the reference launch scripts arrived):** an earlier draft said
> `direct142` shared KV via **NDS running as P2P** and that we "already had the same P2P handshake."
> The reference startup logs disprove this. `direct142` uses **LMCache-native P2P** (`enable_p2p:
> true` + a dedicated `lmcache_controller` + host-staging) with **NDS disabled**. Our stack's
> `P2PHANDSHAKE` is **Mooncake's** transfer-metadata server, not LMCache P2P — our `enable_p2p` is
> `false`. The headline fix (drop Mooncake, grow the local cache) is unchanged; but our
> local-hq config is **local-cache-only**, not a true `direct142` mimic. See "Two reference
> architectures" below.

## Executive summary

Under high load, the prefix-cache hit rate on our two MiniMax-M2.7 backends
(`llmla1-direct65`, `llmla2-direct65`) **collapsed** (down to ~8% and ~19%), and TTFT
and end-to-end latency spiked. A reference external deployment (`direct142`) stayed
flat at ~64–74% through the same peaks.

The drop is **not** a cache bug and **not** a routing problem. It is a congestion
collapse driven by two coupled failures:

1. **GPU KV-cache saturation.** At peak our engines sit at 93–99% GPU KV usage with a
   standing queue (7–14 waiting) and preemptions. At that pressure vLLM evicts its
   built-in prefix blocks immediately, so the built-in prefix hit rate falls to ~0.5%.
2. **A full, lossy remote KV tier (Mooncake).** The fallback remote store was 88% full
   at its 0.9 eviction watermark and **dropping ~46% of KV writes**. The pool is built
   from our own workers (4 pods × TP8 = 32 clients, each donating 75 GiB host RAM ≈
   2.34 TB). Hot prefixes are evicted or never stored, so reuse decays as load grows.

`direct142` avoids both: it uses a **large local CPU cache (100 GiB/engine) with no
remote store**, plus **LMCache-native P2P** between engines (not NDS — see below). Its
retrieve hit rate is 100%, GPU KV ~24%, zero waiting. It never enters the spiral.

**Ruled out — kv-aware routing.** We deployed kv-aware/affinity routing to fix this; it
behaved correctly until the next peak, where the collapse recurred. The problem is the
KV-cache backend, not routing.

**Fix / design change:** reclaim the Mooncake RAM into a big local cache and rely on hard
affinity. Captured in `prod-yz-boom-minmax-lmcache-local-hq-affinity.yaml` (and a BZ port):
`lmcache_max_local_cpu_size: 50 → 128`, `mooncake_enabled: true → false`. On the current chart
this is **local-cache-only** (no cross-engine P2P — see below); a true `direct142` P2P mimic is a
separate chart change.

---

## Evidence

Sources: 4-min pod-log snapshot (08:45–08:49) from our cluster, `mooncake-master` log,
6h of scraped `direct142` metrics, both `/metrics` + rendered `/tmp/lmcache_config.yaml`,
and `free -g` on all nodes. Collector: `142/check_node_ram.sh`.

### Engine / scheduler pressure

| Signal | Ours (direct65) | direct142 (HQ) |
|---|---|---|
| GPU KV cache usage | **93.0–99.7%** (pinned) | median **24%**, max 85% |
| Requests waiting | **7–14** (constant) | median **0** |
| Preemptions | present, climbing | **0** |
| Built-in prefix hit rate | **0.4–1.6%** | healthy (part of stable ~74%) |
| Overall prefix hit (observed) | 82% → 8–33% (decays) | flat ~64–74% |

### Cache-tier behaviour

| Signal | Ours (direct65) | direct142 (HQ) |
|---|---|---|
| Local CPU cache | **50 GiB/worker** | **100 GiB/engine** (`local_cache_usage = 107,366,842,368`) |
| Remote store | Mooncake **on** (`mooncakestore://…`) | **off** (`remote_cache_usage = 0`) |
| Retrieve hit rate | partial; reloads full prefixes | **`retrieve_hit_rate = 1.0`** |
| Slow retrievals | ~900 ms for 2.24 GB | `num_slow_retrieval_* = 0` |
| Remote store fill | **2.08/2.34 TB (88.6%)**, watermark 0.9 | n/a |
| Write success | Mooncake **~54%** (`PutStart Item 356243/656176`) | `local_cpu_evict_failed = 0` |

### The Mooncake smoking gun (our cluster)

```
Mem Storage: 2.08 TB / 2.34 TB (88.6%)                 <- at eviction watermark
Eviction: Success/Attempts=15/15, keys=215706, size=3.19 TB
PutStart: Item=356243/656176                            <- only 54.3% of KV blocks stored
Clients: 32                                             <- our own 4 pods x 8 TP ranks
```

Each of the 32 TP ranks contributes `global_segment_size` = 75 GiB, and 32 × 75 GiB ≈
2.34 TB — the pool is built entirely from our own workers' RAM (~600 GiB/node).

### Retrieval feedback loop

```
Retrieved 75776 tokens, size 2.24 GB, cost 735–925 ms, backends: ['NdsBackend','NdsBackend_nds']
Reqid ... need to load 75520   (same request re-evaluated 100–270x while it waits)
```

Slow remote reloads hold KV blocks while a request waits, keeping KV at ~99% and growing
the queue. On `direct142`, retrieves are 100% local-RAM hits, so requests drain and KV
never climbs.

---

## Why ours diverged from the reference

The `-hq` config is titled "matches HQ as closely as possible", and the **compute** half
does (DP2, TP8, `gpuMemoryUtilization 0.92`, `maxNumBatchedTokens 32768`, batch 32,
scheduler). The **cache** half does not:

Values below are the **resolved `LMCacheEngine` config each side prints at startup** — ground
truth from the reference logs, not inference:

| Axis | Ours (33-lineage) | direct142 (startup log) |
|---|---|---|
| Local CPU | `max_local_cpu_size: 50` | **`100.0`** |
| Remote store | `remote_url: mooncakestore://…`, `global_segment_size: 75 GiB/client` | **`remote_url: None`** |
| NDS | `nds_dev: /dev/md0`, `nds_size: [2048]` | **`nds_dev: None`** (no NDS) |
| LMCache P2P | **`enable_p2p: False`**, no controller | **`enable_p2p: True`** + `lmcache_controller` (`:9800`/`:9900`), `transfer_channel: hccl` |
| Host staging | not set | **`use_host_staging: True`** |
| Connector | `LMCacheAscendConnectorV1Dynamic` | **`LMCacheAscendConnector`** (plain) |
| Scheduler | `--scheduler-cls lsched…` | none (default) |
| LMCache version | 0.4.3.dev2 | 0.4.5.dev2 |

We halved the local cache and bolted on a shared, watermark-evicting, 46%-lossy Mooncake store.
Our `P2PHANDSHAKE` is **Mooncake's** metadata server, not LMCache P2P (`enable_p2p: False`), so it
is **not** the mechanism the reference uses — and it dies the moment Mooncake is off.

---

## RAM budget (why the fix is safe)

Engine nodes are dedicated 1.5 TB (1509 GiB) machines. Actual `free -g` (2026-07-01):

| Node | Role | Total | Used | Available |
|---|---|---|---|---|
| node1 | leader m2-0 | 1509 | 1104 | 391 |
| node8 | leader m2-1 | 1509 | 1096 | 401 |
| node2 | worker m2-0-1 | 1509 | 1093 | 404 |
| node7 | worker m2-1-1 | 1509 | 1093 | 393 |

The ~1096 GiB used per node ≈ 8×50 local (400) + 8×75 Mooncake (600) + ~96 vLLM/OS.
Only ~390 GiB is free **because ~600 GiB is tied up in the full, lossy Mooncake pool.**

Dropping Mooncake and reallocating: usable ≈ 1509 − ~200 (vLLM+OS) = ~1300 GiB ÷ 8 =
~163 GiB/worker ceiling.

| `max_local_cpu_size` | ×8 per node | Node usage | Verdict |
|---|---|---|---|
| 100 (match HQ) | 800 GiB | ~1000/1509 | very safe |
| **128 (chosen)** | 1024 GiB | ~1224/1509 (~285 free) | safe, beats HQ |
| 150 | 1200 GiB | ~1400/1509 | tight, not advised |

Keeping Mooncake caps local at ~85 GiB/worker (390 free ÷ 8) — not enough to reach HQ's
100. Reclaiming Mooncake is what unlocks the headroom.

---

## Two reference architectures (NDS vs P2P vs Mooncake)

The reference bundle shows HQ runs **two different KV designs**, each printing its resolved
`LMCacheEngine` config at startup:

| | **142 / 50** (`lmcache_p2p`) | **33 / 37 / 218 / 207** (`nds_dsched`) |
|---|---|---|
| Cross-engine sharing | **LMCache-native P2P** (`enable_p2p: True`) | **Mooncake remote store** (`enable_p2p: False`) |
| `max_local_cpu_size` | 100 GiB | 50 GiB |
| `remote_url` | `None` | `mooncakestore://…:50088/` |
| Controller | `lmcache_controller` (`:9800`/`:9900`) | none |
| NDS (`nds_dev`) | `None` | `/dev/md0`, `nds_size [2048]` |
| Host staging | `use_host_staging: True` | not set |
| Connector | `LMCacheAscendConnector` | `LMCacheAscendConnectorV1Dynamic` |
| LSched | absent | present |
| LMCache version | 0.4.5.dev2 | 0.4.3.dev2 |

Three distinct pieces, previously conflated:
- **NDS (NVMe Direct Storage)** — a *local* NVMe spill tier (`/dev/md0`, `use_ascend_direct`).
  Belongs to the **33/Mooncake** lineage. **142 does not use it** (`nds_dev: None`).
- **LMCache-native P2P** — engines exchange KV directly over HCCL, coordinated by an
  `lmcache_controller`, staged through host RAM (`use_host_staging`). This is **142's** sharing
  path; it needs no central store and no NDS.
- **Mooncake** — the centralized remote object store (`remote_url: mooncakestore://…`). The 33
  lineage — and ours — use this for cross-engine sharing. Its `P2PHANDSHAKE` is a *Mooncake*
  transfer-metadata server, **not** LMCache P2P.

**Our stack is the 33 lineage.** Chart confirms: connector `LMCacheAscendConnectorV1Dynamic`;
`13-lmcache-config.yaml` emits `remote_url: mooncakestore://…` + NDS keys and has **no**
`enable_p2p`/`enable_controller`/`use_host_staging` fields; there is **no `lmcache_controller`
Deployment**. Our engine logs match: 400 NDS `Local copy` transfers (5.5–19 ms, zero errors) and
**zero** peer copies — NDS was a local tier; all cross-node sharing went through Mooncake.

**Will disabling Mooncake give us 142's P2P? No.** Our stack has no LMCache-P2P path to activate.
Dropping Mooncake leaves each engine **local-cache-only** (NDS stays a local spill) — a big win
with hard affinity, but not 142's shared design. There is also a chart defect: `13-lmcache-config.yaml`
writes `remote_url: mooncakestore://…` whenever LMCache is enabled, ignoring `mooncake_enabled`, so
"Mooncake off" still hands LMCache a dead `mooncakestore` URL. That needs to be made conditional.

**Matching 142 is chart work, not a config value:** plain `LMCacheAscendConnector`; conditional
`remote_url` (→ `None`); NDS off; LMCache-P2P knobs (`enable_p2p`, `enable_controller`,
`controller_pull/reply_url`, `transfer_channel: hccl`, `p2p_use_npu`, `use_host_staging`); a
deployed `lmcache_controller` service; drop `--scheduler-cls`; and an image with LMCache 0.4.5.dev2.

**Confirm the local-hq canary:** under load expect local-tier retrieves (no ~900 ms remote), GPU
KV off 99%, waiting → 0, flat hit curve. Cross-engine reuse will be weak — hard affinity keeps a
conversation on the engine holding its prefix. Enable `lmcache:*` export so `retrieve_hit_rate` is
visible (we export none today; 142 does).

---

## Fix and verification

**Config change** (`prod-yz-boom-minmax-lmcache-local-hq-affinity.yaml`, BZ port
`prod-bz-boom-minmax-lmcache-local-hq-affinity.yaml`):

- `lmcache_max_local_cpu_size: 50 → 128` (GiB per TP worker)
- `mooncake_enabled: true → false`, `deploy_mooncake_master: false`
- `lmcache_nds_enabled: true` stays on the YZ config, but note NDS is only a **local** spill here
  (not cross-engine) — with Mooncake off, each engine is local-cache-only; hard affinity is what
  keeps repeats on the engine that holds them.
- BZ caveats: needs an LMCache-capable image; no NDS on BZ nodes; verify BZ node RAM before 128.

**Deploy:** `cd src && python deploy_vllm.py --config configs/prod-yz-boom-minmax-lmcache-local-hq-affinity.yaml`

**Expected after warmup:** retrieve hit → ~100%, GPU KV off 99%, waiting → 0, preemptions
stop, prefix-hit curve flat like `direct142`. If GPU KV is still tight, cap `max_num_seqs`.

**Rollback:** it's a helm upgrade — revert the three values and redeploy.

---

## Follow-ups

- Enable `lmcache:*` Prometheus export on our engines (currently none exported).
- Alert on `num_requests_waiting > 0` sustained or `gpu_cache_usage_perc > 0.85`.
- Fix the chart so `remote_url` is omitted when `mooncake_enabled: false` (today
  `13-lmcache-config.yaml` always emits `mooncakestore://…`).
- (Optional) Build a true 142-style LMCache-P2P config — chart work: plain connector, NDS off,
  `enable_p2p`/`enable_controller`/`use_host_staging`, a deployed `lmcache_controller`, drop
  `--scheduler-cls`, LMCache 0.4.5.dev2 image. Only if local-cache-only proves insufficient.
- Reintroduce a remote KV store only when justified (many replicas, working set exceeding
  per-node RAM, or prefill/decode disaggregation), and size it so it isn't parked at its
  eviction watermark.
