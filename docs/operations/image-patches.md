# vLLM / LMCache-Ascend image patches

Source-level fixes that must be **baked into the vLLM / LMCache-Ascend container
image** before it is pushed to the private registry. They are part of cluster
preparation: the `lmcache-ascend:hccl-p2p` image the cluster runs is only correct
once these are applied.

> This is the **why + how** doc. The patch files themselves and their applier
> scripts are the executable companion in [`infra/patches/`](../../infra/patches/)
> (see [`infra/patches/README.md`](../../infra/patches/README.md)). When the two
> disagree, treat the files under `infra/patches/` as the source of truth for
> **what** is applied and this doc as the source of truth for **why**.

## Why these patches exist

Under the LMCache **P2P + host-staging** topology (`lmcache_mode: p2p`, the
"direct142" replica — see
[lmcache-p2p-host-staging.md](../internal/lmcache-p2p-host-staging.md)), a
rollback / preemption under KV-cache pressure can produce a **negative
per-source prompt-token delta**. vLLM feeds that delta straight into a Prometheus
`Counter.inc()`, which rejects negative amounts:

```
ValueError: Counters can only be incremented by non-negative amounts.
  at vllm/v1/metrics/loggers.py
```

The unhandled `ValueError` kills the engine (upstream vLLM issues #36755,
#36533). This is a **different** crash from the "gpu size" / >10 GB
device-registration ceiling that host-staging itself fixes (see
[kv-cache-hit-rate-collapse.md](../internal/kv-cache-hit-rate-collapse.md)): the
registration ceiling kills the worker on the *first KV save*; this counter crash
kills the engine *later*, during metrics recording, once a negative delta shows
up. Both have to be handled for a stable P2P + host-staging deploy.

The fix guards the three counters whose value comes from the brittle
cache-accounting subtraction, so a negative delta is skipped instead of crashing.

## The patch files

All files live in [`infra/patches/`](../../infra/patches/). Paths in the build
commands below are relative to the **repo root**, which is also the Docker build
context.

| File | What it is | When to use |
|------|-----------|-------------|
| `vllm-metrics-loggers.diff` | The canonical fix as a `git`/`patch -p1` unified diff against `vllm/v1/metrics/loggers.py`. | Full LMCache-Ascend image rebuilds, where `patch` is available in the build stage. |
| `apply_metrics_fix.py` | Idempotent Python applier for the **same** fix. No `patch`/`git` needed (only `python3`). Verifies the result parses as valid Python. | Layering the fix onto an **existing** image that has no `patch` binary. |
| `Dockerfile.hccl-p2p-metrics-fix` | Thin overlay `Dockerfile` that runs `apply_metrics_fix.py` on top of the already-built `lmcache-ascend:hccl-p2p` image. | Fastest path: ship the crash fix without a full rebuild. |
| `lmcache-ascend-dockerfile.patch-snippet.sh` | Prints `RUN` lines to append to the upstream `LMCache-Ascend/docker/Dockerfile.a2.openEuler`, applying `lmcache-controller.diff` + `vllm-utils.diff` + `vllm-metrics-loggers.diff` in a from-scratch build. | Rebuilding the base image from source. |
| `dump-running-vllm-patch-context.sh` | Verification tool. Dumps the relevant vLLM / LMCache source regions out of a **running pod** and dry-runs the diff, so you can confirm what is (and isn't) already patched. | Before/after a rebuild, to prove the patch state of a live image. |

## How to apply

### Option A — overlay onto the existing image (recommended, no full rebuild)

The base `lmcache-ascend:hccl-p2p` image already bakes in `lmcache-controller.diff`
and `vllm-utils.diff`; this only layers the negative-counter guard on top.

```bash
# from the repo root (repo root is the Docker build context)
docker build -f infra/patches/Dockerfile.hccl-p2p-metrics-fix \
  -t reg.local:32000/lmcache-ascend:hccl-p2p-metrics-fix .

docker push reg.local:32000/lmcache-ascend:hccl-p2p-metrics-fix
```

Then point `vllm_image` + `lmcache_controller_image` at the
`hccl-p2p-metrics-fix` tag (see
[lmcache-p2p-host-staging.md](../internal/lmcache-p2p-host-staging.md) for where
those knobs live).

### Option B — bake into a full LMCache-Ascend rebuild

```bash
# copy the diff into the upstream build context
cp infra/patches/vllm-metrics-loggers.diff /path/to/LMCache-Ascend/docker/

# print the RUN lines to append to Dockerfile.a2.openEuler (after the
# `pip install lmcache && pip install LMCache-Ascend` step)
bash infra/patches/lmcache-ascend-dockerfile.patch-snippet.sh
```

### Option C — apply directly to a file / already-running layer

```bash
python3 infra/patches/apply_metrics_fix.py \
  /vllm-workspace/vllm/vllm/v1/metrics/loggers.py
```

Idempotent: prints `Already patched` on a second run and refuses to write if the
expected code block isn't found (e.g. a different vLLM version).

## Verify what a live pod is actually running

```bash
# defaults to namespace "vllm" and auto-detects a leader pod
bash infra/patches/dump-running-vllm-patch-context.sh
bash infra/patches/dump-running-vllm-patch-context.sh vllm vllm-minimax-m2-1
```

Produces a `patch-verify-<timestamp>/` folder (+ tarball). Check:

- `vllm-loggers-crash-region.txt` — lines around `counter_prompt_tokens_by_source`
- `patch-dry-run.txt` — says "succeeded" if the diff still applies (i.e. **not**
  yet patched); a reversed/failed hunk means the fix is already in place
- `vllm-utils-markers.txt` — confirms `vllm-utils.diff` (ARM `sched_yield` guard)
- `lmcache-controller-markers.txt` — confirms `lmcache-controller.diff`
  (`find_worker_key` / `target_worker_id`)

## Scope / audit note

Only three counters are guarded, because they are the only ones in `record()`
that can go negative on this (P2P / `kv_both` host-staging) topology:

- `counter_prompt_tokens_by_source` (the crash site)
- `counter_prompt_tokens_cached`
- `counter_prompt_tokens_recomputed`

Pure event counts (prefix/mm cache queries/hits, preempted/corrupted requests,
generation tokens) are mathematically non-negative and are left untouched.
`counter_prompt_tokens` can only go negative on **disaggregated P/D**
(`NixlConnector`, issue #38839), which this deployment does not run — revisit
this guard if that changes.

## Related

- [lmcache-p2p-host-staging.md](../internal/lmcache-p2p-host-staging.md) — the
  P2P + host-staging mode this image is built for.
- [registry.md](registry.md) — where images are built and pushed
  (`reg.local:32000`).
- [cluster-setup.md](cluster-setup.md) §9 — the image build/push step in the
  cluster-prep checklist.
- [cluster-prep-automation.md](cluster-prep-automation.md) — the cluster-prep
  automation these patches feed into.
