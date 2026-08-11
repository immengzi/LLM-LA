# P/D metrics contract and advisory planner inputs

This page defines the minimal metrics contract consumed by the `pd_planner.py`
decision function and where those metrics come from in LLM-LA / vLLM /
Prometheus. Status note: the contract is a **design input**, not metric names
already verified against a live cluster; the metric names below must be scraped
on the target Ascend environment before the threshold defaults are frozen.

## Decision function inputs

`pd_planner.py`'s `decide()` is a pure function that consumes exactly two
role-level pressure signals:

```text
MetricsSnapshot:
  prefill_backlog_tokens      float   # prefill-side queued backlog (tokens)
  decode_kv_usage_percent     float   # decode-side KV cache utilization (0..100)
```

It returns a `PlannerDecision`: a recommended target `(prefill, decode)` or
`None`, together with a `reason` and the updated `PlannerState` (consecutive
observation counts + last-change timestamp). The module never executes anything.

## Proposed metric sources

| Signal | Suggested metric | Aggregation | Verification status |
|--------|------------------|-------------|---------------------|
| Prefill backlog | `vllm:num_requests_waiting` or router queue depth × average prompt tokens | aggregated per prefill role | ⚠️ confirm metric name and labels on the target environment |
| Decode KV pressure | `vllm:gpu_cache_usage_perc` | max/mean per decode role | ⚠️ LLM-LA KEDA already uses this metric shape on the single-model path; the P/D role label needs confirmation |

On the standard LLM-LA path, `60-keda-scaledobject.yaml` already uses
`vllm:gpu_cache_usage_perc{model_name=...}` as a `vllm` signal, so this metric
shape has precedent on the non-P/D path. The P/D path still needs a live scrape
of `/metrics` to confirm the role-dimensioned labels.

## Decision rules (defaults can be overridden in the config)

| Parameter | Default | Meaning |
|-----------|---------|---------|
| `min_prefill` / `min_decode` | 1 / 1 | minimum replicas per role |
| `max_total` | 3 | fixed `P+D` budget |
| `prefill_scale_up_tokens` | 512 | prefill backlog above this counts as pressure |
| `prefill_scale_down_tokens` | 128 | below this counts as relaxed (hysteresis floor) |
| `decode_scale_up_kv_percent` | 80 | decode KV utilization above this counts as pressure |
| `decode_scale_down_kv_percent` | 60 | below this counts as relaxed |
| `min_observations` | 5 | consecutive samples required in the same direction |
| `cooldown_seconds` | 300 | minimum seconds between two transitions |

Rules:

```text
prefill pressure and decode relaxed -> recommend D->P (one replica per step)
decode pressure and prefill relaxed -> recommend P->D
both pressured / both relaxed / budget or floor blocks -> no change
```

## Closed loop (current state)

```text
Prometheus / vLLM metrics
        ↓ (scrape or snapshot)
pd_planner.decide()      ← advisory: recommends only, never executes
        ↓
propose candidate (P,D)  ← validated and persisted; dry-run only logs
        ↓
human confirms and commits
        ↓
executor (Docker or Kubernetes backend) runs transition_plan
```

The Docker executor (`pd_rebalancer_docker.py`) and the Kubernetes controller
(`pd_rebalancer.py`) share the same `transition_plan`; the planner only produces
a target and never touches containers or Deployments.

The Kubernetes controller applies targets in two phases: `propose` first
persists a candidate to the state ConfigMap (with `reason` and `proposedAt`),
`commit` promotes it to the active target for the reconcile loop to execute,
and `discard` drops an uncommitted candidate. With `pdRebalancer.dryRun: true`
(or `PD_REBALANCER_DRY_RUN=true`) the controller only prints the `[DRY-RUN]`
plan and skips `/scale`, which makes it easy to observe behavior without moving
replicas.
