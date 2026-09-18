# P/D metrics contract

Inputs consumed by `pd_planner.py`'s `decide()` and their sources.

## Metrics

| Signal | Metric | Aggregation |
|--------|--------|-------------|
| Prefill backlog | `pd_proxy_prefill_inflight` × `pd_proxy_prefill_mean_prompt_tokens` | sum over P/D proxy pods |
| Decode KV pressure | `vllm:kv_cache_usage_perc` | max over decode pods |

`prefill_mean_prompt_tokens_fallback` (default 128) is used until the proxy has
observed at least one successful prefill response. The default is nonzero so a
cold-start queue of in-flight requests still produces backlog; calibrate it to
the workload's mean prompt-token length via `pdRebalancer.plannerConfig`.

## Decision rules (defaults overridable via `pdRebalancer.plannerConfig`)

| Parameter | Default | Meaning |
|-----------|---------|---------|
| `min_prefill` / `min_decode` | 1 / 1 | minimum replicas per role |
| `max_total` | 3 | fixed `P+D` budget |
| `prefill_scale_up_tokens` | 512 | prefill backlog above this counts as pressure |
| `prefill_scale_down_tokens` | 128 | below this counts as relaxed (hysteresis floor) |
| `prefill_mean_prompt_tokens_fallback` | 128 | conservative mean prompt tokens until real prefill responses are observed (calibrate to workload) |
| `decode_scale_up_kv_percent` | 80 | decode KV utilization above this counts as pressure |
| `decode_scale_down_kv_percent` | 60 | below this counts as relaxed |
| `min_observations` | 5 | consecutive samples required in the same direction |
| `cooldown_seconds` | 300 | minimum seconds between two transitions |
| `max_step_replicas` | 1 | largest number of replicas one transition may move (clamped by the role floors and the fixed `P+D` budget) |

```text
prefill pressure and decode relaxed -> recommend D->P (up to max_step_replicas)
decode pressure and prefill relaxed -> recommend P->D (up to max_step_replicas)
both pressured / both relaxed / budget or floor blocks -> no change
```

A transition trades replicas between the roles, so `P + D` is preserved and the
step is bounded by `decode - min_decode` (or `prefill - min_prefill`) and by the
per-role budget cap. The executor applies the whole target atomically, so with
`max_step_replicas: 2` a swing like `P1,D3 -> P3,D1` completes in a single
transition instead of two — one round of per-card sleep/wake flips in
warm-standby mode (and, while the blocking KV warm-up gate is on, one drain
window).

## Closed loop

```text
P/D proxy /metrics + decode engine /metrics
        ↓
pd_planner.decide()     ← advisory: recommends only, never executes
        ↓
propose → commit        ← two-phase target persisted in the state ConfigMap
        ↓
executor (Kubernetes backend) runs transition_plan
```

### Advisory vs. automatic

The planner is **advisory by default** (`pdRebalancer.advisory: true`): every
poll logs the recommendation as `[planner:advisory] ... not applied` and no
target is proposed or committed. Nothing auto-scales until the switch is
flipped explicitly:

```yaml
pdRebalancer:
  advisory: false   # planner auto-proposes and commits recommended targets
```

With `advisory: false`, the planner drives the same two-phase
propose → commit path a human would use, and the executor applies the committed
target through the transition protocol below. `pdRebalancer.dryRun: true` can
be combined with automatic mode as a rehearsal layer: the planner still
proposes/commits targets in the state ConfigMap, but the executor logs the
planned transitions without touching `Deployment /scale`.

## Transition protocol (drain / lock / rollback / capacity)

Once a target is committed, the executor applies it with a correctness-first
protocol instead of a blind `/scale` sequence:

1. **Transition lock** — a `transition` marker (`active`, `target`,
   `previous`, `startedAt`) is persisted in the state ConfigMap. While it is
   set, the planner defers auto decisions and the manual API rejects
   propose/commit/discard with 409.
2. **Drain handshake** — the executor asks the P/D proxy to pause new P/D
   requests (`POST /drain {"enabled": true}`; queued requests wait up to
   `PROXY_DRAIN_MAX_WAIT_SECONDS`), then polls `GET /status` until
   `prefill_inflight == decode_inflight == 0`.
3. **Convergence** — every scale step waits for Deployment status
   (`spec == ready == updated == available`, `observedGeneration` current)
   **and** a live Pod list with no terminating Pods and all Pods Ready, so an
   old pod's accelerator cards are actually released before the target role
   scales up.
4. **Capacity preflight** — before a scale-up step (and again at commit) the
   executor compares the target's `P*TP + D*TP` card demand against free
   `accelerator.resourceName` cards (configured per cluster) across Ready,
   schedulable nodes. A target that cannot fit is rejected with 409 / rolled
   back.
5. **Rollback** — any failure (drain timeout, scale/ready timeout, capacity
   rejection) disables the drain and scales back to `previous`; on success the
   committed target is kept and the marker is cleared. A rebalancer pod
   restart during a transition rolls back on its first pass, since progress
   cannot be resumed safely.

The executor remains the **only** writer of Deployment replica counts; KEDA /
HPA must not target P/D Deployments directly.
