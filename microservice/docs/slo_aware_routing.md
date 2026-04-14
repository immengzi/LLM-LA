# SLO-Aware Pull-Based Routing

This document describes the theory, configuration surface, and interface of
the SLO-aware routing feature added to the pull-based KV-cache router.

Back to overview: `README.md`.  
Related: `router_service.md`, `config_knobs.md`.

---

## 1. Problem Statement

The existing router optimizes for KV cache hit rate and throughput. Two
requests with identical prompts but different urgency (one needs 200 ms TTFT,
another tolerates 2 s) are treated identically. Under load this causes SLO
violations for latency-sensitive requests that happen to land behind less
urgent ones in the queue.

SLO-aware routing adds **deadline awareness** to the pull scoring loop so
that requests closest to missing their SLO are served first.

---

## 2. Theory

### 2.1 SLO Types

Four SLO types are supported:

| Type | Meaning | Deadline semantics |
|------|---------|-------------------|
| `ttft` | Time to first token | Absolute timestamp: `arrival + budget_ms` |
| `tpot` | Time per output token | Per-token budget in seconds (not a timestamp) |
| `ttft+tpot` | Compound | Two independent deadlines; tighter one drives scheduling |
| `e2e` | End-to-end completion | Absolute timestamp: `arrival + budget_ms` |

### 2.2 Slack-Based Scheduling

Requests are sorted by **slack**:

```
slack = deadline - predicted_completion_time
```

- Positive slack: ahead of schedule (lower priority).
- Zero: exactly at deadline.
- Negative: predicted to miss (highest priority, last-chance attempt).
- `+inf`: no SLO annotation (served when no urgent work exists).

This is a continuous sort with no tier boundaries or threshold parameters.

### 2.3 Cache-Aware Latency Decomposition

Prefill time decomposes into **compute** (cold tokens) and **load** (cached
KV blocks):

```
t_prefill = t_compute(b, l_cold) + t_load(b, l_cached)

t_compute(b, l_cold) = α_c·b·l_cold + β_c·b + γ_c·l_cold + δ_c
t_load(b, l_cached)  = α_l·b·l_cached + β_l·l_cached + δ_l

where l_cold   = input_tokens - cached_tokens
      l_cached = prefix_len(endpoint, req_id) × block_size_tokens
      b        = batch_size_estimate
```

Per-token decode time (unaffected by cache state):

```
τ_decode(b, l_a) = α_d·b·l_a + β_d·b + γ_d·l_a + δ_d
```

Coefficients are fitted by least-squares from an offline profiling sweep
(`profiling/profile_latency.py`) and stored in a JSON file.

### 2.4 Slack Computation Per SLO Type

For each (request, endpoint) pair during pull scoring:

- **TTFT**: `slack = deadline_ttft - (now + queue_wait + predicted_ttft)`
- **TPOT**: `slack = tpot_budget_s - predicted_tpot`
- **TTFT+TPOT**: `slack = min(ttft_slack, tpot_slack)`, binding = whichever is smaller
- **e2e**: `slack = deadline_e2e - (now + queue_wait + predicted_e2e)`, binding inferred from latency breakdown

### 2.5 Secondary Sort (Within Equal-Slack Bands)

Slack is quantized to 100 ms bands. Within a band:

| Binding constraint | `SLO_WITH_KV=true` | `SLO_WITH_KV=false` |
|---|---|---|
| TTFT-bound | Prefer highest KV cache hits (cache helps TTFT) | Pure slack order |
| TPOT-bound | Prefer least-loaded endpoint (cache irrelevant) | Pure slack order |
| Negative slack | Skip KV, prefer least-loaded (Step 8 bypass) | Pure slack order |

### 2.6 Admission Control

Two mechanisms, composable:

1. **Fixed batch cap** (`FIXED_BATCH_SIZE=N`): hard ceiling on items returned per pull.
2. **Dynamic throttle** (`ADMISSION_THROTTLE=true`): binary search for largest N
   where `predict_tpot(inflight + N, l_a_avg) <= tpot_budget`.

When both are active, the fixed cap is the ceiling and the dynamic model can
throttle below it.

### 2.7 Predictor Progression

Each predictor dimension supports multiple implementations behind a common
interface, selectable by config:

**Output length** (`OUTPUT_LEN_PREDICTOR`):
- `simple` — char-length heuristic (legacy default).
- `distribution` — per-task-type running median from completions.
- `regression` — per-type linear fit: output = a × input + b.
- `hint_only` — use client-supplied hint, fall back to char-length.

**Batch size** (`BATCH_SIZE_ESTIMATE`):
- `fixed` — operator-configured constant (`FIXED_BATCH_ESTIMATE`).
- `inflight` — router's logical inflight count per endpoint.
- `reported` — actual vLLM running batch (requires sidecar extension).

**Latency** (`LATENCY_PREDICTOR`):
- `linear` — offline profiled analytical model.
- `piecewise` — separate coefficients per concurrency range.
- `bayesian` — online RLS with forgetting factor from live observations.
- `hybrid` — offline prior + online update (same as bayesian with warm start).

---

## 3. Configuration Surface

All behavior is gated behind env vars parsed in `router/config.py`. When all
new knobs are at defaults, the system behaves identically to today.

### 3.1 Master Routing Switches

| Env var | Type | Default | Purpose |
|---------|------|---------|---------|
| `SLO_AWARE` | bool | `false` | Master switch. When false, existing KV+length path is used byte-for-byte. |
| `SLO_WITH_KV` | bool | `true` | Use KV affinity as secondary sort within equal-slack bands. |
| `ADMISSION_THROTTLE` | bool | `false` | Enable dynamic admission control (binary search on TPOT). |
| `FIXED_BATCH_SIZE` | int | `0` | Hard cap on items per pull (0 = no cap). |

### 3.2 Predictor Knobs

| Env var | Type | Default | Values |
|---------|------|---------|--------|
| `OUTPUT_LEN_PREDICTOR` | str | `simple` | `simple`, `distribution`, `regression`, `hint_only` |
| `BATCH_SIZE_ESTIMATE` | str | `fixed` | `fixed`, `inflight`, `reported` |
| `FIXED_BATCH_ESTIMATE` | int | `8` | Used when `BATCH_SIZE_ESTIMATE=fixed` |
| `LATENCY_PREDICTOR` | str | `linear` | `linear`, `piecewise`, `bayesian`, `hybrid` |
| `LATENCY_ONLINE_UPDATE` | bool | `false` | Feed live observations back to latency predictor |
| `LATENCY_PROFILE_PATH` | str | `""` | Path to `latency_profile.json` from offline profiling |

### 3.3 Queue Wait and Chunked Prefill

| Env var | Type | Default | Values |
|---------|------|---------|--------|
| `QUEUE_WAIT_MODEL` | str | `none` | `none`, `simple`, `drain_rate` |
| `CHUNKED_PREFILL_AWARE` | bool | `false` | Apply chunked-prefill correction to TTFT prediction |
| `MAX_NUM_BATCHED_TOKENS` | int | `0` | vLLM's max batched tokens (for chunked prefill) |

### 3.4 Operational Mode Matrix

| SLO_AWARE | ADMISSION_THROTTLE | FIXED_BATCH_SIZE | Behavior |
|---|---|---|---|
| false | -- | -- | Existing system, pure KV affinity |
| true | false | 0 | SLO reordering only |
| true | true | 0 | SLO reordering + dynamic throttling |
| true | true | N | SLO reordering + dynamic throttling capped at N |
| true | false | N | SLO reordering + fixed batch cap |

---

## 4. Client Interface

### 4.1 EnqueueRequest SLO Fields

All fields are optional. Existing clients are completely unaffected.

```json
{
  "prompt": "Summarize the following document...",
  "slo_type": "ttft",
  "slo_ttft_ms": 200.0,
  "slo_tpot_ms": null,
  "slo_e2e_ms": null,
  "task_type": "summarize",
  "output_len_hint": 150
}
```

| Field | Type | Description |
|-------|------|-------------|
| `slo_type` | `str?` | `"ttft"`, `"tpot"`, `"ttft+tpot"`, `"e2e"`, or null |
| `slo_ttft_ms` | `float?` | TTFT budget in milliseconds |
| `slo_tpot_ms` | `float?` | Per-token decode budget in milliseconds |
| `slo_e2e_ms` | `float?` | End-to-end budget in milliseconds |
| `task_type` | `str?` | Task label for the output-length predictor (e.g. `"chat"`, `"code"`) |
| `output_len_hint` | `int?` | Client hint for expected output length (overrides predictor) |

### 4.2 Example: TTFT SLO

```json
POST /enqueue
{
  "prompt": "...",
  "slo_type": "ttft",
  "slo_ttft_ms": 300
}
```

The router converts this to an absolute deadline (`arrival_time + 0.3s`) and
routes the request by slack.

### 4.3 Example: Compound SLO

```json
POST /enqueue
{
  "prompt": "...",
  "slo_type": "ttft+tpot",
  "slo_ttft_ms": 500,
  "slo_tpot_ms": 30,
  "task_type": "chat"
}
```

Both deadlines are tracked independently. The binding constraint (whichever
slack is tighter) drives scheduling priority and determines whether KV
affinity or load-balancing is used as secondary sort.

### 4.4 Example: No SLO (Backward Compatible)

```json
POST /enqueue
{
  "prompt": "..."
}
```

Requests without SLO annotations get `slack = +inf` and sort to the back.
They are NOT starved -- they are served whenever no urgent requests exist.
Under low load they experience no degradation.

---

## 5. Debug Endpoints

### GET /debug/slo/{req_id}

Returns full SLO state for a single request:

```json
{
  "req_id": "abc123",
  "slo_type": "ttft",
  "deadline_ttft": 1713012345.300,
  "arrival_ts": 1713012345.0,
  "input_tokens": 256,
  "predicted_output_len": 128,
  "predicted_ttft": 0.045,
  "slack": 0.255,
  "binding_constraint": "ttft",
  "assigned_endpoint": "vllm-pod-0",
  "actual_ttft": 0.038,
  "actual_output_len": 115
}
```

### GET /debug/slo

Returns aggregate summary:

```json
{
  "total": 42,
  "with_slo": 30,
  "by_type": {"ttft": 20, "e2e": 8, "ttft+tpot": 2, "none": 12}
}
```

---

## 6. Prometheus Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `router_slo_slack_seconds` | Histogram | Slack at dispatch time |
| `router_slo_predicted_miss_total` | Counter | Requests with negative slack at dispatch |
| `router_slo_actual_miss_total` | Counter | Requests that missed SLO (measured on /result) |
| `router_slo_actual_met_total` | Counter | Requests that met SLO |
| `router_output_len_error_ratio` | Histogram | `(predicted - actual) / actual` output length error |
| `router_ttft_prediction_error_seconds` | Histogram | TTFT prediction error (seconds) |
| `router_e2e_prediction_error_seconds` | Histogram | E2E prediction error (seconds) |
| `router_slo_registry_size` | Gauge | Current entries in SLO registry |

**Key dashboards to build:**

1. **SLO attainment**: `rate(router_slo_actual_met_total) / (rate(router_slo_actual_met_total) + rate(router_slo_actual_miss_total))`
2. **Slack distribution**: `router_slo_slack_seconds` histogram over time.
3. **Predictor accuracy**: `router_output_len_error_ratio` and `router_ttft_prediction_error_seconds` histograms should be centered near zero.

---

## 7. Module Map

| Module | Purpose |
|--------|---------|
| `router/slo_state.py` | Per-request SLO registry (in-memory, thread-safe) |
| `router/latency_predictor.py` | Latency predictor interface + linear/piecewise/Bayesian implementations |
| `router/slo_scoring.py` | Slack computation, batch size estimation, queue wait estimation |
| `router/admission.py` | Dynamic admission control (`compute_max_safe_admit`) |
| `router/predictors.py` | Output length predictor interface + simple/distribution/regression/hint implementations |
| `router/router_state.py` | Pull routing: `_legacy_sort()` (existing) vs `_slo_aware_sort()` (new) |
| `profiling/profile_latency.py` | Offline profiling script (not runtime code) |

---

## 8. Deployment Sequence

Each step is independently deployable and reversible by toggling its config
knob back to default. No changes to vLLM, sidecar, or Redis.

```
1. Deploy with SLO_AWARE=false.  Metadata plumbing only, no behavior change.
2. Set OUTPUT_LEN_PREDICTOR=distribution.  Learning from completions, no routing change.
3. Run offline profiling on actual hardware → latency_profile.json.
4. Deploy with SLO_AWARE=true.  Shadow mode: log slack metrics, no routing change yet.
   Observe Prometheus for 24-48h.  Calibrate FIXED_BATCH_ESTIMATE.
5. Slack-based reordering is now active.  A/B test against baseline.
6. Set FIXED_BATCH_SIZE=N from profiling.  Verify TPOT compliance.
7. Set ADMISSION_THROTTLE=true.  Compare against fixed cap.
8. Verify SLO_WITH_KV behavior.  Monitor TTFT vs TPOT workloads.
9. Optionally: QUEUE_WAIT_MODEL=simple, CHUNKED_PREFILL_AWARE=true.
10. Optionally: upgrade predictors based on logged error data.
```

---

## 9. Design Constraints

1. **Existing path preserved.** `SLO_AWARE=false` runs `_legacy_sort()` which
   is the original KV+length code extracted into a method with zero logic changes.

2. **In-memory only.** SLO state is never written to Redis. Updated on every
   pull and result event — too hot for network round-trips.

3. **Hot path is fast.** `compute_slack()` is a few multiply-adds per
   (request, endpoint) pair. No network calls, no heavy computation.

4. **Requests without SLOs are not second-class.** They get `slack=+inf` and
   sort to the back, but are served when no urgent work exists.

5. **Prediction error is a first-class metric.** Every `/result` callback
   compares predictions against reality and exposes errors as Prometheus
   histograms.
