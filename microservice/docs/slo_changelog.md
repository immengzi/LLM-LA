# SLO-Aware Routing — Changelog

Track of all files added/modified for the SLO-aware routing feature.
Use this when copying changes to the external router_service repo.

---

## Files to Copy

### New Files (create these)

| File | Copy to | Description |
|------|---------|-------------|
| `services/router_service/router/slo_state.py` | `router/slo_state.py` | Per-request SLO registry |
| `services/router_service/router/latency_predictor.py` | `router/latency_predictor.py` | Latency predictor interface + implementations |
| `services/router_service/router/slo_scoring.py` | `router/slo_scoring.py` | Slack computation, batch/queue estimators |
| `services/router_service/router/admission.py` | `router/admission.py` | Dynamic admission control |
| `services/router_service/profiling/profile_latency.py` | `profiling/profile_latency.py` | Offline latency profiler (not runtime) |

### Modified Files (diff carefully)

| File | Copy to | What changed |
|------|---------|--------------|
| `services/router_service/router/config.py` | `router/config.py` | +15 SLO config knobs in dataclass + env parsing |
| `services/router_service/router/models.py` | `router/models.py` | +5 optional SLO fields on `EnqueueRequest` |
| `services/router_service/router/predictors.py` | `router/predictors.py` | Full rewrite: `OutputLengthPredictor` interface + 4 implementations + factory |
| `services/router_service/router/metrics.py` | `router/metrics.py` | +8 Prometheus metrics (Histogram import, SLO counters/histograms/gauges) |
| `services/router_service/router/api.py` | `router/api.py` | SLO registration, predictor wiring, result ingestion, debug endpoints |
| `services/router_service/router/router_state.py` | `router/router_state.py` | Refactored `pull_for_endpoint` into legacy/SLO branches, admission control |
| `services/router_service/router/kv_watcher.py` | `router/kv_watcher.py` | +`last_scan_ts` tracking per endpoint for staleness discount |

### Documentation (optional, local only)

| File | Description |
|------|-------------|
| `docs/slo_aware_routing.md` | Theory, interface, and deployment guide |
| `docs/slo_changelog.md` | This file |

---

## Detailed Change Log

### config.py

**Dataclass additions** (after `TRACE_SAMPLING_RATE`):

```
SLO_AWARE: bool = False
SLO_WITH_KV: bool = True
ADMISSION_THROTTLE: bool = False
FIXED_BATCH_SIZE: int = 0
OUTPUT_LEN_PREDICTOR: str = "simple"
BATCH_SIZE_ESTIMATE: str = "fixed"
FIXED_BATCH_ESTIMATE: int = 8
LATENCY_PREDICTOR: str = "linear"
LATENCY_ONLINE_UPDATE: bool = False
LATENCY_PROFILE_PATH: str = ""
QUEUE_WAIT_MODEL: str = "none"
CHUNKED_PREFILL_AWARE: bool = False
MAX_NUM_BATCHED_TOKENS: int = 0
```

**get_config() additions** (before `# TRACE overrides`):
- ~30 lines of env var parsing with normalization and allowlists.

### models.py

**EnqueueRequest** — 5 new optional fields added after `meta`:

```python
slo_type: Optional[str] = None
slo_ttft_ms: Optional[float] = None
slo_tpot_ms: Optional[float] = None
slo_e2e_ms: Optional[float] = None
task_type: Optional[str] = None
output_len_hint: Optional[int] = None
```

### predictors.py

**Full rewrite.** Old file was 23 lines. New file has:
- `OutputLengthPredictor` abstract interface
- `SimpleLengthPredictor` (identical logic to old, now inherits interface)
- `HintOnlyPredictor`
- `TaskTypeDistributionPredictor` (per-type running stats with capped window)
- `InputLengthRegressionPredictor` (online OLS)
- `get_output_length_predictor()` factory (selects by `OUTPUT_LEN_PREDICTOR` config)
- `get_length_predictor()` legacy accessor (backward-compatible for `len_select.py`)

### metrics.py

**Added import:** `Histogram` (alongside existing `Counter`, `Gauge`).

**8 new metrics** (inserted before `# Central queue helpers`):
- `router_slo_slack_seconds` (Histogram)
- `router_slo_predicted_miss_total` (Counter)
- `router_slo_actual_miss_total` (Counter)
- `router_slo_actual_met_total` (Counter)
- `router_output_len_error_ratio` (Histogram)
- `router_ttft_prediction_error_seconds` (Histogram)
- `router_e2e_prediction_error_seconds` (Histogram)
- `router_slo_registry_size` (Gauge)

**8 new helper functions** (appended after existing helpers):
- `observe_slo_slack()`, `inc_slo_predicted_miss()`, `inc_slo_actual_miss()`,
  `inc_slo_actual_met()`, `observe_output_len_error()`,
  `observe_ttft_prediction_error()`, `observe_e2e_prediction_error()`,
  `set_slo_registry_size()`

### api.py

**New imports:**
- `from .slo_state import SLORegistry, SLOEntry`
- 8 SLO metric helpers from `.metrics`

**New globals:**
- `_slo_registry = SLORegistry(...)`

**New helper functions:**
- `_register_slo(rid, req, arrival_ts)` — registers SLO state + calls output length predictor
- `_ingest_slo_actuals(rid, result)` — feeds actuals to registry + predictors, logs prediction errors, checks SLO attainment

**Modified functions:**
- `enqueue()` — added `_register_slo(rid, req, t_start)` after `_remember_run_id`
- `submit()` — same addition
- `_ingest_result_payload()` — added calls to `_ingest_slo_actuals`, inflight decrement, online latency update

**New endpoints:**
- `GET /debug/slo/{req_id}` — per-request SLO state
- `GET /debug/slo` — aggregate summary

### router_state.py

**New imports/globals:**
- Lazy-loaded SLO deps: `_slo_registry`, `_latency_predictor`, `_batch_estimator`, `_queue_wait_estimator`
- `_ensure_slo_deps()`, `_get_slo_registry()` helpers

**Refactored `pull_for_endpoint()`:**
- Pool building (steps 1-2) unchanged.
- Sort logic extracted into two methods:
  - `_legacy_sort()` — exact original KV+length code, untouched.
  - `_slo_aware_sort()` — slack-ascending + secondary KV/load + negative-slack bypass.
- Branching: `if SLO_AWARE: _slo_aware_sort() else: _legacy_sort()`
- After sort: admission controls (fixed cap + dynamic throttle).
- Trace enrichment: adds `slo_type`, `slo_slack`, `slo_binding`, `slo_predicted_output_len`.
- SLO dispatch tracking: calls `slo_registry.update_dispatch()` for chosen items.
- SLO metrics: observes slack histogram + predicted miss counter at dispatch.

**New methods:**
- `_legacy_sort(pool, endpoint)` — extracted from old `pull_for_endpoint`
- `_slo_aware_sort(pool, endpoint, want)` — new slack-based ordering
- `_apply_admission_throttle(current_want, endpoint)` — dynamic TPOT throttle

### kv_watcher.py

**KVWatcher.__init__:**
- Added `self._last_scan_ts: Dict[str, float] = {}` and `self._scan_lock`

**New methods:**
- `get_last_scan_ts(endpoint)` — returns last scan timestamp for staleness discount
- `get_all_last_scan_ts()` — returns all endpoints' timestamps

**Modified `_scan_once()`:**
- After `register_block_owners()`, records `self._last_scan_ts[ep] = time.time()`

---

## Dependencies

No new pip dependencies. The feature uses only stdlib + existing deps
(`prometheus_client`, `pydantic`).

The offline profiler (`profiling/profile_latency.py`) requires `numpy` and
`requests`, which are not router runtime deps. Install separately when
running the profiler.

---

## Rollback

Set `SLO_AWARE=false` (the default). All SLO code paths are skipped.
The `_legacy_sort()` path is identical to the original code.

To fully remove: revert all files listed above and delete the new files.
