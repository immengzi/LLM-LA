# Configuration Knobs

This file documents the knobs exposed via `example_config.yaml`
and parsed by `config.py`.

Back to overview: `README.md`.

---

## Top-level keys

### `switch_cluster: str | null`

Selects a cluster profile from the `clusters.yaml` file next to the experiment config. The selected profile supplies cluster-local defaults such as endpoints, host paths, registry settings, image names, and Mooncake/pinning values.

Explicit values in the experiment config override values from the profile. See `docs/switch_cluster.md` for the full workflow. For the current BZ cluster's `reg.local:32000` registry setup and verification, see the BZ cluster registry record in `docs/operations/registry.md`.

---

### `router_url: str`

Base URL of the router service, e.g.:

    router_url: "http://127.0.0.1:30080"

All `/enqueue` requests are sent to `router_url + "/enqueue"`.

---

### `total_requests: int`

Number of **timed** requests in the main test (does not include warmup).

---

### `prompt_source: str`

Controls how prompts are built:

- `"file"` – use `file_prompts` section.
- `"hf-lmsys"` – use `hf_lmsys` section.
- `"codeflow"` – use the CodeFlowBench dataset (see [codeflowbench](../benchmarking/codeflowbench.md)).

---

## `file_prompts` section

Used when `prompt_source: "file"`.

Fields (see `FilePromptsConfig`):

- `path: str`
  Path to a JSON file with keys: `short`, `medium`, `long`.

- `variant: str`
  Which key to use: one of `short`, `medium`, `long`.
  The chosen string is repeated `total_requests` times.

Example:

    file_prompts:
      path: "prompts.json"
      variant: "medium"

---

## `hf_lmsys` section

Used when `prompt_source: "hf-lmsys"`.

Fields (see `HFLmsysConfig`):

- `dataset_name: str`
  HF dataset name or local path (e.g. `"lmsys/lmsys-chat-1m"`).

- `split: str`
  Dataset split, e.g. `"train"`.

- `tokenizer_name: str`
  HF tokenizer name or local tokenizer directory.

- `streaming: bool`
  If `true`, uses streaming mode for datasets.

- `min_input_tokens: int | null`
- `max_input_tokens: int | null`

  Input prompts outside this token range are skipped.

- `repeat_each: int`
  Each accepted example is repeated this many times.

---

## `load_pattern` section

Controls timing of the **main** request stream. For how each pattern shapes traffic (the open-loop model and when to use which), see [load patterns](../benchmarking/load-patterns.md); this section is the field reference.

Fields (see `LoadPatternConfig`):

### `pattern: str`

One of:

- `dump`
- `det`
- `poisson`
- `bursty`
- `steps`
- `rand`

### `rate_rps: float`

Base requests-per-second, used by several patterns
(`det`, `poisson`, `steps`, `rand` defaults).

### `duration_s: float`

Duration of the main test window.

---

### `warmup_reqs: int`

Number of **dummy warmup requests** sent before the timed schedule.

Semantics:

- If `warmup_reqs <= 0`: no warmup.
- If `warmup_reqs > 0`:
  - Client sends that many dummy prompts.
  - Each warmup request uses the configured `backend` path (router `/enqueue`, or the `aibrix`/`litellm`/`boom` client), not always `/enqueue`.
  - Client waits for all responses.
  - Only then does the main timed load start.

This warms model workers, caches, and router paths.

---

### Bursty mode (`pattern: "bursty"`)

- `burst_on_s: float`
  Length of ON window.

- `burst_off_s: float`
  Length of OFF window.

- `burst_rps_on: float`
  RPS during ON windows.

- `burst_rps_off: float`
  RPS during OFF windows.

The scheduler alternates ON and OFF until `duration_s`
or `total_requests` is exhausted.

---

### Steps mode (`pattern: "steps"`)

- `step_schedule: str`

  Comma-separated `offset:rps` pairs.

  Example:

      step_schedule: "0:5,30:15,60:3"

Meaning:

- from 0s to 30s → 5 RPS
- from 30s to 60s → 15 RPS
- from 60s onward (up to `duration_s`) → 3 RPS

Within each second, timestamps are evenly spaced.

---

### Random mode (`pattern: "rand"`)

- `rand_rps_min: float`
- `rand_rps_max: float`
- `rand_epoch_s: float`
- `loadgen_seed: int`

Every `rand_epoch_s` seconds:

- a random integer RPS is drawn uniformly
  in `[rand_rps_min, rand_rps_max]`,
- that RPS is used to schedule the next epoch’s events.

`loadgen_seed` (default `12345`) is passed to the scheduler for all patterns and controls reproducibility of the stochastic ones (`rand` and `poisson`).

---

## `generation` section

Values in this section are passed through as the `meta` object in the
JSON payload for `/enqueue`. The router or backend model can interpret
these fields as generation parameters.

Fields (see `GenerationConfig`):

- `max_tokens: int`
  Upper bound on generated tokens.

- `temperature: float`
  Sampling temperature.

- `length_mode: str`
  One of:
    - `legacy`
    - `target-output`
    - `target-total`

- `target_output_tokens: int | null`
- `target_total_tokens: int | null`

Only meaningful when a non-legacy length mode is used; otherwise they
are ignored by the server side (but still sent in `meta`).

- `think: bool`
  When `true`, the request includes `enable_thinking: true` and the model
  is allowed to produce internal reasoning tokens before the final answer.
  When `false`, thinking mode is disabled.

- `min_tokens: int`
  Lower bound on generated tokens.

- `ignore_eos: bool`
  When `true`, the backend generates exactly `max_tokens` (ignores EOS) — replay/benchmark mode.

- `use_dataset_output_len: bool`
  Use the per-sample output length from the dataset instead of `max_tokens`.

- `replay_output_lengths_from: str | null`
  Path to a prior run's `logs.json`; replays each request's recorded output length.

---

## Other sections

These additional sections exist in `ClientConfig` (`config.py`); see the linked docs for details:

- `transport.mode` — `sync` | `async_pubsub` (router transport; see [load patterns](../benchmarking/load-patterns.md))
- `backend` — `router` | `aibrix` | `litellm` | `boom`
- `slo.mix` — per-request SLO injection (see [SLO-aware routing](../architecture/slo-aware-routing.md))
- `multi_model.strategy` — `fraction` | `mirror` (gateway multi-model traffic)
- top-level `multi_turn` / `hf_lmsys.multi_turn` — multi-turn conversations (see [multi-turn](../benchmarking/multi-turn.md))

---

## Example configuration

A small example (simplified):

    router_url: "http://127.0.0.1:30080"
    total_requests: 200
    prompt_source: "file"

    file_prompts:
      path: "prompts.json"
      variant: "medium"

    load_pattern:
      pattern: "det"
      rate_rps: 10.0
      duration_s: 60.0
      warmup_reqs: 2
      loadgen_seed: 12345

    generation:
      max_tokens: 256
      temperature: 0.0
      length_mode: "legacy"

Use this as a template and adjust knobs to match your experiments.

---

## Router service environment variables

These are loaded by `router/config.py` into `RouterConfig`. They are set as
environment variables on the router deployment (e.g. in Kubernetes YAML).

### Core networking

- `HOST: str`
  Bind address for the FastAPI/uvicorn server (default `"0.0.0.0"`).

- `PORT: int`
  Listen port for the router HTTP API (default `8080`).

### Redis + model identity

- `REDIS_HOST: str`
  Hostname of the Redis instance used for KV metadata.

- `REDIS_PORT: int`
  Port for Redis (default `6379`).

- `MODEL_NAME: str`
  Logical model name used to prefix KV keys in Redis.

### Hash / KV services

- `KV_HASH_SOURCE: str`
  KV-block hash source: `inline` (default; in-process for the Python router,
  in-container for the Go gateway, using `prefix_hash.py`) or `external` (legacy
  standalone `vllm-cpu-hash` service). Set via client config `router_hash_source`
  / Helm `router.hashSource`.

- `HASH_SERVICE_URL: str`
  Endpoint of the `/compute_hashes` hasher. For the Go gateway in `inline` mode
  this is the in-container hasher (`http://127.0.0.1:9095`); in `external` mode
  it is the legacy service (`http://vllm-cpu-hash:9095`). Unused by the Python
  router in `inline` mode.

- `KV_OWNER_SOURCE: str`
  Block-owner source for `prefix`/`both` routing: `lookup` (default; targeted
  per-request Redis `HGETALL` at admit, giving a truthful `kv_hit`) or `watcher`
  (legacy background scan into a shared map). Inert under `affinity`/`none`. Set
  via client config `router_owner_source` / Helm `router.ownerSource`.

- `KV_LOOKUP_MAX_BLOCKS: int`
  Cap on how many leading block hashes are looked up per request when
  `KV_OWNER_SOURCE=lookup` (default `512`). Set via client config
  `router_lookup_max_blocks` / Helm `router.lookupMaxBlocks`.

### K8s / sidecar discovery

- `NAMESPACE: str`
  Kubernetes namespace where vLLM pods live.

- `LABEL_SELECTOR: str`
  Label selector used to discover vLLM pods (e.g. `app=vllm-qwen`).

- `VLLM_PORT: int`
  vLLM HTTP port (for KV watcher’s pod → endpoint mapping).

- `SIDECAR_PORT: int`
  Sidecar HTTP port (used by push router to call `/push` and `/health`).

### KV watcher controls

- `KV_WATCH_INTERVAL_S: float`
  Interval between Redis KV scans.

- `KV_WATCH_MAX_KEYS: int`
  Max number of `kvblock` keys to scan per pass.

- `KV_DISCOVERY_INTERVAL_S: float`
  Interval between Kubernetes pod discovery runs.

- `KV_LOG_KEYS: str`
  Verbosity for KV watcher logs: `off | summary | full`.

### Routing behaviour

> These env vars are set by the chart from the client `helm:` keys. For the `helm:` ↔ chart ↔ env mapping, see [experiment configs](experiment-configs.md#client-config-and-the-helm-section).

- `ROUTER_MODE: str`
  One of:
  - `pull`
  - `push-rr`
  - `push-random`
  - `push-leastq`
  - `push-throughput`
  - `push-p2c` (aliases: `power-of-two`, `push-power-of-two`, `push-pow2`)
  - `push-kv-cost` (aliases: `kv-cost`, `push-cost`)
  - `push-least-kv` (aliases: `least-kv-cache`, `least-gpu-cache`, `push-least-gpu`)
  - `push-least-latency` (aliases: `least-latency`, `push-latency`)
  - `push-least-busy` (aliases: `least-busy-time`, `least-busy`)
  - `central-push`
  - `external-push`

  See [router.md](../architecture/router.md) for strategy descriptions. `push-kv-cost` also uses
  `ROUTER_KV_OVERLAP_CREDIT`, `ROUTER_PREFILL_LOAD_SCALE`, and `ROUTER_TEMPERATURE`
  (Helm: `router.kvCost.*`).

- `ROUTER_SIDECAR_ENABLED: bool` (default `true`)
  When `false` **and** `ROUTER_MODE` is `push-*` or `central-push`, run
  sidecar-less direct-to-vLLM delivery: the router delivers directly to each
  pod's vLLM and hosts the KV-events subscriber itself. Ignored (with a warning)
  for `pull` / `external-push`. Set via the chart from `sidecar.enabled`. See
  [router.md](../architecture/router.md#sidecar-less-push--central-push-router_sidecar_enabledfalse).

- `VLLM_KV_EVENTS_PORT: int` (default `5557`) / `VLLM_KV_EVENTS_TOPIC: str` (default `kv@`)
  Per-pod vLLM KV-cache-events ZMQ port + topic prefix the router subscribes to in
  sidecar-less push-*/central-push with prefix/`both` routing (must match the engine's
  `--kv-events-config`).

- `KV_AWARE: bool`
  Enable/disable KV-aware scoring when assigning work.

- `LEN_AWARE: bool`
  Enable/disable length-aware ordering inside the pool.

- `LEN_POLICY: str`
  Length policy when `LEN_AWARE` is true:
  - `short_first`
  - `long_first`

- `POOL_FACTOR: int`
  Pool size multiplier: router looks at `want * POOL_FACTOR` items
  when building the candidate set for a pull.

- `DEFAULT_MAX_TOKENS: int`
  Fallback predicted output length if the predictor returns no value.

### Timeouts and synchronous wait

- `RESULT_TIMEOUT_S: float`
  How long `/enqueue` waits for a `/result` before returning 504.

- `RESULT_POLL_INTERVAL_S: float`
  Sleep interval between checks inside `wait_for_result`.

- `HASH_TIMEOUT_S: float`
  Timeout for HTTP calls to the hasher (Go gateway; inline or external).

- `PUSH_TIMEOUT_S: float`
  Timeout for router → sidecar `/push` calls (push modes).

- `LEASTQ_TIMEOUT_S: float`
  Timeout for sidecar `/health` probes in `push-leastq` mode.

### Logging

- `REQ_LOG_MODE: str`
  Per-request routing logs:
  - `off`
  - `summary`
  - `full`

Separately (in the Docker entrypoint):

- `ACCESS_LOG: str`
  Controls uvicorn access log: `"true"` or `"false"`.

- `output_log_mode: str`

Controls how much of the model output is written into the client’s
per-request log file (`logs.json`).

- `"preview"`
- `"full"`

- `log_request_body: bool` (default `false`)

When `true`, the full request body (messages + sampling params, i.e. the exact
wire payload) is written into each `logs.json` record under `request_body`.
Opt-in; off by default so existing runs are unchanged. For proxy backends
(boom/litellm/aibrix) this is the OpenAI chat JSON actually sent; for the router
backend it is the `/enqueue` wire payload (`prompt` + `meta` + SLO fields).

- `request_body_max_bytes: int` (default `16384`)

Caps each logged body. Bodies whose JSON exceeds this are replaced by a bounded
marker `{"_truncated": true, "bytes": N, "preview": "..."}`. `0` means unlimited.

---

## Sidecar environment variables

These are set on the sidecar container and read by `sidecar/config.py`.

### Core endpoints

- `ROUTER_URL: str`
  Base URL of the router (e.g. `http://router-service:8080`).

- `VLLM_URL: str`
  Base URL of the local vLLM server in the same pod
  (e.g. `http://127.0.0.1:8200`).

- `MODEL_NAME: str`
  Logical model name (should match router / Redis configuration).

### Sidecar HTTP + capacity

- `SIDECAR_PORT: int`
  Port where the sidecar FastAPI server listens (for `/health`, `/push`).

- `BATCH_SIZE: int`
  Maximum number of requests allowed in `pending + inflight` for this pod.
  Also used as a target concurrency for worker threads.

- `SIDECAR_MODE: str`
  Mode of operation, currently:
  - `pull` (default) – sidecar pulls work from router via `/pull`.

### KV events + Redis

- `VLLM_HOST: str`
  Host used by the ZMQ subscriber to reach vLLM (typically `127.0.0.1`).

- `VLLM_SUB_PORT: int`
  ZMQ subscription port exposed by vLLM (`kv-events-config`).

- `REDIS_HOST: str`
  Hostname for Redis (same Redis as router).

- `REDIS_PORT: int`
  Port for Redis (default `6379`).

- `CONTAINER_NAME`
  Usually injected from pod metadata; used for logging / identification.

- `MODEL_NAME_REDIS: str`
  Model name prefix for Redis keys (should match `MODEL_NAME`).

Additional tuning knobs (if configured in code) may include worker counts and
log verbosity, but the variables above are the core interface between the
sidecar and the rest of the system.
