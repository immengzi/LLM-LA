# Configuration Knobs

This file documents the knobs exposed via `example_config.yaml`
and parsed by `config.py`.

Back to overview: `README.md`.

---

## Top-level keys

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

Controls timing of the **main** request stream.

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
  - Each is a synchronous `/enqueue` call.
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

`loadgen_seed` controls reproducibility of the random pattern.

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

- `HASH_SERVICE_URL: str`  
  Base URL of the prefix-hash service (used for `/compute_hashes`).

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

- `ROUTER_MODE: str`  
  One of:
  - `pull`
  - `push-rr`
  - `push-random`
  - `push-leastq`

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
  Timeout for calls to the prefix-hash service.

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
