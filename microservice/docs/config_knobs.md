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
