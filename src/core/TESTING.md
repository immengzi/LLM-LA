# Testing the LA-Boom core services

This document describes the local testing pipeline for `src/core`: Python unit
+ component tests, Go unit tests, and a docker-compose end-to-end (e2e) test.
Everything is driven by `make`.

## Layout

```
src/core/
├── Makefile                       # top-level orchestration (test / e2e / ci)
├── services/
│   ├── router_service/            # Python router (FastAPI)
│   │   ├── Makefile               # make test
│   │   ├── requirements-dev.txt   # test deps (no transformers/tokenizers)
│   │   └── tests/                 # unit + component tests
│   ├── sidecar/                   # Python sidecar (FastAPI + workers)
│   │   ├── Makefile               # make test
│   │   ├── requirements-dev.txt
│   │   └── tests/
│   ├── go/                        # Go router + sidecar
│   │   ├── Makefile               # make test  (needs Go 1.26)
│   │   └── internal/**/*_test.go
│   ├── tests/                     # standalone service tests (fingerprint mw)
│   └── prefix_hash/tests/         # skipped unless vllm+transformers present
└── tests/e2e/                     # docker-compose e2e stack + pytest suite
    ├── Makefile                   # make e2e
    ├── docker-compose.yml
    ├── mock_vllm/                 # fake OpenAI server (deterministic echo)
    ├── router.e2e.Dockerfile
    ├── sidecar.e2e.Dockerfile
    └── tests/test_e2e_pull.py
```

## Prerequisites

- Python 3.11+ (`python3`)
- Docker + `docker compose` (for e2e only)
- Go 1.26 for Go tests. If your local Go is older, run them in a container
  (see below) — CI uses the `golang:1.26` image.

The per-service Makefiles create an isolated `.venv/` inside each service dir on
first run and install that service's `requirements-dev.txt`. Nothing is
installed into your global environment. These venvs are git-ignored.

## Quick start

From `src/core`:

```bash
make test        # all Python unit + component tests (router + sidecar + services)
make test-go     # Go unit tests (requires Go 1.26)
make e2e         # docker-compose end-to-end test (build + up + test + down)
make ci          # what CI runs locally (all Python suites)
make clean       # remove all venvs and caches
```

Run a single suite:

```bash
make test-router
make test-sidecar
make test-services
```

Or invoke a service directly:

```bash
cd services/router_service && make test        # or: make test-cov
cd services/sidecar        && make test
```

## What is covered

### Python — unit tests (pure logic, no network)
- **router**: `config`, `kv_aware`, `prefix_hash` (with a fake tokenizer so no
  multi-GB model download), `predictors`, `latency_predictor`, `admission`,
  `slo_state`, `slo_scoring`, `len_select`, `models`.
- **sidecar**: `config`, `local_queue`, tool-call merge/complete, ZMQ endpoint
  resolution + msgpack decode + Redis projection (via `fakeredis`).
- **services**: `fingerprint_middleware` (JSON + SSE patching).

### Python — component tests (FastAPI `TestClient`, in-process)
- **router**: `pull_for_endpoint` scheduling (FIFO, KV-aware, length-aware,
  fixed batch, fairness, affinity) and endpoints (`/health/router`, `/metrics`,
  `/health/backends`, `/pull`, `/result`, `/submit`, `/enqueue`, `/debug/slo`,
  `/latency_log`).
- **sidecar**: `/metrics`, `/health`, `/push`.

External dependencies are faked: Redis via `fakeredis`, the tokenizer via a
lightweight stub, and side-effecting calls (KV registration, backend discovery)
are monkeypatched. `PYTHONHASHSEED=0` is enforced so `prefix_hash` is
deterministic.

### Go — unit tests (`services/go`)
- **sidecar**: local queue FIFO/inflight accounting, config load + normalization,
  vLLM payload building, tool-call merge/complete, `toBool`/`toInt` helpers.
- **gateway**: `ROUTER_STRATEGY` → KV/affinity mapping, `ROUTER_MODE`
  normalization + aliases, enum fallbacks, numeric clamps (plus the existing
  parity/affinity tests).

Run in a container if you don't have Go 1.26:

```bash
docker run --rm -v "$PWD/services/go":/src -w /src golang:1.26 go test -race ./...
```

### End-to-end (`tests/e2e`)
`docker compose` brings up **real Redis**, a **mock vLLM** (deterministic
OpenAI-compatible echo server), the **router**, and the **sidecar** in pull
mode. The pytest suite drives the router's OpenAI API and asserts the full round
trip:

```
client → router /v1/chat/completions → central queue → sidecar /pull
       → mock vLLM /v1/chat/completions → sidecar /result → router → client
```

It verifies non-streaming, streaming (SSE), and repeated requests, plus router
and sidecar health.

```bash
make e2e                  # one-shot: build, up, test, down
# iterative:
cd tests/e2e
make up                   # build + start stack in background
make test                 # run pytest against the running stack
make logs                 # tail logs
make down                 # stop + remove
```

Ports exposed on the host: router `18080`, sidecar `19000`. Override the target
with `ROUTER_E2E_URL` / `SIDECAR_E2E_URL` to point the suite at a remote stack.

## Pre-commit hooks

Git hooks enforce lint correctness and basic hygiene before code is committed.
Config lives at the repo root: [.pre-commit-config.yaml](../../.pre-commit-config.yaml).

Install once (from the repo root or via the Makefile):

```bash
make -C src/core hooks       # pip install pre-commit + install git hooks
# equivalent to:
pip install pre-commit && pre-commit install
```

What runs, and when:

- On **commit** (staged files only, so untouched legacy code is never blocked):
  - hygiene: trailing whitespace, end-of-file newline, YAML/JSON validity,
    accidental large files, merge-conflict markers, private keys
  - **ruff** correctness lint (`--select E9,F`): syntax errors, undefined names,
    unused imports/variables — with autofix. Intentionally *not* style/line-length,
    so it flags real bugs without churning existing formatting.
  - **gofmt** on changed `.go` files
- On **push**: the full Python unit suite (`make -C src/core test-py`, ~7s).

Run all hooks across the whole repo manually (lint/format audit):

```bash
make -C src/core lint        # = pre-commit run --all-files
```

Go `go vet` / `go test` (need Go 1.26) and the e2e stack are not run by the local
hooks — they run in CI.

## Continuous integration

`.github/workflows/ci.yml` runs on pushes/PRs that touch `src/core/**`:

- **lint** — runs the pre-commit hooks on the PR's changed files (PRs only).
- **python-tests** — matrix (router / sidecar / services), Python 3.11, coverage.
- **go-tests** — `golang:1.26` container, `go vet` + `go test -race`.
- **docker-build** — builds the self-contained sidecar production image.
- **helm-lint** — lints `vllm-kv-stack`.
- **e2e** — builds and runs the compose stack, executes the e2e suite, tears down.

## Notes / gotchas

- The router's `requirements-dev.txt` intentionally omits `transformers` /
  `tokenizers`. Tests exercise the inline-hash path with a fake tokenizer; the
  e2e stack runs with `ROUTER_STRATEGY=none` / `KV_AWARE=false` so no tokenizer
  or Kubernetes API is needed.
- Production Go and `prefix_hash` images require a `build.sh` prestep or a
  private base image, so they are not built in CI; the e2e job proves the core
  code packages and runs via the lightweight e2e Dockerfiles.
- If a service's dev deps change, delete its `.venv/` (or run `make clean`) so
  the Makefile reinstalls on the next `make test`.
