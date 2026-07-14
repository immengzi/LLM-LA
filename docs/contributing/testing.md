# Testing

LA-Boom's core services (`src/core`) have a local + CI testing pipeline: Python
unit and component tests, Go unit tests, and a docker-compose end-to-end test.
Everything is driven by `make`. The canonical, in-repo reference is
[`src/core/TESTING.md`](../../src/core/TESTING.md); this page is the summary.

## Quick start

```bash
make -C src/core test        # all Python unit + component tests
make -C src/core test-go     # Go unit tests (requires Go 1.26)
make -C src/core e2e         # docker-compose end-to-end test
make -C src/core ci          # what CI runs locally (Python suites)
```

Run a single suite:

```bash
make -C src/core test-router
make -C src/core test-sidecar
make -C src/core test-services
```

## What is covered

- **Python unit tests** — router (`config`, `kv_aware`, `prefix_hash` with a fake
  tokenizer, `predictors`, `latency_predictor`, `admission`, `slo_state`,
  `slo_scoring`, `len_select`, `models`), sidecar (`config`, `local_queue`,
  tool-call merge, ZMQ/msgpack/Redis via `fakeredis`), and the fingerprint
  middleware. No GPU or model download required.
- **Python component tests** — router `pull_for_endpoint` scheduling and every
  FastAPI endpoint, sidecar `/health` `/push` `/metrics`, via FastAPI's
  `TestClient` with faked dependencies.
- **Go unit tests** — sidecar (queue, config, vLLM payload + tool-call helpers)
  and gateway config (strategy/mode mapping, enum fallbacks, clamps).
- **End-to-end** — `docker compose` brings up real Redis, a deterministic mock
  vLLM (OpenAI-compatible echo server), the router, and the sidecar in pull
  mode; the suite drives the router's OpenAI API and asserts the full round trip
  (non-streaming, streaming SSE, repeated requests).

## Go tests without Go 1.26 locally

```bash
docker run --rm -v "$PWD/src/core/services/go":/src -w /src golang:1.26 \
  go test -race ./...
```

## End-to-end details

```bash
cd src/core/tests/e2e
make up      # build + start the stack
make test    # run pytest against it
make down    # stop + remove
```

Host ports: router `18080`, sidecar `19000`. Override targets with
`ROUTER_E2E_URL` / `SIDECAR_E2E_URL`.

## Continuous integration

`.github/workflows/ci.yml` runs on pushes/PRs touching `src/core/**`:

- **lint** — pre-commit hooks on the PR's changed files.
- **python-tests** — matrix (router / sidecar / services), Python 3.11, coverage.
- **go-tests** — `golang:1.26`, `go vet` + `go test -race`.
- **docker-build** — builds the self-contained sidecar image.
- **helm-lint** — lints `vllm-kv-stack`.
- **e2e** — builds and runs the compose stack, executes the suite, tears down.
