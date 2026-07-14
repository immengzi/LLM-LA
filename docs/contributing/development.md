# Contributing

Thanks for contributing to LA-Boom. This page covers local setup, the git
hooks, and the pull-request workflow. For the full test pipeline see
[Testing](./testing.md).

## Prerequisites

- Python 3.11+
- Docker + `docker compose` (for the end-to-end tests)
- Go 1.26 for the Go services. If your local Go is older, run Go tests in a
  container (see [Testing](./testing.md)); CI uses the `golang:1.26` image.

## Local setup

Everything is driven by `make` from `src/core`. Each service creates an isolated,
git-ignored virtualenv on first use.

```bash
# Python unit + component tests (router, sidecar, shared services)
make -C src/core test

# Go unit tests (needs Go 1.26)
make -C src/core test-go

# Docker-compose end-to-end tests (mock vLLM + Redis + router + sidecar)
make -C src/core e2e

# See all targets
make -C src/core help
```

## Pre-commit hooks

The repo ships a [.pre-commit-config.yaml](../../.pre-commit-config.yaml).
Install the git hooks once after cloning:

```bash
make -C src/core hooks
# equivalent to: pip install pre-commit && pre-commit install
```

What runs, and when (hooks only touch the files you stage, so untouched legacy
code is never blocked):

- On **commit**: hygiene (trailing whitespace, end-of-file, YAML/JSON validity,
  large files, merge markers, private keys), `ruff` correctness lint
  (`--select E9,F`), and `gofmt` on changed Go files.
- On **push**: the full Python unit suite (`make -C src/core test-py`).

Run every hook across the repo manually:

```bash
make -C src/core lint     # = pre-commit run --all-files
```

## Pull-request workflow

1. Branch off `main`.
2. Make your change with tests. Keep the tree lint-clean (`make -C src/core lint`).
3. Ensure `make -C src/core test` passes locally (Go + e2e run in CI).
4. Open a PR. CI runs lint, the Python matrix, Go 1.26 tests, an image build,
   Helm lint, and the e2e stack — see [Testing](./testing.md#continuous-integration).

## Coding conventions

- Python lint gates on correctness (pyflakes/syntax), not style; do not
  introduce undefined names, unused imports, or dead variables.
- Go code must be `gofmt`-clean and pass `go vet`.
- Keep comments focused on intent, not narration.
