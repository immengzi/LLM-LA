# LA-Boom core: test orchestration.
#
# Common targets:
#   make test          # all Python unit + component tests (router, sidecar, services)
#   make test-router   # router service tests only
#   make test-sidecar  # sidecar service tests only
#   make test-services # standalone services tests (fingerprint middleware, ...)
#   make test-go       # Go tests (needs Go 1.26 — see note below)
#   make e2e           # docker-compose end-to-end tests
#   make ci            # what CI runs locally: all Python tests (+ Go if available)
#   make hooks         # install pre-commit + pre-push git hooks
#   make lint          # run pre-commit hooks on all files (lint/format audit)
#   make clean         # remove all venvs / caches
#
# Go note: internal/go modules require Go 1.26. If your local Go is older, use
# the CI job or:  docker run --rm -v "$$PWD/services/go":/src -w /src golang:1.26 make test

ROUTER_DIR := services/router_service
SIDECAR_DIR := services/sidecar
SERVICES_DIR := services
GO_DIR := services/go
E2E_DIR := tests/e2e

VENV := .venv
PY := $(VENV)/bin/python

.PHONY: test test-py test-router test-sidecar test-services test-go \
        e2e lint-go ci hooks lint venv clean help

# Repo root (this Makefile lives at src/core, hooks config is at the repo root).
REPO_ROOT := $(abspath $(CURDIR)/../..)

help:
	@grep -E '^#   make ' $(MAKEFILE_LIST) | sed 's/^#   /  /'

test: test-py

test-py: test-router test-sidecar test-services

test-router:
	$(MAKE) -C $(ROUTER_DIR) test

test-sidecar:
	$(MAKE) -C $(SIDECAR_DIR) test

# Standalone tests under services/ (e.g. fingerprint middleware). Reuse the
# router venv, which already has fastapi/httpx installed.
test-services:
	$(MAKE) -C $(ROUTER_DIR) venv
	cd $(SERVICES_DIR) && PYTHONHASHSEED=0 router_service/.venv/bin/python -m pytest tests

test-go:
	$(MAKE) -C $(GO_DIR) test

lint-go:
	$(MAKE) -C $(GO_DIR) vet

e2e:
	$(MAKE) -C $(E2E_DIR) e2e

ci: test-py
	@echo "[ci] Python suites passed. Run 'make test-go' in a Go 1.26 env for Go tests."

# Install the git pre-commit + pre-push hooks (uses .pre-commit-config.yaml at
# the repo root). Requires: pip install pre-commit.
hooks:
	@command -v pre-commit >/dev/null 2>&1 || { echo "Installing pre-commit..."; pip install pre-commit; }
	cd $(REPO_ROOT) && pre-commit install --install-hooks

# Run all pre-commit hooks across the whole repo (manual lint/format audit).
lint:
	@command -v pre-commit >/dev/null 2>&1 || { echo "pre-commit not found; run 'make hooks' first"; exit 1; }
	cd $(REPO_ROOT) && pre-commit run --all-files

clean:
	-$(MAKE) -C $(ROUTER_DIR) clean
	-$(MAKE) -C $(SIDECAR_DIR) clean
	-$(MAKE) -C $(GO_DIR) clean
	-$(MAKE) -C $(E2E_DIR) clean
	rm -rf $(VENV)
