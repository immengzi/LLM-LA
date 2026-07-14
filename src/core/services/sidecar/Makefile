# Sidecar service: unit + component tests.
#
# Quick start:
#   make test          # create venv (first run), then run pytest
#   make test-cov      # tests with coverage report
#   make clean         # remove the local venv

VENV := .venv
PY := $(VENV)/bin/python
PIP := $(VENV)/bin/pip

.PHONY: test test-cov lint venv clean

venv: $(VENV)/.installed

$(VENV)/.installed: requirements-dev.txt
	python3 -m venv $(VENV)
	$(PY) -m pip install -U pip
	$(PY) -m pip install -r requirements-dev.txt
	touch $@

test: venv
	$(PY) -m pytest

test-cov: venv
	$(PY) -m pytest --cov=sidecar --cov-report=term-missing

lint: venv
	$(PY) -m pyflakes sidecar || true

clean:
	rm -rf $(VENV) .pytest_cache
