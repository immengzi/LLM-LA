# End-to-end tests: docker-compose stack (redis + mock vLLM + router + sidecar)
# plus a host-side pytest suite that drives the router's OpenAI API.
#
# Quick start:
#   make e2e        # build + up + wait + test + down  (one-shot, CI-friendly)
#
# Iterative:
#   make up         # build + start the stack in the background
#   make test       # run the pytest suite against the running stack
#   make logs       # tail service logs
#   make down       # stop + remove the stack

COMPOSE := docker compose
VENV := .venv
PY := $(VENV)/bin/python

.PHONY: e2e up down test logs venv clean

e2e: up
	@echo "[e2e] running test suite ..."
	@$(MAKE) test; status=$$?; $(MAKE) down; exit $$status

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f

venv: $(VENV)/.installed

$(VENV)/.installed: requirements.txt
	python3 -m venv $(VENV)
	$(PY) -m pip install -U pip
	$(PY) -m pip install -r requirements.txt
	touch $@

test: venv
	$(PY) -m pytest

clean:
	rm -rf $(VENV) .pytest_cache
	-$(COMPOSE) down -v
