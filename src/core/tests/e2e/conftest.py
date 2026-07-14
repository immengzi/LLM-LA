"""Shared fixtures for the e2e suite.

These tests assume the docker-compose stack in this directory is already up
(``make e2e`` / ``make e2e-up`` do this). Base URLs are overridable via env so
the same tests can run against a remote stack.
"""
import os
import time

import pytest
import requests

ROUTER_URL = os.getenv("ROUTER_E2E_URL", "http://localhost:18080")
SIDECAR_URL = os.getenv("SIDECAR_E2E_URL", "http://localhost:19000")


def _wait_ready(url: str, timeout_s: float = 60.0) -> None:
    deadline = time.time() + timeout_s
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            r = requests.get(url, timeout=3)
            if r.status_code == 200:
                return
        except Exception as e:  # noqa: BLE001 - retry loop
            last_err = e
        time.sleep(1.0)
    raise RuntimeError(f"service not ready at {url}: {last_err}")


@pytest.fixture(scope="session")
def router_url() -> str:
    _wait_ready(f"{ROUTER_URL}/health/router")
    return ROUTER_URL


@pytest.fixture(scope="session")
def sidecar_url() -> str:
    _wait_ready(f"{SIDECAR_URL}/health")
    return SIDECAR_URL
