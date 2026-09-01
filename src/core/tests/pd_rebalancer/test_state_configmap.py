"""Controller-owned state ConfigMap bootstrap (GET 404 -> create)."""
from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any, Callable, Optional

import pytest


MODULE_PATH = Path(__file__).parents[2] / "vllm-kv-stack" / "files" / "pd_rebalancer.py"
SPEC = importlib.util.spec_from_file_location("pd_rebalancer", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(MODULE)


def make_api(
    handler: Callable[..., Any],
) -> MODULE.KubernetesApi:
    api = object.__new__(MODULE.KubernetesApi)
    api.namespace = "default"
    api.request = handler  # type: ignore[method-assign]
    return api


def test_state_returns_existing_configmap_without_creating() -> None:
    existing = {"data": {"targets.json": '{"qwen":{"target":{"prefill":1,"decode":2}}}'}}
    calls: list[tuple[Any, ...]] = []

    def handler(*args: Any, **kwargs: Any) -> dict[str, Any]:
        calls.append((args, kwargs))
        return existing

    api = make_api(handler)
    state = api.state("pd-state")
    assert state == existing
    assert len(calls) == 1
    assert calls[0][0][0] == "GET"


def test_state_creates_empty_configmap_on_first_use() -> None:
    created = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "pd-state", "namespace": "default"},
        "data": {"targets.json": "{}"},
    }
    calls: list[tuple[Any, ...]] = []

    def handler(
        method: str,
        path: str,
        body: Optional[dict[str, Any]] = None,
        content_type: str = "application/merge-patch+json",
        allow_404: bool = False,
    ) -> Any:
        calls.append((method, path, body, content_type, allow_404))
        if method == "GET":
            return None  # request() maps a 404 with allow_404=True to None
        return created

    api = make_api(handler)
    state = api.state("pd-state")
    assert state == created
    assert [call[0] for call in calls] == ["GET", "POST"]
    assert calls[1][1].endswith("/api/v1/namespaces/default/configmaps")
    assert calls[1][3] == "application/json"
    assert calls[1][2] == {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "pd-state", "namespace": "default"},
        "data": {"targets.json": "{}"},
    }


def test_state_reads_winner_when_create_races() -> None:
    winner = {"metadata": {"name": "pd-state"}, "data": {"targets.json": "{}"}}
    calls: list[str] = []

    def handler(
        method: str,
        path: str,
        body: Optional[dict[str, Any]] = None,
        content_type: str = "application/merge-patch+json",
        allow_404: bool = False,
    ) -> Any:
        calls.append(method)
        if method == "GET":
            return None if len(calls) == 1 else winner
        raise MODULE.ApiError("already exists", 409)

    api = make_api(handler)
    state = api.state("pd-state")
    assert state == winner
    assert calls == ["GET", "POST", "GET"]


def test_state_propagates_non_conflict_create_errors() -> None:
    def handler(
        method: str,
        path: str,
        body: Optional[dict[str, Any]] = None,
        content_type: str = "application/merge-patch+json",
        allow_404: bool = False,
    ) -> Any:
        if method == "GET":
            return None
        raise MODULE.ApiError("forbidden", 403)

    api = make_api(handler)
    with pytest.raises(MODULE.ApiError) as exc:
        api.state("pd-state")
    assert exc.value.code == 403
