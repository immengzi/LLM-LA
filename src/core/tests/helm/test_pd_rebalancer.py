"""Helm rendering tests for the quiescent P/D replica rebalancer."""
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[4]
CHART = REPO_ROOT / "src" / "core" / "vllm-kv-stack"
BASE_MODEL = (
    "models[0].name=qwen,models[0].modelSubPath=placeholder,"
    "models[0].prefillDecode.enabled=true"
)


def render(*extra: str) -> str:
    if shutil.which("helm") is None:
        pytest.skip("helm is not installed")
    result = subprocess.run(
        ["helm", "template", "test", str(CHART), "--set", BASE_MODEL, *extra],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout


def test_rebalancer_is_off_without_explicit_model_opt_in() -> None:
    assert "name: test-pd-rebalancer" not in render()


def test_rebalancer_renders_least_privilege_control_plane() -> None:
    manifest = render("--set", "models[0].prefillDecode.dynamicRebalance.enabled=true")
    for name in ("test-pd-rebalancer", "test-pd-rebalancer-code", "test-pd-rebalancer-state"):
        assert f"name: {name}" in manifest
    assert 'resources: ["deployments", "deployments/scale"]' in manifest
    assert 'resources: ["configmaps"]' in manifest
    assert "- name: PD_REBALANCER_DRY_RUN" in manifest
    assert 'value: "false"' in manifest


def test_rebalancer_dry_run_renders_true_when_enabled() -> None:
    manifest = render(
        "--set",
        "models[0].prefillDecode.dynamicRebalance.enabled=true",
        "--set",
        "pdRebalancer.dryRun=true",
    )
    assert 'value: "true"' in manifest


def test_rebalancer_rejects_mismatched_tensor_parallelism() -> None:
    if shutil.which("helm") is None:
        pytest.skip("helm is not installed")
    result = subprocess.run(
        [
            "helm",
            "template",
            "test",
            str(CHART),
            "--set",
            BASE_MODEL,
            "--set",
            "models[0].prefillDecode.dynamicRebalance.enabled=true",
            "--set",
            "models[0].prefillDecode.prefill.tensorParallelSize=1",
            "--set",
            "models[0].prefillDecode.decode.tensorParallelSize=2",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "equal prefill/decode tensorParallelSize" in result.stderr


@pytest.mark.parametrize(
    ("settings", "error"),
    [
        (
            [
                "models[0].prefillDecode.prefill.replicas=1",
                "models[0].prefillDecode.decode.replicas=1",
                "models[0].prefillDecode.dynamicRebalance.maxTotalReplicas=1",
            ],
            "maxTotalReplicas must be at least 2",
        ),
        (
            [
                "models[0].prefillDecode.prefill.replicas=1",
                "models[0].prefillDecode.dynamicRebalance.minPrefillReplicas=2",
            ],
            "initial replicas must satisfy the configured role minimums",
        ),
        (
            [
                "models[0].prefillDecode.prefill.replicas=2",
                "models[0].prefillDecode.decode.replicas=1",
                "models[0].prefillDecode.dynamicRebalance.maxTotalReplicas=2",
            ],
            "cannot be below the initial P/D replicas",
        ),
    ],
)
def test_rebalancer_rejects_unreachable_replica_budgets(
    settings: list[str], error: str
) -> None:
    if shutil.which("helm") is None:
        pytest.skip("helm is not installed")
    arguments = [
        "helm",
        "template",
        "test",
        str(CHART),
        "--set",
        BASE_MODEL,
        "--set",
        "models[0].prefillDecode.dynamicRebalance.enabled=true",
    ]
    for setting in settings:
        arguments.extend(["--set", setting])
    result = subprocess.run(arguments, check=False, capture_output=True, text=True)
    assert result.returncode != 0
    assert error in result.stderr
