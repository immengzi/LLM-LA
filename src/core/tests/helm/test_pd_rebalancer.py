"""Helm rendering tests for the quiescent P/D replica rebalancer."""
from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[4]
CHART = REPO_ROOT / "src" / "core" / "vllm-kv-stack"
BASE_MODEL = (
    "models[0].name=qwen,models[0].modelSubPath=placeholder,"
    "models[0].tensorParallelSize=1,"
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
    for name in ("test-pd-rebalancer", "test-pd-rebalancer-code"):
        assert f"name: {name}" in manifest
    assert 'resources: ["deployments", "deployments/scale"]' in manifest
    assert 'resources: ["configmaps"]' in manifest
    assert 'verbs: ["get", "patch"]' in manifest
    assert 'verbs: ["create"]' in manifest
    assert 'resources: ["pods"]' in manifest
    assert "- name: PD_REBALANCER_DRY_RUN" in manifest
    assert 'value: "false"' in manifest
    assert "- name: PD_REBALANCER_HEARTBEAT_TIMEOUT_SECONDS" in manifest
    assert "- name: PD_REBALANCER_DRAIN_TIMEOUT_SECONDS" in manifest
    assert "- name: PD_REBALANCER_CAPACITY_PREFLIGHT" in manifest
    assert "- name: PD_REBALANCER_CAPACITY_RESOURCE" in manifest
    assert "- name: PD_REBALANCER_PLANNER_SCRAPE_DEADLINE_SECONDS" in manifest
    assert '\\"prefillTp\\":1' in manifest
    assert '\\"decodeTp\\":1' in manifest


def test_rebalancer_renders_capacity_cluster_role_when_enabled() -> None:
    manifest = render("--set", "models[0].prefillDecode.dynamicRebalance.enabled=true")
    assert "kind: ClusterRole" in manifest
    assert 'resources: ["nodes"]' in manifest
    assert "test-pd-rebalancer-capacity" in manifest


def test_rebalancer_skips_capacity_cluster_role_when_disabled() -> None:
    manifest = render(
        "--set",
        "models[0].prefillDecode.dynamicRebalance.enabled=true",
        "--set",
        "pdRebalancer.capacityPreflight.enabled=false",
    )
    assert "kind: ClusterRole" not in manifest
    assert "- name: PD_REBALANCER_CAPACITY_PREFLIGHT" in manifest
    assert 'value: "false"' in manifest


def test_rebalancer_dry_run_renders_true_when_enabled() -> None:
    manifest = render(
        "--set",
        "models[0].prefillDecode.dynamicRebalance.enabled=true",
        "--set",
        "pdRebalancer.dryRun=true",
    )
    assert 'value: "true"' in manifest


def test_rebalancer_state_configmap_is_controller_owned() -> None:
    # The rebalancer owns its state ConfigMap: Helm must not render or manage
    # it, so `helm template`, `--force`, and three-way merges cannot stomp the
    # committed topology. RBAC grants name-scoped get/patch plus create for the
    # controller's GET-404 -> POST bootstrap.
    manifest = render("--set", "models[0].prefillDecode.dynamicRebalance.enabled=true")
    assert "name: test-pd-rebalancer-state" not in manifest
    assert 'lookup "v1" "ConfigMap"' not in manifest
    assert 'resourceNames: ["test-pd-rebalancer-state"]' in manifest
    assert 'verbs: ["create"]' in manifest


def test_rebalancer_managed_deployments_fall_back_to_chart_replicas() -> None:
    # lookup() is unavailable under helm template, so the preserved-replica
    # path must fall back to chart values and keep rendering valid manifests.
    manifest = render(
        "--set",
        "models[0].prefillDecode.dynamicRebalance.enabled=true",
        "--set",
        "models[0].prefillDecode.prefill.replicas=2",
        "--set",
        "models[0].prefillDecode.decode.replicas=1",
    )
    prefill_block = re.search(
        r"name: vllm-qwen-prefill\n(?:.*\n){0,15}?[ \t]*replicas: (\d+)", manifest
    )
    decode_block = re.search(
        r"name: vllm-qwen-decode\n(?:.*\n){0,15}?[ \t]*replicas: (\d+)", manifest
    )
    assert prefill_block is not None and prefill_block.group(1) == "2"
    assert decode_block is not None and decode_block.group(1) == "1"


def test_rebalancer_accepts_mismatched_tensor_parallelism() -> None:
    """P/D may run different tensor-parallel sizes.

    The rebalancer models JSON keeps the per-role TP values and the engine
    side handles the KV layout difference through the store connector's
    sub-key split, so an asymmetric pair must render rather than fail.
    """
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
    assert result.returncode == 0, result.stderr
    # PD_REBALANCER_MODELS_JSON is rendered as a quoted (escaped) JSON string.
    assert '\\"prefillTp\\":1' in result.stdout
    assert '\\"decodeTp\\":2' in result.stdout


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
