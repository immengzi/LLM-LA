# tests/helm/test_hardware_switch.py
# -*- coding: utf-8 -*-
"""Unit tests for the vllm-kv-stack ``hardware`` switch (ascend ↔ nvidia).

The chart selects the accelerator backend with a single value
(``.Values.hardware``). These tests mock-switch that knob via ``helm template
--set`` and assert the rendered manifests compile and contain the correct
resources / runtime class / Ascend toolkit setup for each backend.

Covers the standard Deployment path, the Data-Parallel LeaderWorkerSet path,
and the prefill/decode (P/D) disaggregation path, because all three must honour
the same switch (a regression found and fixed while writing these tests left
Ascend mounts on DP pods under ``hardware=nvidia``).
"""
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[4]
CHART = REPO_ROOT / "src" / "core" / "vllm-kv-stack"

# Public-chart placeholder; real values are supplied by the deployment overlay.
ACCELERATOR_RESOURCE = "accelerator.example.com/device"

ASCEND_MARKERS = (
    ACCELERATOR_RESOURCE,
    "ASCEND_RT_VISIBLE_DEVICES",
    "ascend-toolkit/set_env.sh",
    "dcmi-volume",
    "hisi-hdc-volume",
    "devmm-svm-volume",
    "npu-smi-volume",
)
NVIDIA_MARKERS = (
    "nvidia.com/gpu",
    "runtimeClassName: nvidia",
)


def _require_helm() -> None:
    if shutil.which("helm") is None:
        pytest.skip("helm is not installed")


def _render(*extra: str) -> str:
    """Render the chart; fail the test if helm template does not compile."""
    _require_helm()
    result = subprocess.run(
        [
            "helm",
            "template",
            "test",
            str(CHART),
            "--set",
            "modelVolume.modelSubPath=placeholder",
            *extra,
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"helm template failed (exit {result.returncode}):\n{result.stderr}"
    )
    assert result.stdout.strip(), "helm template produced empty output"
    return result.stdout


def _docs(manifest: str) -> List[Dict[str, Any]]:
    return [d for d in yaml.safe_load_all(manifest) if d]


def _engine_pod_specs(docs: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Collect pod specs for every vLLM engine container across Deployment + LWS."""
    specs: List[Dict[str, Any]] = []
    for doc in docs:
        kind = doc.get("kind")
        if kind == "Deployment" and str(doc.get("metadata", {}).get("name", "")).startswith(
            "vllm-"
        ):
            specs.append(doc["spec"]["template"]["spec"])
        elif kind == "LeaderWorkerSet":
            lwt = doc["spec"]["leaderWorkerTemplate"]
            specs.append(lwt["leaderTemplate"]["spec"])
            specs.append(lwt["workerTemplate"]["spec"])
    assert specs, "no vLLM engine pod specs found in rendered manifests"
    return specs


def _vllm_containers(pod_spec: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [c for c in pod_spec.get("containers", []) if c.get("name") == "vllm"]


def _env_names(container: Dict[str, Any]) -> List[str]:
    return [e.get("name") for e in container.get("env") or [] if e.get("name")]


def _mount_names(container: Dict[str, Any]) -> List[str]:
    return [m.get("name") for m in container.get("volumeMounts") or [] if m.get("name")]


def _volume_names(pod_spec: Dict[str, Any]) -> List[str]:
    return [v.get("name") for v in pod_spec.get("volumes") or [] if v.get("name")]


def _accelerator_count(container: Dict[str, Any], resource: str) -> Optional[int]:
    req = ((container.get("resources") or {}).get("requests") or {}).get(resource)
    lim = ((container.get("resources") or {}).get("limits") or {}).get(resource)
    if req is None and lim is None:
        return None
    assert req == lim, f"requests/limits mismatch for {resource}: {req!r} vs {lim!r}"
    return int(req)


# ---------------------------------------------------------------------------
# Compilation / lint-level smoke: both backends must render cleanly
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "hardware",
    ["ascend", "nvidia", "NVIDIA", "Ascend", ""],
)
def test_helm_template_compiles_for_hardware(hardware: str):
    """Mock-switch ``hardware`` (incl. case variants / empty=default) and compile."""
    extra: List[str] = []
    if hardware:
        extra.extend(["--set", f"hardware={hardware}"])
    _render(*extra)


def test_helm_lint_passes_for_both_backends():
    _require_helm()
    for hardware in ("ascend", "nvidia"):
        result = subprocess.run(
            ["helm", "lint", str(CHART), "--set", f"hardware={hardware}"],
            check=False,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, (
            f"helm lint failed for hardware={hardware}:\n{result.stdout}\n{result.stderr}"
        )


# ---------------------------------------------------------------------------
# Standard Deployment path
# ---------------------------------------------------------------------------

def test_default_hardware_is_ascend():
    """Chart default (no --set) must preserve historical Ascend behaviour."""
    manifest = _render()
    for marker in ASCEND_MARKERS:
        assert marker in manifest, f"missing Ascend marker on default render: {marker}"
    for marker in NVIDIA_MARKERS:
        assert marker not in manifest, f"unexpected NVIDIA marker on default: {marker}"

    for pod in _engine_pod_specs(_docs(manifest)):
        assert pod.get("runtimeClassName") in (None, "")
        for c in _vllm_containers(pod):
            assert _accelerator_count(c, ACCELERATOR_RESOURCE) == 8
            assert _accelerator_count(c, "nvidia.com/gpu") is None
            assert "ASCEND_RT_VISIBLE_DEVICES" in _env_names(c)
            assert "VLLM_USE_V1" in _env_names(c)
            mounts = _mount_names(c)
            assert "dcmi-volume" in mounts
            assert "hisi-hdc-volume" in mounts


def test_nvidia_switch_drops_ascend_and_requests_gpu():
    manifest = _render("--set", "hardware=nvidia")
    for marker in NVIDIA_MARKERS:
        assert marker in manifest, f"missing NVIDIA marker: {marker}"
    for marker in ASCEND_MARKERS:
        assert marker not in manifest, f"Ascend marker leaked under nvidia: {marker}"

    for pod in _engine_pod_specs(_docs(manifest)):
        assert pod.get("runtimeClassName") == "nvidia"
        for c in _vllm_containers(pod):
            assert _accelerator_count(c, "nvidia.com/gpu") == 8
            assert _accelerator_count(c, ACCELERATOR_RESOURCE) is None
            env = _env_names(c)
            assert "ASCEND_RT_VISIBLE_DEVICES" not in env
            assert "HCCL_OP_EXPANSION_MODE" not in env
            assert "VLLM_USE_V1" in env
            mounts = set(_mount_names(c))
            vols = set(_volume_names(pod))
            for forbidden in (
                "dcmi-volume",
                "npu-smi-volume",
                "hisi-hdc-volume",
                "devmm-svm-volume",
                "ascend-driver-lib64-volume",
                "hccn-conf",
            ):
                assert forbidden not in mounts
                assert forbidden not in vols
            assert "ascend-toolkit/set_env.sh" not in (c.get("args") or [""])[0]


def test_nvidia_runtime_class_override():
    manifest = _render(
        "--set", "hardware=nvidia",
        "--set", "vllm.runtimeClassName=nvidia-custom",
    )
    for pod in _engine_pod_specs(_docs(manifest)):
        assert pod.get("runtimeClassName") == "nvidia-custom"


def test_tensor_parallel_size_drives_accelerator_count():
    for hardware, resource in (
        ("ascend", ACCELERATOR_RESOURCE),
        ("nvidia", "nvidia.com/gpu"),
    ):
        manifest = _render(
            "--set", f"hardware={hardware}",
            "--set", "tensorParallelSize=2",
        )
        for pod in _engine_pod_specs(_docs(manifest)):
            for c in _vllm_containers(pod):
                assert _accelerator_count(c, resource) == 2


def test_explicit_ascend_matches_default():
    """``hardware=ascend`` must be byte-equivalent to the chart default."""
    default = _render()
    explicit = _render("--set", "hardware=ascend")
    # Drop helm Source comments (release-independent) and compare docs.
    def _norm(text: str) -> List[Dict[str, Any]]:
        return _docs(text)

    assert _norm(default) == _norm(explicit)


# ---------------------------------------------------------------------------
# Data-Parallel (LeaderWorkerSet) path — must honour the same switch
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("hardware,resource,want_runtime", [
    ("ascend", ACCELERATOR_RESOURCE, None),
    ("nvidia", "nvidia.com/gpu", "nvidia"),
])
def test_data_parallel_hardware_switch(hardware, resource, want_runtime):
    manifest = _render(
        "--set", f"hardware={hardware}",
        "--set", "dataParallel.enabled=true",
        "--set", "dataParallel.size=2",
        "--set", "tensorParallelSize=4",
    )
    docs = _docs(manifest)
    assert any(d.get("kind") == "LeaderWorkerSet" for d in docs)

    if hardware == "nvidia":
        for marker in ASCEND_MARKERS:
            assert marker not in manifest, f"DP Ascend leak under nvidia: {marker}"
        assert "hccn-conf" not in manifest
    else:
        assert "ascend-toolkit/set_env.sh" in manifest
        assert "dcmi-volume" in manifest
        assert "hccn-conf" in manifest
        assert "nvidia.com/gpu" not in manifest

    for pod in _engine_pod_specs(docs):
        assert pod.get("runtimeClassName") == want_runtime
        for c in _vllm_containers(pod):
            assert _accelerator_count(c, resource) == 4
            other = "nvidia.com/gpu" if resource == ACCELERATOR_RESOURCE else ACCELERATOR_RESOURCE
            assert _accelerator_count(c, other) is None


# ---------------------------------------------------------------------------
# Multi-model list still flips with the global hardware switch
# ---------------------------------------------------------------------------

def test_multi_model_list_honours_hardware_switch():
    models_json = (
        '[{"name":"a","modelSubPath":"a","replicas":1,"tensorParallelSize":1},'
        '{"name":"b","modelSubPath":"b","replicas":1,"tensorParallelSize":2}]'
    )
    manifest = _render(
        "--set", "hardware=nvidia",
        "--set-json", f"models={models_json}",
    )
    docs = _docs(manifest)
    names = {
        d["metadata"]["name"]
        for d in docs
        if d.get("kind") == "Deployment" and str(d["metadata"]["name"]).startswith("vllm-")
    }
    assert names == {"vllm-a", "vllm-b"}

    by_name = {
        d["metadata"]["name"]: d
        for d in docs
        if d.get("kind") == "Deployment" and d["metadata"]["name"] in names
    }
    assert _accelerator_count(
        _vllm_containers(by_name["vllm-a"]["spec"]["template"]["spec"])[0],
        "nvidia.com/gpu",
    ) == 1
    assert _accelerator_count(
        _vllm_containers(by_name["vllm-b"]["spec"]["template"]["spec"])[0],
        "nvidia.com/gpu",
    ) == 2
    for marker in ASCEND_MARKERS:
        assert marker not in manifest


# ---------------------------------------------------------------------------
# Prefill/decode (P/D) disaggregation path — must honour the same switch
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("hardware,resource,want_runtime", [
    ("ascend", ACCELERATOR_RESOURCE, None),
    ("nvidia", "nvidia.com/gpu", "nvidia"),
])
def test_pd_disaggregation_hardware_switch(hardware, resource, want_runtime):
    """P/D prefill+decode pools must flip with ``hardware`` like template 40."""
    models_json = (
        '[{"name":"pdm","modelSubPath":"pdm","replicas":1,"tensorParallelSize":2,'
        '"prefillDecode":{"enabled":true,'
        '"prefill":{"replicas":1,"tensorParallelSize":2},'
        '"decode":{"replicas":1,"tensorParallelSize":2}}}]'
    )
    manifest = _render(
        "--set", f"hardware={hardware}",
        "--set-json", f"models={models_json}",
    )
    docs = _docs(manifest)
    pd_deploys = [
        d for d in docs
        if d.get("kind") == "Deployment"
        and str(d.get("metadata", {}).get("name", "")) in {
            "vllm-pdm-prefill", "vllm-pdm-decode",
        }
    ]
    names = {d["metadata"]["name"] for d in pd_deploys}
    assert names == {"vllm-pdm-prefill", "vllm-pdm-decode"}

    if hardware == "nvidia":
        for marker in ASCEND_MARKERS:
            assert marker not in manifest, f"P/D Ascend leak under nvidia: {marker}"
        assert ACCELERATOR_RESOURCE not in manifest
    else:
        assert "ascend-toolkit/set_env.sh" in manifest
        assert "dcmi-volume" in manifest
        assert "hisi-hdc-volume" in manifest
        assert "nvidia.com/gpu" not in manifest

    for d in pd_deploys:
        pod = d["spec"]["template"]["spec"]
        assert pod.get("runtimeClassName") == want_runtime
        for c in _vllm_containers(pod):
            assert _accelerator_count(c, resource) == 2
            other = "nvidia.com/gpu" if resource == ACCELERATOR_RESOURCE else ACCELERATOR_RESOURCE
            assert _accelerator_count(c, other) is None
            if hardware == "nvidia":
                assert "hisi-hdc-volume" not in _mount_names(c)
                assert "devmm-svm-volume" not in _mount_names(c)
            else:
                assert "hisi-hdc-volume" in _mount_names(c)
                assert "devmm-svm-volume" in _mount_names(c)
