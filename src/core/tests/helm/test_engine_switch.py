# tests/helm/test_engine_switch.py
# -*- coding: utf-8 -*-
"""Unit tests for the vllm-kv-stack ``engine.type`` switch (vllm ↔ sglang).

Composes with the ``hardware`` switch (ascend ↔ nvidia). Mock both knobs via
``helm template --set`` and assert the rendered manifests compile with the
correct engine launch command, workload name, accelerator resource, and
runtime class for every supported combination.
"""
from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, List

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[4]
CHART = REPO_ROOT / "src" / "core" / "vllm-kv-stack"

ASCEND_DRIVER_MARKERS = (
    "dcmi-volume",
    "hisi-hdc-volume",
    "devmm-svm-volume",
    "npu-smi-volume",
)


def _require_helm() -> None:
    if shutil.which("helm") is None:
        pytest.skip("helm is not installed")


def _render(*extra: str) -> str:
    _require_helm()
    result = subprocess.run(
        [
            "helm",
            "template",
            "test",
            str(CHART),
            "--set",
            "global.imageRegistry=",
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


def _engine_deployments(docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for d in docs:
        if d.get("kind") != "Deployment":
            continue
        name = str(d.get("metadata", {}).get("name", ""))
        if name.startswith("vllm-") or name.startswith("sglang-"):
            out.append(d)
    assert out, "no engine Deployments found"
    return out


# ---------------------------------------------------------------------------
# Compilation matrix
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("hardware", ["ascend", "nvidia"])
@pytest.mark.parametrize("engine", ["vllm", "sglang"])
def test_helm_template_compiles_for_engine_and_hardware(hardware: str, engine: str):
    _render("--set", f"hardware={hardware}", "--set", f"engine.type={engine}")


# ---------------------------------------------------------------------------
# Default remains vLLM + Ascend
# ---------------------------------------------------------------------------

def test_default_render_remains_vllm_on_ascend():
    manifest = _render()
    assert "vllm.entrypoints.openai.api_server" in manifest
    assert "sglang.launch_server" not in manifest
    assert "name: vllm-qwen" in manifest
    assert "name: sglang-qwen" not in manifest
    assert "huawei.com/Ascend910" in manifest
    assert "nvidia.com/gpu" not in manifest
    assert "runtimeClassName: nvidia" not in manifest
    assert "component: vllm" in manifest
    # ServiceMonitor keeps the historical name for the default engine.
    assert any(
        d.get("kind") == "ServiceMonitor"
        and d.get("metadata", {}).get("name") == "vllm"
        for d in _docs(manifest)
    )
    assert 'value: "vllm"' in manifest


# ---------------------------------------------------------------------------
# Opt-in composition: one switch alone must not flip the other
# ---------------------------------------------------------------------------

def test_hardware_nvidia_alone_keeps_vllm_engine():
    """hardware=nvidia must not select SGLang when engine.type is unset."""
    manifest = _render("--set", "hardware=nvidia")
    assert "vllm.entrypoints.openai.api_server" in manifest
    assert "sglang.launch_server" not in manifest
    assert "name: vllm-qwen" in manifest
    assert "name: sglang-qwen" not in manifest
    assert "component: vllm" in manifest
    assert "nvidia.com/gpu" in manifest
    assert "runtimeClassName: nvidia" in manifest


def test_engine_sglang_alone_keeps_ascend_hardware():
    """engine.type=sglang must keep Ascend NPUs when hardware is unset."""
    manifest = _render(
        "--set", "engine.type=sglang",
        "--set", "images.sglang=reg.local/sglang-ascend:latest",
    )
    assert "name: sglang-qwen" in manifest
    assert "exec python -m sglang.launch_server" in manifest
    assert "component: sglang" in manifest
    assert "huawei.com/Ascend910" in manifest
    assert "nvidia.com/gpu" not in manifest
    assert "runtimeClassName: nvidia" not in manifest
    assert "dcmi-volume" in manifest


def test_sglang_rejects_data_parallel_at_chart():
    """SGLang + dataParallel must fail closed at helm template time."""
    _require_helm()
    result = subprocess.run(
        [
            "helm", "template", "test", str(CHART),
            "--set", "global.imageRegistry=",
            "--set", "modelVolume.modelSubPath=placeholder",
            "--set", "engine.type=sglang",
            "--set", "dataParallel.enabled=true",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    err = (result.stderr or "") + (result.stdout or "")
    assert "does not support dataParallel" in err


# ---------------------------------------------------------------------------
# SGLang on NVIDIA (production GPU path)
# ---------------------------------------------------------------------------

def test_sglang_nvidia_uses_pinned_cuda_image_and_gpu_resource():
    manifest = _render(
        "--set", "hardware=nvidia",
        "--set", "engine.type=sglang",
        "--set", "sglang.toolCallParser=qwen",
    )
    assert "name: sglang-qwen" in manifest
    assert "name: vllm-qwen" not in manifest
    assert "image: lmsysorg/sglang:v0.5.15-cu129" in manifest
    assert "exec python -m sglang.launch_server" in manifest
    assert "--tool-call-parser qwen" in manifest
    assert "--page-size 16" in manifest
    assert "runtimeClassName: nvidia" in manifest
    assert "nvidia.com/gpu" in manifest
    assert "huawei.com/Ascend910" not in manifest
    for marker in ASCEND_DRIVER_MARKERS:
        assert marker not in manifest
    assert "ascend-toolkit/set_env.sh" not in manifest
    assert 'value: "sglang"' in manifest
    assert "KV_HASH_BACKEND" in manifest
    assert "SGLANG_CONTRACT_VERSION" in manifest
    assert "component: sglang" in manifest


# ---------------------------------------------------------------------------
# SGLang on Ascend (NPU path — same engine switch, hardware=ascend)
# ---------------------------------------------------------------------------

def test_sglang_ascend_requests_npu_and_keeps_driver_mounts():
    """SGLang on Ascend uses the shared hardware switch, not hardcoded GPUs."""
    manifest = _render(
        "--set", "hardware=ascend",
        "--set", "engine.type=sglang",
        "--set", "images.sglang=reg.local/sglang-ascend:latest",
    )
    assert "name: sglang-qwen" in manifest
    assert "exec python -m sglang.launch_server" in manifest
    assert "huawei.com/Ascend910" in manifest
    assert "nvidia.com/gpu" not in manifest
    assert "runtimeClassName: nvidia" not in manifest
    assert "dcmi-volume" in manifest
    assert "image: reg.local/sglang-ascend:latest" in manifest


# ---------------------------------------------------------------------------
# vLLM still honours hardware when engine stays default
# ---------------------------------------------------------------------------

def test_vllm_nvidia_still_uses_gpu_resource():
    manifest = _render("--set", "hardware=nvidia", "--set", "engine.type=vllm")
    assert "name: vllm-qwen" in manifest
    assert "vllm.entrypoints.openai.api_server" in manifest
    assert "sglang.launch_server" not in manifest
    assert "nvidia.com/gpu" in manifest
    assert "runtimeClassName: nvidia" in manifest
    assert "huawei.com/Ascend910" not in manifest


# ---------------------------------------------------------------------------
# Workload naming + discovery labels
# ---------------------------------------------------------------------------

def test_sglang_models_list_uses_engine_aware_names_and_selector():
    models = json.dumps(
        [{"name": "qwen", "engine": "sglang", "modelSubPath": "qwen3-8b"}]
    )
    manifest = _render(
        "--set", "hardware=nvidia",
        "--set", "engine.type=sglang",
        "--set-json", f"models={models}",
    )
    assert "name: sglang-qwen" in manifest
    assert "component=sglang" in manifest
    assert "component: sglang" in manifest
    assert "kind: ServiceMonitor" in manifest


def test_mixed_model_engines_are_rejected_by_chart_validation():
    """The router has one global hash backend; mixed engines must not compile."""
    _require_helm()
    models = json.dumps(
        [
            {"name": "a", "engine": "vllm", "modelSubPath": "a", "tensorParallelSize": 1},
            {"name": "b", "engine": "sglang", "modelSubPath": "b", "tensorParallelSize": 1},
        ]
    )
    result = subprocess.run(
        [
            "helm", "template", "test", str(CHART),
            "--set", "global.imageRegistry=",
            "--set", "modelVolume.modelSubPath=placeholder",
            "--set", "hardware=nvidia",
            "--set", "engine.type=vllm",
            "--set-json", f"models={models}",
        ],
        check=False, capture_output=True, text=True,
    )
    assert result.returncode != 0
    assert "mixed model engines" in result.stderr


def test_sglang_go_render_sets_engine_env_and_images():
    manifest = _render(
        "--set", "hardware=nvidia",
        "--set", "engine.type=sglang",
        "--set", "serviceImpl=go",
    )
    docs = _docs(manifest)
    deployments = {
        d["metadata"]["name"]: d
        for d in docs
        if d.get("kind") == "Deployment"
    }
    assert "sglang-qwen" in deployments
    engine_pod = deployments["sglang-qwen"]["spec"]["template"]["spec"]
    assert engine_pod.get("runtimeClassName") == "nvidia"
    containers = {c["name"]: c for c in engine_pod["containers"]}
    assert "sglang" in containers
    assert "kv-sidecar" in containers
    sidecar_env = {
        e["name"]: e.get("value") for e in containers["kv-sidecar"].get("env") or []
    }
    assert sidecar_env.get("INFERENCE_ENGINE") == "sglang"
    assert sidecar_env.get("KV_EVENT_EXPECTED_PAGE_SIZE") == "16"
    assert containers["sglang"]["resources"]["requests"]["nvidia.com/gpu"] == 8

    router = deployments["router-service"]["spec"]["template"]["spec"]
    router_ct = next(c for c in router["containers"] if c["name"] == "router")
    router_env = {e["name"]: e.get("value") for e in router_ct.get("env") or []}
    assert router_env.get("INFERENCE_ENGINE") == "sglang"
    assert router_env.get("KV_HASH_BACKEND") == "sglang"
    assert router_env.get("SGLANG_CONTRACT_VERSION") == "0.5.15"
    assert router_env.get("LABEL_SELECTOR") == "app=sglang-qwen"

def test_sglang_nvidia_runtime_class_override():
    manifest = _render(
        "--set", "hardware=nvidia",
        "--set", "engine.type=sglang",
        "--set", "sglang.runtimeClassName=nvidia-custom",
    )
    for dep in _engine_deployments(_docs(manifest)):
        assert dep["spec"]["template"]["spec"].get("runtimeClassName") == "nvidia-custom"
