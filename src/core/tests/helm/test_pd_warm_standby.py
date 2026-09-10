"""Helm rendering tests for the per-card dual-engine P/D warm standby."""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml


REPO_ROOT = Path(__file__).resolve().parents[4]
CHART = REPO_ROOT / "src" / "core" / "vllm-kv-stack"
BASE_MODEL = (
    "models[0].name=qwen,"
    "models[0].modelSubPath=placeholder,"
    "models[0].tensorParallelSize=1,"
    "models[0].prefillDecode.enabled=true,"
    "models[0].prefillDecode.dynamicRebalance.enabled=true,"
    "models[0].prefillDecode.dynamicRebalance.mode=warmstandby,"
    "models[0].prefillDecode.replicas=3,"
    "models[0].prefillDecode.dynamicRebalance.maxTotalReplicas=3"
)


def _require_helm() -> None:
    if shutil.which("helm") is None:
        pytest.skip("helm is not installed")


def render(*extra: str, expect_ok: bool = True) -> tuple[str, str]:
    _require_helm()
    result = subprocess.run(
        ["helm", "template", "test", str(CHART), "--set", BASE_MODEL, *extra],
        check=False,
        capture_output=True,
        text=True,
    )
    if expect_ok:
        assert result.returncode == 0, result.stderr
    return result.stdout, result.stderr


def docs(manifest: str) -> list[dict]:
    return [d for d in yaml.safe_load_all(manifest) if d]


def find_docs(manifest: str, kind: str, name: str) -> list[dict]:
    return [
        d
        for d in docs(manifest)
        if d.get("kind") == kind and (d.get("metadata") or {}).get("name") == name
    ]


def engine_container(spec: dict, name: str) -> dict:
    return next(c for c in spec["containers"] if c["name"] == name)


def test_warmstandby_renders_single_dual_engine_deployment() -> None:
    manifest, _ = render()
    deployments = [d for d in docs(manifest) if d.get("kind") == "Deployment"]
    names = [(d.get("metadata") or {}).get("name") for d in deployments]
    assert "vllm-qwen-pd" in names
    assert "vllm-qwen-prefill" not in names
    assert "vllm-qwen-decode" not in names

    deployment = find_docs(manifest, "Deployment", "vllm-qwen-pd")[0]
    assert deployment["spec"]["replicas"] == 3
    assert deployment["spec"]["strategy"]["type"] == "Recreate"
    assert deployment["spec"]["template"]["spec"]["shareProcessNamespace"] is True
    assert deployment["spec"]["template"]["spec"]["hostIPC"] is True
    labels = deployment["spec"]["template"]["metadata"]["labels"]
    assert labels["pd-prefill-awake"] == "false"
    assert labels["pd-decode-awake"] == "true"

    spec = deployment["spec"]["template"]["spec"]
    prefill = engine_container(spec, "vllm-prefill")
    decode = engine_container(spec, "vllm-decode")
    assert prefill["ports"][0]["containerPort"] == 8200
    assert decode["ports"][0]["containerPort"] == 8201

    args = "\n".join(prefill["args"]) + "\n" + "\n".join(decode["args"])
    assert args.count("--enable-sleep-mode") == 2
    assert "--compilation-config" not in args
    assert "pre-warm pool" not in args


def test_warmstandby_pool_boot_sleep_renders_both_engines_asleep() -> None:
    manifest, _ = render("--set", "models[0].prefillDecode.poolBootSleep=true")
    deployment = find_docs(manifest, "Deployment", "vllm-qwen-pd")[0]
    labels = deployment["spec"]["template"]["metadata"]["labels"]
    assert labels["pd-prefill-awake"] == "false"
    assert labels["pd-decode-awake"] == "false"

    spec = deployment["spec"]["template"]["spec"]
    decode = engine_container(spec, "vllm-decode")
    args = "\n".join(decode["args"])
    assert "pre-warm pool" in args


def test_warmstandby_rebalancer_sleep_env_wired() -> None:
    manifest, _ = render()
    deployment = find_docs(manifest, "Deployment", "test-pd-rebalancer")[0]
    container = deployment["spec"]["template"]["spec"]["containers"][0]
    env = {e["name"]: e.get("value", "") for e in container["env"]}
    assert env["PD_REBALANCER_SLEEP_RETRIES"] == "5"
    assert env["PD_REBALANCER_SLEEP_BACKOFF_SECONDS"] == "2"


def test_warmstandby_rebalancer_sleep_env_overridable() -> None:
    manifest, _ = render(
        "--set", "pdRebalancer.sleepRetries=9",
        "--set", "pdRebalancer.sleepBackoffSeconds=3",
    )
    deployment = find_docs(manifest, "Deployment", "test-pd-rebalancer")[0]
    container = deployment["spec"]["template"]["spec"]["containers"][0]
    env = {e["name"]: e.get("value", "") for e in container["env"]}
    assert env["PD_REBALANCER_SLEEP_RETRIES"] == "9"
    assert env["PD_REBALANCER_SLEEP_BACKOFF_SECONDS"] == "3"


def test_warmstandby_sleep_env_and_no_forbidden_knobs() -> None:
    manifest, _ = render()
    deployment = find_docs(manifest, "Deployment", "vllm-qwen-pd")[0]
    spec = deployment["spec"]["template"]["spec"]
    env_names: list[str] = []
    all_env: dict[str, str] = {}
    for container in spec["containers"]:
        for entry in container["env"]:
            env_names.append(entry["name"])
            all_env[entry["name"]] = entry.get("value", "")
    assert "PYTORCH_NPU_ALLOC_CONF" not in env_names
    assert all_env["VLLM_SERVER_DEV_MODE"] == "1"
    assert all_env["VLLM_WORKER_MULTIPROC_METHOD"] == "spawn"
    assert all_env["VLLM_ASCEND_ENABLE_NZ"] == "0"


def test_warmstandby_npu_resource_only_on_prefill_container() -> None:
    manifest, _ = render()
    deployment = find_docs(manifest, "Deployment", "vllm-qwen-pd")[0]
    spec = deployment["spec"]["template"]["spec"]
    prefill = engine_container(spec, "vllm-prefill")
    decode = engine_container(spec, "vllm-decode")
    assert prefill["resources"]["requests"].get("accelerator.example.com/device") == 1
    assert prefill["resources"]["limits"].get("accelerator.example.com/device") == 1
    assert "accelerator.example.com/device" not in decode["resources"]["requests"]
    assert "accelerator.example.com/device" not in decode["resources"]["limits"]
    mounts = [m["name"] for m in decode["volumeMounts"]]
    assert "pod-annotations" in mounts


def test_warmstandby_services_select_by_role_and_awake() -> None:
    manifest, _ = render()
    prefill_svc = find_docs(manifest, "Service", "vllm-qwen-prefill")[0]
    decode_svc = find_docs(manifest, "Service", "vllm-qwen-decode")[0]
    assert prefill_svc["spec"]["selector"] == {
        "app": "vllm-qwen-pd",
        "pd-prefill-awake": "true",
    }
    assert decode_svc["spec"]["selector"] == {
        "app": "vllm-qwen-pd",
        "pd-decode-awake": "true",
    }
    assert prefill_svc["spec"]["ports"][0] == {"name": "http", "port": 8200, "targetPort": 8200}
    assert decode_svc["spec"]["ports"][0] == {"name": "http", "port": 8200, "targetPort": 8201}


def test_warmstandby_proxy_targets_unchanged_services() -> None:
    manifest, _ = render()
    proxy = find_docs(manifest, "Deployment", "vllm-qwen-pd-proxy")[0]
    container = proxy["spec"]["template"]["spec"]["containers"][0]
    env = {e["name"]: e["value"] for e in container["env"]}
    assert env["PREFILL_BASE"] == "http://vllm-qwen-prefill:8200"
    assert env["DECODE_BASE"] == "http://vllm-qwen-decode:8200"


def test_warmstandby_rebalancer_rbac_and_models_json() -> None:
    manifest, _ = render()
    role = find_docs(manifest, "Role", "test-pd-rebalancer")[0]
    rules = {rule["resources"][0]: rule for rule in role["rules"]}
    assert rules["deployments"]["resourceNames"] == ["vllm-qwen-pd"]
    assert "patch" in rules["pods"]["verbs"]

    deployment = find_docs(manifest, "Deployment", "test-pd-rebalancer")[0]
    container = deployment["spec"]["template"]["spec"]["containers"][0]
    env = {e["name"]: e.get("value", "") for e in container["env"]}
    models = json.loads(env["PD_REBALANCER_MODELS_JSON"])
    assert models == [
        {
            "name": "qwen",
            "mode": "warmstandby",
            "deployment": "vllm-qwen-pd",
            "prefillDeployment": "vllm-qwen-prefill",
            "decodeDeployment": "vllm-qwen-decode",
            "proxyService": "vllm-qwen",
            "minPrefillReplicas": 1,
            "minDecodeReplicas": 1,
            "maxTotalReplicas": 3,
            "replicas": 3,
            "prefillTp": 1,
            "decodeTp": 1,
            "prefillPort": 8200,
            "decodePort": 8201,
            "sleepLevel": 1,
        }
    ]


def test_warmstandby_allows_equal_tensor_parallel_size() -> None:
    render(
        "--set", "models[0].prefillDecode.prefill.tensorParallelSize=2",
        "--set", "models[0].prefillDecode.decode.tensorParallelSize=2",
    )


def test_warmstandby_rejects_unequal_tensor_parallel_size() -> None:
    _, stderr = render(
        "--set", "models[0].prefillDecode.prefill.tensorParallelSize=2",
        "--set", "models[0].prefillDecode.decode.tensorParallelSize=1",
        expect_ok=False,
    )
    assert "requires equal prefill/decode tensorParallelSize" in stderr


def test_warmstandby_rejects_replicas_below_max_total() -> None:
    _, stderr = render(
        "--set", "models[0].prefillDecode.replicas=2",
        expect_ok=False,
    )
    assert "warmstandby replicas" in stderr


def test_scale_mode_still_renders_two_pools() -> None:
    _require_helm()
    result = subprocess.run(
        [
            "helm", "template", "test", str(CHART), "--set",
            "models[0].name=qwen,"
            "models[0].modelSubPath=placeholder,"
            "models[0].tensorParallelSize=1,"
            "models[0].prefillDecode.enabled=true,"
            "models[0].prefillDecode.dynamicRebalance.enabled=true",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    names = [
        (d.get("metadata") or {}).get("name")
        for d in docs(result.stdout)
        if d.get("kind") == "Deployment"
    ]
    assert "vllm-qwen-prefill" in names
    assert "vllm-qwen-decode" in names
    assert "vllm-qwen-pd" not in names
    assert "--enable-sleep-mode" not in result.stdout


def test_sleep_overlay_render_depends_on_overlay_files() -> None:
    """The sleep overlay renders when its files exist and fails fast otherwise.

    The overlay files (`camem.py`, `mooncake_transfer_engine.py`) ship as a
    separate package, so this test accepts either checkout state.
    """
    files_present = all(
        (CHART / "files" / name).exists()
        for name in ("camem.py", "mooncake_transfer_engine.py")
    )
    manifest, stderr = render(
        "--set", "vllm.sleepOverlay.enabled=true", expect_ok=files_present
    )
    if files_present:
        assert "te-unreg-sc" in manifest
    else:
        assert "vllm.sleepOverlay.enabled requires" in stderr
