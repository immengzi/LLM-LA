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
    "models[0].prefillDecode.dynamicRebalance.maxTotalReplicas=3,"
    # Warm standby runs both engines of a card on the pod's NPUs, so the chart
    # requires an explicit role-disjoint port plan (validated by
    # `vllmkv.validateWarmStandbyPorts`; see the multi-process port plan in
    # docs/design/pd-warm-standby.md).
    "models[0].prefillDecode.prefill.hcclSocketPortRange=63000-63050,"
    "models[0].prefillDecode.prefill.hcclHostSocketPortRange=62000-62050,"
    "models[0].prefillDecode.prefill.hixlListenPort=16700,"
    "models[0].prefillDecode.decode.hcclSocketPortRange=65000-65050,"
    "models[0].prefillDecode.decode.hcclHostSocketPortRange=64000-64050,"
    "models[0].prefillDecode.decode.hixlListenPort=16800"
)

CAMEM_PATH = "/vllm-workspace/vllm-ascend/vllm_ascend/device_allocator/camem.py"


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


def test_warmstandby_requires_an_explicit_port_plan() -> None:
    """Without a port plan the render fails and says what is missing."""
    _require_helm()
    result = subprocess.run(
        [
            "helm", "template", "test", str(CHART), "--set",
            "models[0].name=qwen,"
            "models[0].modelSubPath=placeholder,"
            "models[0].tensorParallelSize=1,"
            "models[0].prefillDecode.enabled=true,"
            "models[0].prefillDecode.dynamicRebalance.enabled=true,"
            "models[0].prefillDecode.dynamicRebalance.mode=warmstandby,"
            "models[0].prefillDecode.replicas=3,"
            "models[0].prefillDecode.dynamicRebalance.maxTotalReplicas=3",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "hixlListenPort must be set for BOTH roles" in result.stderr


def test_warmstandby_rejects_a_shared_hixl_listen_port() -> None:
    _, stderr = render(
        "--set", "models[0].prefillDecode.decode.hixlListenPort=16700",
        expect_ok=False,
    )
    assert "must differ" in stderr


def test_warmstandby_rejects_overlapping_socket_ranges() -> None:
    _, stderr = render(
        "--set", "models[0].prefillDecode.decode.hcclSocketPortRange=63000-63050",
        expect_ok=False,
    )
    assert "overlaps" in stderr


def test_warmstandby_rejects_the_libhcomm_default_ports() -> None:
    """16666/16667 are the ports libhcomm binds for itself (HETEROG_CCL_PORT
    and AICPU_RETRY_BACKUP_PORT), so neither role may plan them."""
    for port, name in ((16666, "HETEROG_CCL_PORT"), (16667, "AICPU_RETRY_BACKUP_PORT")):
        _, stderr = render(
            "--set", f"models[0].prefillDecode.decode.hixlListenPort={port}",
            expect_ok=False,
        )
        assert name in stderr, (port, stderr)


def test_warmstandby_rejects_ranges_containing_a_libhcomm_default() -> None:
    """Any range containing 16666 or 16667 is rejected - including a range that
    starts exactly at 16667, the boundary an endpoint comparison misses."""
    for raw in ("16666-16690", "16600-16666", "16660-16670", "16667-16690", "16667-16667"):
        _, stderr = render(
            "--set", f"models[0].prefillDecode.prefill.hcclHostSocketPortRange={raw}",
            expect_ok=False,
        )
        assert "libhcomm binds for itself" in stderr, raw


def test_warmstandby_accepts_a_range_below_the_libhcomm_defaults() -> None:
    render("--set", "models[0].prefillDecode.prefill.hcclHostSocketPortRange=16600-16665")


def test_warmstandby_accepts_unequal_tensor_parallel_size() -> None:
    """P/D may run different tensor-parallel sizes.

    The engine side already supports a TP mismatch through the store connector
    (AscendStore sub-key split), and a warmstandby pod shares one card pair
    between its two engines, so the accelerator footprint must follow
    max(prefillTp, decodeTp) - sizing it by the prefill role alone would
    under-request for an asymmetric pair such as P=1/D=2.
    """
    for prefill_tp, decode_tp in ((2, 1), (1, 2)):
        manifest, _ = render(
            "--set", f"models[0].prefillDecode.prefill.tensorParallelSize={prefill_tp}",
            "--set", f"models[0].prefillDecode.decode.tensorParallelSize={decode_tp}",
        )
        requested: list[int] = []
        for dep in (d for d in docs(manifest) if d.get("kind") == "Deployment"):
            for container in dep["spec"]["template"]["spec"]["containers"]:
                requests = (container.get("resources") or {}).get("requests") or {}
                if "accelerator.example.com/device" in requests:
                    requested.append(int(requests["accelerator.example.com/device"]))
        assert requested, (prefill_tp, decode_tp)
        assert set(requested) == {max(prefill_tp, decode_tp)}, (
            prefill_tp,
            decode_tp,
            requested,
        )


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


def test_sleep_overlay_files_are_injected_with_set_file(tmp_path: Path) -> None:
    """The overlay body is injected at deploy time, not read from the chart.

    The patch package ships `overlay.json` + `make-overlay-command.py`; each
    patched upstream file becomes `vllm.upstreamOverlay.files.<name>` =
    {key, path, content}, with the body coming from
    `--set-file ...content=<file>`. Nothing about the file names is hardcoded
    in the chart, so the file set can move with the upstream version.
    """
    _require_helm()
    body = tmp_path / "camem.py"
    body.write_text("# patched camem\n")
    result = subprocess.run(
        [
            "helm", "template", "test", str(CHART), "--set", BASE_MODEL,
            "--set", "vllm.upstreamOverlay.enabled=true",
            "--set", "vllm.upstreamOverlay.files.camem.key=camem.py",
            "--set", f"vllm.upstreamOverlay.files.camem.path={CAMEM_PATH}",
            "--set-file", f"vllm.upstreamOverlay.files.camem.content={body}",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr

    configmap = find_docs(result.stdout, "ConfigMap", "test-te-unreg-sc")[0]
    assert configmap["data"]["camem.py"].strip() == "# patched camem"

    deployment = find_docs(result.stdout, "Deployment", "vllm-qwen-pd")[0]
    spec = deployment["spec"]["template"]["spec"]
    for container in ("vllm-prefill", "vllm-decode"):
        overlay = [
            m
            for m in engine_container(spec, container)["volumeMounts"]
            if m.get("subPath") == "camem.py"
        ]
        assert overlay, container
        assert overlay[0]["mountPath"] == CAMEM_PATH


def test_upstream_overlay_enabled_without_files_fails_fast() -> None:
    _, stderr = render("--set", "vllm.upstreamOverlay.enabled=true", expect_ok=False)
    assert "vllm.upstreamOverlay.enabled requires" in stderr
