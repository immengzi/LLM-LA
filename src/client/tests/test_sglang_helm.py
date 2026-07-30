import json
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml


REPO_ROOT = Path(__file__).resolve().parents[3]
CHART = REPO_ROOT / "src" / "core" / "vllm-kv-stack"


def _render(*extra: str) -> subprocess.CompletedProcess[str]:
    if shutil.which("helm") is None:
        pytest.skip("helm is not installed")
    return subprocess.run(
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


def test_default_render_remains_vllm():
    result = _render()
    assert result.returncode == 0, result.stderr
    assert "vllm.entrypoints.openai.api_server" in result.stdout
    assert "sglang.launch_server" not in result.stdout
    assert "huawei.com/Ascend" in result.stdout
    assert "sglang_token_usage" not in result.stdout
    assert "prometheus.io/scrape" not in result.stdout
    assert "name: vllm-qwen" in result.stdout
    assert "component: vllm" in result.stdout
    assert "app=vllm-qwen" in result.stdout


def test_sglang_render_uses_pinned_nvidia_profile():
    result = _render('--set', 'hardware=nvidia',
        "--set",
        "global.imageRegistry=",
        "--set",
        "engine.type=sglang",
        "--set",
        "sglang.toolCallParser=qwen",
    )
    assert result.returncode == 0, result.stderr
    manifest = result.stdout
    assert "name: sglang-qwen" in manifest
    assert "name: vllm-qwen" not in manifest
    assert "component: sglang" in manifest
    assert "image: lmsysorg/sglang:v0.5.15-cu129" in manifest
    assert "exec python -m sglang.launch_server" in manifest
    assert "--tool-call-parser qwen" in manifest
    assert "--enable-auto-tool-choice" not in manifest
    assert "--page-size 16" in manifest
    assert '"publisher":"zmq"' in manifest
    assert '"replay_endpoint":"tcp://*:5558"' in manifest
    assert "runtimeClassName: nvidia" in manifest
    assert "nvidia.com/gpu: 8" in manifest
    assert "huawei.com/Ascend" not in manifest
    assert 'name: INFERENCE_ENGINE' in manifest
    assert 'value: "sglang"' in manifest
    assert 'name: KV_HASH_BACKEND\n              value: "sglang"' in manifest
    assert (
        'name: SGLANG_CONTRACT_VERSION\n              value: "0.5.15"'
        in manifest
    )
    assert 'name: KV_BLOCK_SIZE' in manifest
    assert 'prometheus.io/path: "/metrics"' in manifest
    assert 'prometheus.io/port: "8200"' in manifest

def test_sglang_models_list_uses_component_engine_selector():
    result = _render('--set', 'hardware=nvidia',
        "--set",
        "engine.type=sglang",
        "--set-json",
        "models="
        + json.dumps(
            [
                {
                    "name": "qwen",
                    "engine": "sglang",
                    "modelSubPath": "qwen3-8b",
                }
            ]
        ),
    )
    assert result.returncode == 0, result.stderr
    assert "name: sglang-qwen" in result.stdout
    assert "component=sglang" in result.stdout
    assert "component: sglang" in result.stdout
    assert "kind: ServiceMonitor" in result.stdout
    assert "name: sglang" in result.stdout


def test_sglang_helper_fallback_uses_compatible_cuda_image():
    result = _render('--set', 'hardware=nvidia',
        "--set",
        "global.imageRegistry=",
        "--set",
        "engine.type=sglang",
        "--set",
        "images.sglang=",
    )
    assert result.returncode == 0, result.stderr
    assert "image: lmsysorg/sglang:v0.5.15-cu129" in result.stdout


def test_sglang_go_render_uses_go_images_env_probes_and_resources():
    result = _render('--set', 'hardware=nvidia',
        "--set",
        "global.imageRegistry=",
        "--set",
        "engine.type=sglang",
        "--set",
        "serviceImpl=go",
        "--set",
        "sglang.kvEvents.discoveryTimeoutS=3.5",
        "--set",
        "sglang.kvEvents.port=6557",
        "--set",
        "sglang.kvEvents.replayPort=6558",
        "--set",
        "sglang.kvEvents.topic=events@",
    )
    assert result.returncode == 0, result.stderr
    resources = [
        resource for resource in yaml.safe_load_all(result.stdout) if resource
    ]
    deployments = {
        resource["metadata"]["name"]: resource
        for resource in resources
        if resource.get("kind") == "Deployment"
    }

    router = next(
        container
        for container in deployments["router-service"]["spec"]["template"]["spec"][
            "containers"
        ]
        if container["name"] == "router"
    )
    engine_pod = deployments["sglang-qwen"]["spec"]["template"]["spec"]
    engine = next(
        container for container in engine_pod["containers"] if container["name"] == "sglang"
    )
    sidecar = next(
        container
        for container in engine_pod["containers"]
        if container["name"] == "kv-sidecar"
    )
    router_env = {item["name"]: item.get("value") for item in router["env"]}
    sidecar_env = {item["name"]: item.get("value") for item in sidecar["env"]}
    engine_ports = {item["name"]: item["containerPort"] for item in engine["ports"]}

    assert router["image"] == "kv-router-go:latest"
    assert sidecar["image"] == "kv-sidecar-go:latest"
    assert engine["image"] == "lmsysorg/sglang:v0.5.15-cu129"
    assert router_env["INFERENCE_ENGINE"] == "sglang"
    assert router_env["LABEL_SELECTOR"] == "app=sglang-qwen"
    assert router_env["KV_HASH_BACKEND"] == "sglang"
    assert router_env["SGLANG_CONTRACT_VERSION"] == "0.5.15"
    assert router_env["KV_BLOCK_SIZE"] == "16"
    assert router_env["HASH_SERVICE_URL"] == "http://127.0.0.1:9095"
    assert sidecar_env["INFERENCE_ENGINE"] == "sglang"
    assert sidecar_env["INFERENCE_URL"] == "http://127.0.0.1:8200"
    assert sidecar_env["INFERENCE_HOST"] == "127.0.0.1"
    assert sidecar_env["INFERENCE_HEALTH_PATH"] == "/health"
    assert sidecar_env["KV_EVENT_PORT"] == "6557"
    assert sidecar_env["KV_EVENT_REPLAY_PORT"] == "6558"
    assert sidecar_env["KV_EVENT_TOPIC"] == "events@"
    assert sidecar_env["KV_EVENT_EXPECTED_PAGE_SIZE"] == "16"
    assert sidecar_env["KV_EVENT_DISCOVERY_ENABLED"] == "true"
    assert sidecar_env["KV_EVENT_DISCOVERY_TIMEOUT_S"] == "3.5"
    assert sidecar["readinessProbe"]["httpGet"]["path"] == "/ready"
    assert sidecar["livenessProbe"]["httpGet"]["path"] == "/health"
    assert sidecar["resources"]["requests"] == {"cpu": "200m", "memory": "512Mi"}
    assert sidecar["resources"]["limits"] == {"cpu": "1000m", "memory": "1Gi"}
    assert engine["resources"]["requests"]["nvidia.com/gpu"] == 8
    assert engine["startupProbe"]["failureThreshold"] == 720
    assert engine["livenessProbe"]["failureThreshold"] == 5
    assert engine_ports["kvpub"] == 6557
    assert engine_ports["kvreplay"] == 6558
    assert "tcp://*:6557" in result.stdout
    assert "tcp://*:6558" in result.stdout
    assert '"topic":"events@' in result.stdout
    assert "huawei.com/Ascend" not in engine["resources"]["requests"]
    assert "VLLM_ENGINE" not in sidecar_env


def test_sglang_health_generate_is_readiness_only_and_keda_is_engine_aware():
    result = _render('--set', 'hardware=nvidia',
        "--set",
        "global.imageRegistry=",
        "--set",
        "engine.type=sglang",
        "--set",
        "sglang.readinessPath=/health_generate",
        "--set",
        "autoscaling.enabled=true",
        "--set",
        "autoscaling.signal=vllm",
    )
    assert result.returncode == 0, result.stderr
    manifest = result.stdout
    assert "path: /health_generate" in manifest
    assert manifest.count("path: /health_generate") == 1
    assert '__name__=~"sglang(:|_)token_usage"' in manifest
    assert "num_queue_reqs" not in manifest
    assert "num_running_reqs" not in manifest
    assert "vllm:gpu_cache_usage_perc" not in manifest
    assert 'threshold: "0.8"' in manifest


def test_sglang_is_a_first_class_keda_signal():
    result = _render('--set', 'hardware=nvidia',
        "--namespace",
        "tenant-a",
        "--set",
        "global.imageRegistry=",
        "--set",
        "engine.type=sglang",
        "--set",
        "autoscaling.enabled=true",
        "--set",
        "autoscaling.signal=sglang",
        "--set",
        "autoscaling.sglangThreshold=0.25",
    )
    assert result.returncode == 0, result.stderr
    manifest = result.stdout
    assert "laboom_autoscale_qwen_sglang" in manifest
    expected = (
        'max({__name__=~"sglang(:|_)token_usage",namespace="tenant-a",'
        'model_name="served-model"}) or vector(0)'
    )
    assert expected in manifest
    assert "num_queue_reqs" not in manifest
    assert "num_running_reqs" not in manifest
    assert 'namespace="default"' not in manifest
    assert 'threshold: "0.25"' in manifest


def test_sglang_keda_keeps_router_queue_and_custom_query_paths():
    queue = _render(
        "--namespace",
        "tenant-a",
        "--set",
        "autoscaling.enabled=true",
        "--set",
        "autoscaling.signal=queue",
    )
    assert queue.returncode == 0, queue.stderr
    assert (
        'router_central_queue_length_by_model{namespace="tenant-a",'
        'model="served-model"}'
    ) in queue.stdout

    custom = _render('--set', 'hardware=nvidia',
        "--set",
        "engine.type=sglang",
        "--set",
        "autoscaling.enabled=true",
        "--set",
        "autoscaling.signal=sglang",
        "--set-string",
        'autoscaling.sglangQuery=sum(custom_metric{scope="isolated"})',
    )
    assert custom.returncode == 0, custom.stderr
    assert 'sum(custom_metric{scope="isolated"})' in custom.stdout
    assert '__name__=~"sglang(:|_)token_usage"' not in custom.stdout


def test_sglang_engine_liveness_restarts_quickly_after_long_startup():
    result = _render('--set', 'hardware=nvidia',
        "--set",
        "engine.type=sglang",
        "--set",
        "sglang.livenessFailureThreshold=7",
    )
    assert result.returncode == 0, result.stderr
    resources = [
        resource for resource in yaml.safe_load_all(result.stdout) if resource
    ]
    deployment = next(
        resource
        for resource in resources
        if resource.get("kind") == "Deployment"
        and resource["metadata"]["name"] == "sglang-qwen"
    )
    engine = next(
        container
        for container in deployment["spec"]["template"]["spec"]["containers"]
        if container["name"] == "sglang"
    )
    assert engine["startupProbe"]["failureThreshold"] == 720
    assert engine["livenessProbe"]["failureThreshold"] == 7


@pytest.mark.parametrize(
    "arguments, expected",
    [
        (("--set", "router.hashSource=external"), "does not support external"),
        (("--set", "mooncake.enabled=true"), "does not support Mooncake"),
        (("--set", "lmcache.enabled=true"), "does not support LMCache"),
    ],
)
def test_sglang_rejects_unsupported_chart_combinations(arguments, expected):
    result = _render('--set', 'hardware=nvidia', "--set", "engine.type=sglang", *arguments)
    assert result.returncode != 0
    assert expected in result.stderr


@pytest.mark.parametrize("engine", ["vllm", "sglang"])
def test_helm_rejects_unknown_service_implementation_globally(engine):
    result = _render(
        "--set",
        f"engine.type={engine}",
        "--set",
        "serviceImpl=rust",
    )
    assert result.returncode != 0
    assert "expected python or go" in result.stderr


@pytest.mark.parametrize(
    "arguments, expected",
    [
        (
            (
                "--set",
                "engine.type=sglang",
                "--set",
                "router.strategy=affinity",
                "--set",
                "sglang.trustRemoteCode=true",
            ),
            "does not support trustRemoteCode",
        ),
        (
            (
                "--set",
                "engine.type=sglang",
                "--set-json",
                "models="
                + json.dumps(
                    [
                        {
                            "name": "qwen",
                            "engine": "sglang",
                            "modelSubPath": "qwen",
                            "sglang": {"trustRemoteCode": True},
                        }
                    ]
                ),
            ),
            "does not support trustRemoteCode",
        ),
        (
            (
                "--set",
                "engine.type=sglang",
                "--set-json",
                "models="
                + json.dumps(
                    [
                        {
                            "name": "sglang",
                            "engine": "sglang",
                            "modelSubPath": "sglang",
                        },
                        {
                            "name": "vllm",
                            "engine": "vllm",
                            "modelSubPath": "vllm",
                        },
                    ]
                ),
            ),
            "does not support mixed model engines",
        ),
        (
            (
                "--set",
                "engine.type=sglang",
                "--set-json",
                "models="
                + json.dumps(
                    [
                        {
                            "name": "a",
                            "engine": "sglang",
                            "modelSubPath": "a",
                        },
                        {
                            "name": "b",
                            "engine": "sglang",
                            "modelSubPath": "b",
                        },
                    ]
                ),
            ),
            "requires exactly one model",
        ),
        (
            (
                "--set",
                "engine.type=sglang",
                "--set-json",
                "models="
                + json.dumps(
                    [
                        {
                            "name": "qwen",
                            "engine": "sglang",
                            "modelSubPath": "qwen",
                            "sglang": {"pageSize": 32},
                        }
                    ]
                ),
            ),
            "must match global sglang.pageSize",
        ),
        (
            (
                "--set",
                "engine.type=sglang",
                "--set-json",
                "models="
                + json.dumps(
                    [
                        {
                            "name": "qwen",
                            "engine": "sglang",
                            "modelSubPath": "qwen",
                            "sglang": {"tokenizerPath": "/other"},
                        }
                    ]
                ),
            ),
            "cannot match the router-mounted /model tokenizer",
        ),
        (
            (
                "--set",
                "engine.type=sglang",
                "--set-json",
                "models="
                + json.dumps(
                    [
                        {
                            "name": "qwen",
                            "engine": "sglang",
                            "modelSubPath": "qwen",
                            "sglang": {"extraArgs": ["--model-path=/other"]},
                        }
                    ]
                ),
            ),
            "overrides chart-managed flag",
        ),
    ],
)
def test_helm_rejects_unsafe_sglang_router_contracts(arguments, expected):
    result = _render(*arguments)
    assert result.returncode != 0
    assert expected in result.stderr


def test_helm_allows_multi_model_sglang_when_prefix_hashing_is_disabled():
    models = [
        {"name": "a", "engine": "sglang", "modelSubPath": "a"},
        {"name": "b", "engine": "sglang", "modelSubPath": "b"},
    ]
    result = _render('--set', 'hardware=nvidia',
        "--set",
        "engine.type=sglang",
        "--set",
        "router.strategy=affinity",
        "--set-json",
        "models=" + json.dumps(models),
    )
    assert result.returncode == 0, result.stderr


def test_helm_allows_multi_model_sglang_when_router_is_not_deployed():
    models = [
        {"name": "a", "engine": "sglang", "modelSubPath": "a"},
        {"name": "b", "engine": "sglang", "modelSubPath": "b"},
    ]
    result = _render('--set', 'hardware=nvidia',
        "--set",
        "engine.type=sglang",
        "--set",
        "deploy.router=false",
        "--set-json",
        "models=" + json.dumps(models),
    )
    assert result.returncode == 0, result.stderr


def test_helm_measurement_reenables_single_sglang_model_boundary():
    models = [
        {"name": "a", "engine": "sglang", "modelSubPath": "a"},
        {"name": "b", "engine": "sglang", "modelSubPath": "b"},
    ]
    result = _render('--set', 'hardware=nvidia',
        "--set",
        "engine.type=sglang",
        "--set",
        "router.strategy=affinity",
        "--set",
        "router.measurePrefix=true",
        "--set-json",
        "models=" + json.dumps(models),
    )
    assert result.returncode != 0
    assert "requires exactly one model" in result.stderr


def test_helm_preserves_multi_model_vllm_prefix_render():
    models = [
        {"name": "a", "engine": "vllm", "modelSubPath": "a"},
        {"name": "b", "engine": "vllm", "modelSubPath": "b"},
    ]
    result = _render(
        "--set",
        "engine.type=vllm",
        "--set",
        "router.strategy=prefix",
        "--set-json",
        "models=" + json.dumps(models),
    )
    assert result.returncode == 0, result.stderr
    assert "sglang.launch_server" not in result.stdout
    assert "name: vllm-a" in result.stdout
    assert "name: vllm-b" in result.stdout
    assert "component: vllm" in result.stdout
