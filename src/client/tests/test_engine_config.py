import sys
from pathlib import Path

import click
import pytest
import yaml


CLIENT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CLIENT_DIR))

from config import HelmConfig, load_config, migrate_legacy_helm_to_models
from sweep_methods import _inject_sglang_models, _validate_engine_config


def test_default_migration_preserves_vllm_model_shape():
    helm = HelmConfig()

    migrate_legacy_helm_to_models(helm)

    model = helm.models[0]
    assert "engine" not in model
    assert "sglang" not in model
    assert "image" not in model
    assert model["vllm"]["gpuMemoryUtilization"] == 0.95


def test_load_and_migrate_sglang_config(tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "helm": {
                    "engine_type": "sglang",
                    "sglang_page_size": 32,
                    "sglang_mem_fraction_static": 0.82,
                    "sglang_tool_call_parser": "qwen3_coder",
                    "sglang_extra_args": ["--max-running-requests=64"],
                }
            }
        ),
        encoding="utf-8",
    )

    loaded = load_config(str(config_path))
    helm = loaded.helm
    migrate_legacy_helm_to_models(helm)

    assert loaded.metrics.engine_type == "sglang"
    model = helm.models[0]
    assert model["engine"] == "sglang"
    assert model["image"] == "lmsysorg/sglang:v0.5.15-cu129"
    assert model["sglang"] == {
        "pageSize": 32,
        "memFractionStatic": 0.82,
        "trustRemoteCode": False,
        "extraArgs": ["--max-running-requests=64"],
        "healthPath": "/health",
        "readinessPath": "",
        "metricsPath": "/metrics",
        "metricsPort": 8200,
        "toolCallParser": "qwen3_coder",
    }


def test_sglang_validation_rejects_unsupported_combinations():
    helm = HelmConfig(
        engine_type="sglang",
        service_impl="go",
        router_hash_source="external",
        data_parallel_enabled=True,
        mooncake_enabled=True,
        lmcache_enabled=True,
    )

    try:
        _validate_engine_config(helm)
    except click.ClickException as exc:
        message = str(exc)
    else:
        raise AssertionError("unsupported SGLang configuration was accepted")

    assert "router_hash_source must be inline" in message
    assert "data_parallel/LWS is not supported" in message
    assert "Mooncake is not supported" in message
    assert "LMCache is not supported" in message


@pytest.mark.parametrize("service_impl", ["python", "go"])
def test_sglang_validation_accepts_both_service_implementations(service_impl):
    helm = HelmConfig(
        engine_type="sglang",
        service_impl=service_impl,
        models=[{"name": "qwen", "engine": "sglang"}],
    )
    assert _validate_engine_config(helm) == "sglang"


@pytest.mark.parametrize("engine_type", ["vllm", "sglang"])
def test_engine_validation_rejects_unknown_service_implementation(engine_type):
    helm = HelmConfig(engine_type=engine_type, service_impl="rust")

    with pytest.raises(click.ClickException, match="expected 'python' or 'go'"):
        _validate_engine_config(helm)


def test_engine_validation_rejects_unknown_engine():
    helm = HelmConfig(engine_type="tgi")

    try:
        _validate_engine_config(helm)
    except click.ClickException as exc:
        assert "expected 'vllm' or 'sglang'" in str(exc)
    else:
        raise AssertionError("unknown engine was accepted")


def test_sglang_model_injection_preserves_per_model_overrides():
    helm = HelmConfig(engine_type="sglang", sglang_page_size=16)
    models = [{"name": "qwen", "sglang": {"pageSize": 64}}]

    _inject_sglang_models(helm, models)

    assert models[0]["engine"] == "sglang"
    assert models[0]["image"] == "lmsysorg/sglang:v0.5.15-cu129"
    assert models[0]["sglang"]["pageSize"] == 64


@pytest.mark.parametrize(
    "helm, expected",
    [
        (
            HelmConfig(
                engine_type="sglang",
                router_strategy="affinity",
                sglang_trust_remote_code=True,
            ),
            "sglang_trust_remote_code",
        ),
        (
            HelmConfig(
                engine_type="sglang",
                models=[
                    {
                        "name": "qwen",
                        "engine": "sglang",
                        "sglang": {"trustRemoteCode": True},
                    }
                ],
            ),
            "trustRemoteCode",
        ),
        (
            HelmConfig(
                engine_type="sglang",
                models=[
                    {"name": "a", "engine": "sglang"},
                    {"name": "b", "engine": "vllm"},
                ],
            ),
            "mixed model engines",
        ),
        (
            HelmConfig(
                engine_type="sglang",
                models=[
                    {"name": "a", "engine": "sglang"},
                    {"name": "b", "engine": "sglang"},
                ],
            ),
            "requires exactly one model",
        ),
        (
            HelmConfig(
                engine_type="sglang",
                sglang_page_size=16,
                models=[
                    {
                        "name": "qwen",
                        "engine": "sglang",
                        "sglang": {"pageSize": 32},
                    }
                ],
            ),
            "pageSize must match",
        ),
        (
            HelmConfig(
                engine_type="sglang",
                models=[
                    {
                        "name": "qwen",
                        "engine": "sglang",
                        "sglang": {"tokenizerPath": "/other"},
                    }
                ],
            ),
            "custom SGLang model/tokenizer overrides",
        ),
        (
            HelmConfig(
                engine_type="sglang",
                sglang_extra_args=["--page-size=32"],
            ),
            "sglang_extra_args cannot override",
        ),
        (
            HelmConfig(
                engine_type="sglang",
                models=[
                    {
                        "name": "qwen",
                        "engine": "sglang",
                        "sglang": {"extraArgs": ["--model-path=/other"]},
                    }
                ],
            ),
            r"models\[\]\.sglang\.extraArgs cannot override",
        ),
    ],
)
def test_client_rejects_unsafe_sglang_router_contracts(helm, expected):
    with pytest.raises(click.ClickException, match=expected):
        _validate_engine_config(helm)


def test_client_allows_multi_model_sglang_when_prefix_hashing_is_disabled():
    helm = HelmConfig(
        engine_type="sglang",
        router_strategy="affinity",
        models=[
            {"name": "a", "engine": "sglang"},
            {"name": "b", "engine": "sglang"},
        ],
    )
    assert _validate_engine_config(helm) == "sglang"


def test_client_allows_multi_model_sglang_when_router_is_not_deployed():
    helm = HelmConfig(
        engine_type="sglang",
        router_strategy="prefix",
        models=[
            {"name": "a", "engine": "sglang"},
            {"name": "b", "engine": "sglang"},
        ],
    )
    assert _validate_engine_config(helm, router_deployed=False) == "sglang"


def test_client_preserves_multi_model_vllm_prefix_validation():
    helm = HelmConfig(
        engine_type="vllm",
        router_strategy="prefix",
        models=[
            {"name": "a", "engine": "vllm"},
            {"name": "b", "engine": "vllm"},
        ],
    )
    assert _validate_engine_config(helm) == "vllm"
