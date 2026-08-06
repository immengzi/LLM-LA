"""Inference-engine-specific health probing behavior."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class EngineHealthProfile:
    engine_type: str
    health_path: str
    readiness_path: str

    def accepts(self, response: Any) -> bool:
        """Interpret a health response for this engine."""
        return int(getattr(response, "status_code", 0)) == 200


def get_engine_health_profile(
    engine_type: str,
    *,
    health_path: str | None = None,
    readiness_path: str | None = None,
) -> EngineHealthProfile:
    engine = str(engine_type or "vllm").strip().lower()
    if engine not in {"vllm", "sglang"}:
        engine = "vllm"

    default_health = "/health"
    health = str(health_path or default_health).strip() or default_health

    # /health_generate can execute a generation and must never become the
    # liveness path. It is accepted only as an explicit readiness override.
    if health.rstrip("/") == "/health_generate":
        health = default_health
    ready = str(readiness_path or health).strip() or health

    return EngineHealthProfile(
        engine_type=engine,
        health_path=health,
        readiness_path=ready,
    )
