# sim_backend_http_shim.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import List, Optional
from config import load_config, get_config, set_config

_HTTP_EPS: List[str] = []


def _iter_http_eps(cfg) -> List[str]:
    host = getattr(cfg, "SIM_HTTP_HOST", "127.0.0.1")
    base = int(getattr(cfg, "SIM_HTTP_PORT_BASE", 9101))
    eps: List[str] = []
    sim = getattr(cfg, "SIM_ENDPOINTS", {})
    idx = 0
    if isinstance(sim, dict):
        for base_name in sorted(sim.keys()):
            spec = sim[base_name] or {}
            count = int(spec.get("count", 1))
            for _ in range(count):
                eps.append(f"http://{host}:{base + idx}")
                idx += 1
    elif isinstance(sim, list):
        for item in sim:
            if isinstance(item, str) and "@" in item:
                eps.append(f"http://{host}:{base + idx}")
                idx += 1
    return eps


def configure(config_path: Optional[str] = None) -> None:
    cfg = load_config(config_path) if config_path is not None else get_config()
    set_config(cfg)
    global _HTTP_EPS
    _HTTP_EPS = _iter_http_eps(cfg)


def endpoints() -> List[str]:
    return list(_HTTP_EPS)


def has_endpoint(url: str) -> bool:
    return url in _HTTP_EPS
