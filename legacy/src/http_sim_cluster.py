# http_sim_cluster.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import os, sys, subprocess, time, signal
from typing import List, Tuple

import click
import requests

from config import load_config, set_config

# ----------------------------
# Helpers: endpoint expansion
# ----------------------------

def _expand_eps_from_cfg(cfg) -> List[Tuple[str, float]]:
    """
    Returns list of (name, sec_per_token).

    Accepts in cfg.SIM_ENDPOINTS either:
      - dict:
          foo: { sec_per_token: 0.008, count: 2 }
          bar: { spt: 0.010 }
      - list:
          ["foo@0.008", "bar@0.010"]

    No TPS anywhere. If no speed is provided, defaults to 0.01 sec/token.
    """
    sim = getattr(cfg, "SIM_ENDPOINTS", {})
    eps: List[Tuple[str, float]] = []

    def _get_spt(spec) -> float:
        if isinstance(spec, dict):
            if "sec_per_token" in spec:
                return float(spec["sec_per_token"])
            if "spt" in spec:
                return float(spec["spt"])
        return 0.01  # default ~100 tok/s

    if isinstance(sim, dict):
        for base_name in sorted(sim.keys()):
            spec = sim[base_name] or {}
            spt = _get_spt(spec)
            count = int(spec.get("count", 1))
            for i in range(count):
                name = f"{base_name}-{i+1}" if count > 1 else base_name
                eps.append((name, spt))

    elif isinstance(sim, list):
        for item in sim:
            if isinstance(item, str) and "@" in item:
                n, val = item.split("@", 1)
                spt = float(val.strip())
                eps.append((n.strip(), spt))

    return eps

def _http_endpoints(cfg) -> List[str]:
    host = getattr(cfg, "SIM_HTTP_HOST", "127.0.0.1")
    base = int(getattr(cfg, "SIM_HTTP_PORT_BASE", 9101))
    urls: List[str] = []
    idx = 0
    sim = getattr(cfg, "SIM_ENDPOINTS", {})
    if isinstance(sim, dict):
        for base_name in sorted(sim.keys()):
            spec = sim[base_name] or {}
            count = int(spec.get("count", 1))
            for _ in range(count):
                urls.append(f"http://{host}:{base+idx}")
                idx += 1
    elif isinstance(sim, list):
        for _ in sim:
            urls.append(f"http://{host}:{base+idx}")
            idx += 1
    return urls

# ----------------------------
# CLI
# ----------------------------

@click.command()
@click.option(
    "--config",
    "config_path",
    type=str,
    default="sim_distro",
    show_default=True,
    help="Profile under BASE_CONFIGS_PATH (loads <name>.yaml)",
)
@click.option(
    "--wait-health/--no-wait-health",
    default=True,
    show_default=True,
    help="Wait for all /health to pass before returning control",
)
@click.option(
    "--health-timeout",
    type=float,
    default=10.0,
    show_default=True,
    help="Seconds to wait for /health",
)
def main(config_path: str, wait_health: bool, health_timeout: float):
    cfg = load_config(config_path)
    set_config(cfg)
    host = getattr(cfg, "SIM_HTTP_HOST", "127.0.0.1")
    base_port = int(getattr(cfg, "SIM_HTTP_PORT_BASE", 9101))

    eps = _expand_eps_from_cfg(cfg)
    if not eps:
        print("[SIM] No SIM_ENDPOINTS configured. Nothing to launch.")
        return

    procs = []
    launched: List[Tuple[str, float, str, int]] = []  # (name, sec_per_token, url, port)

    # Engine is fixed to VT (asyncio) in the server
    engine = "vt"

    try:
        for idx, (name, spt) in enumerate(eps):
            port = base_port + idx
            env = os.environ.copy()
            env["CONFIG_PATH"] = config_path
            env["NAME"] = name
            env["SEC_PER_TOKEN"] = str(spt)
            env["HOST"] = host
            env["PORT"] = str(port)

            p = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "uvicorn",
                    "http_sim_server:app",
                    "--host",
                    host,
                    "--port",
                    str(port),
                    "--workers",
                    "1",
                ],
                env=env,
            )
            procs.append(p)
            url = f"http://{host}:{port}"
            launched.append((name, spt, url, port))
            print(f"[SIM] launched {name} at {url} (engine={engine})")

        if wait_health:
            urls = _http_endpoints(cfg)
            deadline = time.time() + health_timeout
            remaining = set(urls)
            while remaining and time.time() < deadline:
                done = set()
                for url in list(remaining):
                    try:
                        r = requests.get(url + cfg.HEALTH_PATH, timeout=1.0)
                        if r.ok:
                            done.add(url)
                    except Exception:
                        pass
                remaining -= done
                if remaining:
                    time.sleep(0.25)

            if remaining:
                print("[SIM] WARNING: health check timed out for:", sorted(remaining))
            else:
                print("[SIM] all simulated endpoints are healthy.")

        # ---- Endpoint → server summary (confirmed via /health) ----
        print("\n[SIM] Endpoint map (confirmed):")
        for name, spt, url, port in launched:
            resolved = {"name": name, "sec_per_token": spt, "engine": engine}
            try:
                r = requests.get(url + cfg.HEALTH_PATH, timeout=1.0)
                if r.ok:
                    data = r.json() or {}
                    resolved["name"] = data.get("name", name)
                    resolved["sec_per_token"] = data.get("sec_per_token", spt)
                    resolved["engine"] = data.get("engine", engine)
            except Exception:
                pass
            print(
                f"  {url:<27} -> {resolved['name']} (sec_per_token={resolved['sec_per_token']}, engine={resolved['engine']})"
            )

        print(f"\n[SIM] {len(procs)} endpoints up. Press Ctrl+C to stop.")
        while True:
            time.sleep(3600)

    except KeyboardInterrupt:
        print("\n[SIM] Stopping...")
    finally:
        for p in procs:
            try:
                p.send_signal(signal.SIGINT)
            except Exception:
                pass
        for p in procs:
            try:
                p.wait(timeout=5)
            except Exception:
                try:
                    p.kill()
                except Exception:
                    pass


if __name__ == "__main__":
    main()
