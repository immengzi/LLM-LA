#!/usr/bin/env python3
from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import click
import yaml


REPO_ROOT = Path(__file__).resolve().parent
CONFIGS_DIR = REPO_ROOT / "configs"
EXPERIMENTS_ROOT = REPO_ROOT / "experiments"


# ---------------------------
# subprocess helpers
# ---------------------------

def _run(cmd: List[str], *, check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    click.echo(f"[cmd] {' '.join(cmd)}")
    return subprocess.run(
        cmd,
        check=check,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.STDOUT if capture else None,
    )


def _kubectl(args: List[str], *, check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    return _run(["kubectl", *args], check=check, capture=capture)


# ---------------------------
# experiment dir detection
# ---------------------------

def _snapshot_existing_experiments() -> set[str]:
    if not EXPERIMENTS_ROOT.exists():
        return set()
    return {p.name for p in EXPERIMENTS_ROOT.iterdir() if p.is_dir() and p.name.isdigit()}


def _newest_experiment_dir(before: set[str]) -> Optional[Path]:
    if not EXPERIMENTS_ROOT.exists():
        return None
    after = {p.name for p in EXPERIMENTS_ROOT.iterdir() if p.is_dir() and p.name.isdigit()}
    new = sorted(list(after - before), key=lambda s: int(s))
    if not new:
        return None
    return EXPERIMENTS_ROOT / new[-1]


# ---------------------------
# master plan parsing
# ---------------------------

def _resolve_client_config_path(key: str) -> Path:
    """
    master_config.yaml keys can be:
      - "a.yaml"          -> configs/a.yaml
      - "configs/a.yaml"  -> repo_root/configs/a.yaml
      - "/abs/path/a.yaml"
    """
    p = Path(key).expanduser()

    if not p.is_absolute():
        parts = p.parts
        if parts and parts[0] == "configs":
            p = REPO_ROOT / p
        else:
            p = CONFIGS_DIR / p

    return p.resolve()


def _load_master_plan(master_path: Path) -> Dict[Path, List[str]]:
    raw = yaml.safe_load(master_path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict) or not raw:
        raise RuntimeError("master_config.yaml must be a non-empty mapping: {config: [methods...] }")

    plan: Dict[Path, List[str]] = {}
    for k, v in raw.items():
        if not isinstance(k, str) or not k.strip():
            raise RuntimeError(f"Invalid config key: {k!r}")
        if not isinstance(v, list) or not all(isinstance(x, str) and x.strip() for x in v):
            raise RuntimeError(f"Invalid methods list for {k!r}. Expected a YAML list of strings.")

        cfg_path = _resolve_client_config_path(k)
        methods = [x.strip() for x in v]
        plan[cfg_path] = methods

    return plan


# ---------------------------
# k8s manifest helpers
# ---------------------------

def _load_multi_doc_yaml(path: Path) -> List[dict]:
    docs = list(yaml.safe_load_all(path.read_text(encoding="utf-8")))
    out: List[dict] = []
    for d in docs:
        if d is None:
            continue
        if not isinstance(d, dict):
            raise RuntimeError(f"Non-mapping YAML document in {path}")
        out.append(d)
    return out


def _infer_namespace_from_manifest(docs: List[dict]) -> str:
    """
    Use the namespace declared in the manifest itself:
      kind: Namespace
      metadata:
        name: <ns>
    """
    for obj in docs:
        if obj.get("kind") == "Namespace":
            meta = obj.get("metadata") or {}
            name = meta.get("name")
            if isinstance(name, str) and name.strip():
                return name.strip()
    raise RuntimeError(
        "Could not infer namespace: no 'kind: Namespace' doc with metadata.name found in vllm-k8s.yaml"
    )


def _set_env_var(container: dict, key: str, value: str) -> bool:
    env = container.get("env")
    if not isinstance(env, list):
        return False
    for item in env:
        if isinstance(item, dict) and item.get("name") == key:
            item["value"] = value
            return True
    return False


def _patch_router_mode(docs: List[dict], *, router_mode: str) -> None:
    """
    Patch ONLY what your k8s config already defines:
      Deployment/router-service -> container name=router -> env ROUTER_MODE
    """
    changed = False
    for obj in docs:
        if obj.get("kind") != "Deployment":
            continue
        meta = obj.get("metadata") or {}
        if meta.get("name") != "router-service":
            continue

        spec = obj.get("spec") or {}
        tpl_spec = ((spec.get("template") or {}).get("spec") or {})
        containers = tpl_spec.get("containers") or []
        for c in containers:
            if isinstance(c, dict) and c.get("name") == "router":
                if _set_env_var(c, "ROUTER_MODE", router_mode):
                    changed = True

    if not changed:
        raise RuntimeError(
            "Failed to patch ROUTER_MODE. Expected in vllm-k8s.yaml:\n"
            "Deployment metadata.name: router-service\n"
            "container name: router\n"
            "env: - name: ROUTER_MODE\n"
        )


def _write_multi_doc_yaml(docs: List[dict], out_path: Path) -> None:
    with out_path.open("w", encoding="utf-8") as f:
        yaml.safe_dump_all(docs, f, sort_keys=False)


# ---------------------------
# k8s lifecycle
# ---------------------------

def _delete_and_apply(rendered_manifest: Path) -> None:
    _kubectl(["delete", "-f", str(rendered_manifest), "--ignore-not-found=true"], check=False)
    _kubectl(["apply", "-f", str(rendered_manifest)], check=True)


def _wait_ready(namespace: str, timeout_s: float = 900.0) -> None:
    """
    Safety timeout only (not a config knob).
    """
    deadline = time.time() + float(timeout_s)

    for kind in ("deploy", "sts", "ds"):
        try:
            out = _kubectl(["get", kind, "-n", namespace, "-o", "name"], capture=True).stdout or ""
        except subprocess.CalledProcessError:
            continue

        names = [ln.strip() for ln in out.splitlines() if ln.strip()]
        for name in names:
            remaining = max(1, int(deadline - time.time()))
            try:
                _kubectl(["rollout", "status", name, "-n", namespace, f"--timeout={remaining}s"], check=True)
            except subprocess.CalledProcessError:
                click.echo(f"[warn] rollout status failed for {name}, continuing...")

    remaining = max(1, int(deadline - time.time()))
    _kubectl(
        ["wait", "-n", namespace, "--for=condition=Ready", "pod", "--all", f"--timeout={remaining}s"],
        check=True,
    )


# ---------------------------
# run client
# ---------------------------

def _run_client(config_path: Path) -> None:
    _run([sys.executable, str(REPO_ROOT / "main.py"), "--config", str(config_path)], check=True)


# ---------------------------
# Click CLI (no knobs)
# ---------------------------

@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
def cli() -> None:
    """
    Reads configs/1-master_config.yaml (only: config -> methods),
    patches vllm-k8s.yaml ROUTER_MODE accordingly, redeploys, then runs main.py.
    """
    master_path = (CONFIGS_DIR / "1-master_config.yaml").resolve()
    manifest_path = (REPO_ROOT / "vllm-k8s.yaml").resolve()

    if not master_path.is_file():
        raise click.ClickException(f"Missing {master_path}")
    if not manifest_path.is_file():
        raise click.ClickException(f"Missing {manifest_path}")

    plan = _load_master_plan(master_path)

    for cfg in plan.keys():
        if not cfg.is_file():
            raise click.ClickException(f"Client config not found: {cfg}")

    base_docs = _load_multi_doc_yaml(manifest_path)
    namespace = _infer_namespace_from_manifest(base_docs)

    jobs: List[Tuple[Path, str]] = []
    for cfg, methods in plan.items():
        for m in methods:
            jobs.append((cfg, m))

    click.echo(f"[sweep] master_config={master_path}")
    click.echo(f"[sweep] manifest={manifest_path}")
    click.echo(f"[sweep] namespace(inferred)={namespace}")
    click.echo(f"[sweep] jobs={len(jobs)}")

    for i, (cfg_path, method) in enumerate(jobs, start=1):
        click.echo("\n" + "=" * 90)
        click.echo(f"[sweep] job {i}/{len(jobs)}  config={cfg_path.name}  method={method}")
        click.echo("=" * 90)

        docs_list = _load_multi_doc_yaml(manifest_path)
        _patch_router_mode(docs_list, router_mode=method)

        with tempfile.TemporaryDirectory(prefix="k8s_sweep_") as td:
            rendered = Path(td) / "rendered.yaml"
            _write_multi_doc_yaml(docs_list, rendered)

            _delete_and_apply(rendered)
            _wait_ready(namespace)

            before = _snapshot_existing_experiments()
            _run_client(cfg_path)
            exp_dir = _newest_experiment_dir(before)

            if exp_dir is None:
                click.echo("[warn] could not detect new experiment dir; skipping artifact snapshot")
                continue

            shutil.copy2(rendered, exp_dir / "vllm-k8s.yaml")
            meta = {
                "client_config": str(cfg_path),
                "router_method": method,
                "master_config": str(master_path),
                "ts_unix": time.time(),
            }
            (exp_dir / "sweep_meta.json").write_text(
                json.dumps(meta, indent=2, sort_keys=True), encoding="utf-8"
            )

            click.echo(f"[sweep] experiment_dir={exp_dir}")

    click.echo("\n[sweep] done.")


if __name__ == "__main__":
    cli()
