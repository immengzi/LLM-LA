#!/usr/bin/env python3
"""
redis_verify.py
~~~~~~~~~~~~~~~
Cross-check that Redis actually stores the KV block hashes the router computed.

Reads the router's own per-request truth (``router_logs.json``, the NDJSON the
RouterLogCollector persists from ``/latency_log``). For every record that
carries ``block_hashes`` (router run with ROUTER_LOG_BLOCK_HASHES=true) it asks
Redis, per block:

  * EXISTS  <model>:kvblock:<hash>            -> is the block stored at all?
  * HEXISTS <model>:kvblock:<hash> <endpoint> -> does the pod we routed to own it?

It recomputes the contiguous leading-prefix match from Redis and compares it to
the router's logged ``kv_hits_len``. Results (per-record + a summary) are written
to ``redis_verify.json`` in the experiment directory.

Redis is reached through ``kubectl exec`` into the redis pod (no python redis
dependency, no NodePort needed) -- the same path debug/verify_redis_blocks.py
uses. Fully best-effort: any failure is captured in the output and never raises.

This is the batch/inline counterpart of debug/verify_redis_blocks.py, wired into
main.py so a sweep run produces the verification artifact automatically.
"""
from __future__ import annotations

import json
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


class _Redis:
    def __init__(self, namespace: str, kubectl: str, selector: str,
                 pod: Optional[str], cli_cmd: Optional[str]):
        self.namespace = namespace
        self.kubectl = kubectl
        self.selector = selector
        self._pod = pod
        self._cli_cmd = cli_cmd

    def _detect_pod(self) -> str:
        if self._pod:
            return self._pod
        out = subprocess.run(
            [self.kubectl, "-n", self.namespace, "get", "pod", "-l", self.selector,
             "-o", "jsonpath={.items[0].metadata.name}"],
            capture_output=True, text=True,
        )
        pod = out.stdout.strip()
        if out.returncode != 0 or not pod:
            raise RuntimeError(
                f"could not find redis pod (ns={self.namespace}, "
                f"selector={self.selector}): {out.stderr.strip()}"
            )
        self._pod = pod
        return pod

    def _base_cmd(self) -> List[str]:
        if self._cli_cmd:
            return shlex.split(self._cli_cmd)
        pod = self._detect_pod()
        return [self.kubectl, "-n", self.namespace, "exec", "-i", pod, "--", "redis-cli"]

    def run_pipe(self, commands: List[str]) -> List[str]:
        if not commands:
            return []
        payload = "\n".join(commands) + "\n"
        proc = subprocess.run(
            self._base_cmd(), input=payload, capture_output=True, text=True,
        )
        if proc.returncode != 0 and not self._cli_cmd and self._pod:
            # Pod may have been recreated; drop cached pod and retry once.
            self._pod = None
            proc = subprocess.run(
                self._base_cmd(), input=payload, capture_output=True, text=True,
            )
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.strip() or "redis-cli failed")
        return proc.stdout.splitlines()


def _verify_record(rec: dict, redis: _Redis, max_blocks: int,
                   model_default: str) -> Optional[dict]:
    hashes = rec.get("block_hashes")
    if not hashes:
        return None

    model = rec.get("model") or model_default or ""
    endpoint = rec.get("endpoint_id") or rec.get("endpoint") or ""
    if not model or not endpoint:
        return None

    if max_blocks and max_blocks > 0:
        hashes = hashes[:max_blocks]

    cmds: List[str] = []
    for h in hashes:
        key = f"{model}:kvblock:{h}"
        cmds.append(f"EXISTS {key}")
        cmds.append(f"HEXISTS {key} {endpoint}")

    replies = redis.run_pipe(cmds)
    if len(replies) != 2 * len(hashes):
        raise RuntimeError(
            f"unexpected redis reply count: got {len(replies)}, "
            f"expected {2 * len(hashes)}"
        )

    present = owned = redis_prefix = 0
    prefix_broken = False
    for i in range(len(hashes)):
        ex = replies[2 * i].strip() == "1"
        he = replies[2 * i + 1].strip() == "1"
        if ex:
            present += 1
        if he:
            owned += 1
        if not prefix_broken:
            if he:
                redis_prefix += 1
            else:
                prefix_broken = True

    return {
        "req_id": str(rec.get("req_id", ""))[:12],
        "endpoint": endpoint,
        "model": model,
        "n": len(hashes),
        "present": present,
        "owned": owned,
        "redis_prefix": redis_prefix,
        "router_kv_hits": rec.get("kv_hits_len"),
        "router_total": rec.get("total_blocks"),
        "hashes_match": present == len(hashes),
    }


def _dedup_records(logs_path: Path) -> List[dict]:
    """Load NDJSON, keeping the last record per (req_id) so re-polled ring
    entries are not double-counted. Records without req_id are all kept."""
    by_rid: Dict[str, dict] = {}
    anon: List[dict] = []
    with logs_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            rid = rec.get("req_id") or rec.get("rid")
            if rid:
                by_rid[str(rid)] = rec
            else:
                anon.append(rec)
    return list(by_rid.values()) + anon


def verify_router_logs(
    router_logs_path: str | Path,
    out_path: str | Path,
    *,
    namespace: str = "vllm",
    kubectl: str = "kubectl",
    selector: str = "app=redis",
    pod: Optional[str] = None,
    cli_cmd: Optional[str] = None,
    max_blocks: int = 0,
    model_default: str = "",
) -> Dict[str, Any]:
    """Verify block hashes in router_logs against Redis; write redis_verify.json.

    Returns the summary dict. Best-effort: on any fatal error the summary carries
    an ``error`` field and is still written to disk.
    """
    router_logs_path = Path(router_logs_path)
    out_path = Path(out_path)

    summary: Dict[str, Any] = {
        "checked": 0, "hashes_match": 0, "missing": 0, "records_with_hashes": 0,
    }
    records_out: List[dict] = []

    if not router_logs_path.is_file():
        summary["error"] = f"router logs not found: {router_logs_path}"
        _write(out_path, summary, records_out)
        return summary

    redis = _Redis(namespace, kubectl, selector, pod, cli_cmd)

    try:
        recs = _dedup_records(router_logs_path)
    except Exception as e:
        summary["error"] = f"failed to read {router_logs_path}: {e}"
        _write(out_path, summary, records_out)
        return summary

    redis_errors = 0
    for rec in recs:
        if not rec.get("block_hashes"):
            continue
        summary["records_with_hashes"] += 1
        try:
            result = _verify_record(rec, redis, max_blocks, model_default)
        except Exception as e:
            redis_errors += 1
            if redis_errors <= 3:
                print(f"[redis-verify] redis error: {e}", file=sys.stderr)
            continue
        if result is None:
            continue
        records_out.append(result)
        summary["checked"] += 1
        if result["hashes_match"]:
            summary["hashes_match"] += 1
        else:
            summary["missing"] += 1

    if redis_errors:
        summary["redis_errors"] = redis_errors
    _write(out_path, summary, records_out)
    return summary


def _write(out_path: Path, summary: Dict[str, Any], records: List[dict]) -> None:
    try:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8") as f:
            json.dump({"summary": summary, "records": records}, f, indent=2)
    except Exception as e:
        print(f"[redis-verify] failed to write {out_path}: {e}", file=sys.stderr)
