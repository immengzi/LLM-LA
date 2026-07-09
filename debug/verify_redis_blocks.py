#!/usr/bin/env python3
"""
verify_redis_blocks.py
~~~~~~~~~~~~~~~~~~~~~~~~
Reads a running experiment log (the NDJSON ``logs.json`` produced by
prod_latency_collector.py) and, for each request, confirms that Redis is
actually storing the same KV block hashes the router computed.

For every request record that carries ``block_hashes`` (requires the router to
run with ROUTER_LOG_BLOCK_HASHES=true) the script asks Redis two things per
block:

  * EXISTS  <model>:kvblock:<hash>            -> is the block stored at all?
  * HEXISTS <model>:kvblock:<hash> <endpoint> -> does the pod we routed to own it?

It then recomputes the contiguous leading-prefix match from Redis and compares
it to the router's logged ``kv_hits_len``. A match means the router's hashing
and Redis agree end to end.

Redis key prefix is taken from each record's ``model`` field (e.g.
"served-model-minmax"), which is exactly what the sidecar writes under
(MODEL_NAME_REDIS = servedModelName). This is independent of the router's own
MODEL_NAME, so it stays correct even if the router looks under the wrong prefix.

Redis is reached through ``kubectl exec`` into the redis pod by default (no
python redis dependency, no NodePort needed).

Usage (run from the repo root, e.g. python debug/verify_redis_blocks.py ...)
-----
    # LIVE: poll the router directly, then send requests with Claude Code and
    # watch each one's router hashes get confirmed against Redis (no collector):
    python debug/verify_redis_blocks.py --router-url http://192.168.0.79:30080 -n vllm

    # Watch the newest experiment logs.json under a root instead:
    python debug/verify_redis_blocks.py --experiments-root /data/experiments -n vllm

    # Point at a specific logs.json:
    python debug/verify_redis_blocks.py --logs /data/experiments/49/logs.json -n vllm

    # One-shot over whatever is already there, then exit:
    python debug/verify_redis_blocks.py --router-url http://192.168.0.79:30080 --once
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Set, Tuple

DEFAULT_ROOT_CANDIDATES = ["/data/experiments", "src/client/experiments", "experiments"]


# --------------------------------------------------------------------------- #
# Locating the experiment log
# --------------------------------------------------------------------------- #
def _newest_logs_under(root: Path) -> Optional[Path]:
    """Return the logs.json of the highest-numbered experiment dir under root."""
    if not root.is_dir():
        return None
    best: Optional[Tuple[int, Path]] = None
    for child in root.iterdir():
        if not child.is_dir():
            continue
        logs = child / "logs.json"
        if not logs.is_file():
            continue
        try:
            n = int(child.name)
        except ValueError:
            n = -1
        key = (n, logs)
        if best is None or key[0] > best[0] or (key[0] == best[0] and logs.stat().st_mtime > best[1].stat().st_mtime):
            best = key
    return best[1] if best else None


def resolve_logs_path(args) -> Path:
    if args.logs:
        p = Path(args.logs)
        if not p.is_file():
            sys.exit(f"[verify] logs file not found: {p}")
        return p

    roots = [args.experiments_root] if args.experiments_root else DEFAULT_ROOT_CANDIDATES
    for r in roots:
        found = _newest_logs_under(Path(r))
        if found:
            print(f"[verify] using newest experiment log: {found}")
            return found
    sys.exit(
        "[verify] could not find a logs.json. Pass --logs <path> or "
        f"--experiments-root <dir> (looked in: {roots})"
    )


# --------------------------------------------------------------------------- #
# Redis access (via kubectl exec ... redis-cli, pipelined over stdin)
# --------------------------------------------------------------------------- #
class Redis:
    def __init__(self, namespace: str, kubectl: str, selector: str,
                 pod: Optional[str], cli_cmd: Optional[str]):
        self.namespace = namespace
        self.kubectl = kubectl
        self.selector = selector
        self._pod = pod
        # Advanced override: a full shell command that runs redis-cli reading
        # commands from stdin (e.g. "redis-cli -h 1.2.3.4 -p 30079").
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
            sys.exit(
                f"[verify] could not find redis pod (ns={self.namespace}, "
                f"selector={self.selector}): {out.stderr.strip()}"
            )
        self._pod = pod
        print(f"[verify] redis pod: {pod} (ns={self.namespace})")
        return pod

    def _base_cmd(self) -> List[str]:
        if self._cli_cmd:
            # Split a plain command string; runs directly (no kubectl).
            import shlex
            return shlex.split(self._cli_cmd)
        pod = self._detect_pod()
        return [self.kubectl, "-n", self.namespace, "exec", "-i", pod, "--", "redis-cli"]

    def run_pipe(self, commands: List[str]) -> List[str]:
        """Send many commands over one redis-cli stdin; return one reply line each.

        Only used with integer-reply commands (EXISTS / HEXISTS) so every reply
        is exactly one non-empty line and positional mapping is unambiguous.
        """
        if not commands:
            return []
        payload = "\n".join(commands) + "\n"
        try:
            proc = subprocess.run(
                self._base_cmd(), input=payload, capture_output=True, text=True,
            )
        except FileNotFoundError as e:
            sys.exit(f"[verify] failed to run redis client: {e}")
        if proc.returncode != 0:
            # Pod may have been recreated; drop cached pod and retry once.
            if not self._cli_cmd and self._pod:
                self._pod = None
                proc = subprocess.run(
                    self._base_cmd(), input=payload, capture_output=True, text=True,
                )
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.strip() or "redis-cli failed")
        return proc.stdout.splitlines()


# --------------------------------------------------------------------------- #
# Verification of one request record
# --------------------------------------------------------------------------- #
def verify_record(rec: dict, redis: Redis, max_blocks: int) -> Optional[dict]:
    hashes = rec.get("block_hashes")
    if not hashes:
        return None  # nothing to check (ROUTER_LOG_BLOCK_HASHES off, or empty)

    model = rec.get("model") or ""
    endpoint = rec.get("endpoint_id") or ""
    if not model or not endpoint:
        return None

    if max_blocks and max_blocks > 0:
        hashes = hashes[:max_blocks]

    # Interleave EXISTS + HEXISTS so we get both facts per block in one round trip.
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

    present = 0
    owned = 0
    redis_prefix = 0
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
        "req_id": rec.get("req_id", "")[:12],
        "endpoint": endpoint,
        "model": model,
        "n": len(hashes),
        "present": present,
        "owned": owned,
        "redis_prefix": redis_prefix,
        "router_kv_hits": rec.get("kv_hits_len"),
        "router_total": rec.get("total_blocks"),
    }


def format_result(r: dict) -> Tuple[str, str]:
    """Return (line, status) where status is 'match' or 'missing'.

    The headline verdict is about hash IDENTITY: are the router-computed block
    hashes actually present in Redis? (present == n). The router_kv_hits vs
    redis_prefix figures are shown as secondary info -- they can legitimately
    differ (kv_hits is measured at dispatch, before this request's own blocks
    are emitted, and is also affected by the router's MODEL_NAME prefix).
    """
    n = r["n"]
    present = r["present"]

    def pct(x):
        return f"{x}/{n}" + (f" {100*x//n}%" if n else "")

    if present == n:
        status = "match"
        verdict = "HASHES MATCH redis"
    else:
        status = "missing"
        verdict = f"MISSING {n - present}/{n} not in redis"

    kv = r["router_kv_hits"]
    line = (
        f"[{r['req_id']}] ep={r['endpoint']} computed={n} "
        f"present={pct(present)} owned_by_ep={pct(r['owned'])} "
        f"-> {verdict}  (router_kv_hits={kv} vs redis_prefix={r['redis_prefix']})"
    )
    return line, status


# --------------------------------------------------------------------------- #
# Polling the router /latency_log directly (no collector needed)
# --------------------------------------------------------------------------- #
def _normalize_router_entry(e: dict) -> dict:
    """Map raw /latency_log field names onto the logs.json names verify uses."""
    e = dict(e)
    e.setdefault("req_id", e.get("rid"))
    e.setdefault("endpoint_id", e.get("endpoint"))
    return e


def iter_router_records(router_url: str, last: int, follow: bool,
                        poll_interval: float, from_start: bool) -> Iterator[dict]:
    """Poll {router}/latency_log?last=N and yield each new request once (by rid)."""
    url = f"{router_url.rstrip('/')}/latency_log?last={last}"
    seen: Set[str] = set()
    primed = from_start  # if not from_start, swallow the first batch as "already seen"
    while True:
        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, json.JSONDecodeError, TimeoutError) as ex:
            print(f"[verify] router poll failed: {ex}", file=sys.stderr)
            if not follow:
                return
            time.sleep(poll_interval)
            continue

        entries = data if isinstance(data, list) else [data]
        for e in entries:
            rid = e.get("rid") or e.get("req_id")
            if not rid or rid in seen:
                continue
            seen.add(rid)
            if not primed:
                continue  # backlog present at startup; skip until only-new
            yield _normalize_router_entry(e)
        primed = True

        # Keep the seen-set from growing without bound over long sessions.
        if len(seen) > 20000:
            seen = set(list(seen)[-10000:])

        if not follow:
            return
        time.sleep(poll_interval)


# --------------------------------------------------------------------------- #
# Tailing the NDJSON log
# --------------------------------------------------------------------------- #
def iter_records(path: Path, follow: bool, poll_interval: float, from_start: bool):
    """Yield parsed JSON records from an NDJSON file, optionally following it."""
    pos = 0 if from_start else path.stat().st_size
    buf = ""
    while True:
        try:
            size = path.stat().st_size
        except FileNotFoundError:
            if not follow:
                return
            time.sleep(poll_interval)
            continue
        if size < pos:  # truncated / rotated
            pos = 0
            buf = ""
        with path.open("r", encoding="utf-8", errors="replace") as fh:
            fh.seek(pos)
            chunk = fh.read()
            pos = fh.tell()
        buf += chunk
        lines = buf.split("\n")
        buf = lines.pop()  # keep trailing partial line for next round
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue
        if not follow:
            return
        time.sleep(poll_interval)


# --------------------------------------------------------------------------- #
def main():
    ap = argparse.ArgumentParser(
        description="Confirm Redis stores the same KV block hashes the router computed."
    )
    src = ap.add_argument_group("request source (pick one)")
    src.add_argument("--router-url",
                     help="Poll the router /latency_log directly (no collector "
                          "needed), e.g. http://192.168.0.79:30080")
    src.add_argument("--router-last", type=int, default=100,
                     help="How many recent entries to fetch per router poll (default: 100).")
    src.add_argument("--logs", help="Path to a specific logs.json (NDJSON).")
    src.add_argument("--experiments-root",
                     help="Root dir; the newest <N>/logs.json is used. "
                          f"Default search: {DEFAULT_ROOT_CANDIDATES}")

    rds = ap.add_argument_group("redis access")
    rds.add_argument("-n", "--namespace", default="vllm", help="K8s namespace (default: vllm)")
    rds.add_argument("--kubectl", default="kubectl", help="kubectl binary (default: kubectl)")
    rds.add_argument("--redis-selector", default="app=redis",
                     help="Label selector for the redis pod (default: app=redis)")
    rds.add_argument("--redis-pod", default=None, help="Redis pod name (skip auto-detect)")
    rds.add_argument("--redis-cli-cmd", default=None,
                     help="Advanced: full redis-cli command reading stdin "
                          "(e.g. 'redis-cli -h 192.168.0.79 -p 30079'); "
                          "bypasses kubectl exec.")

    beh = ap.add_argument_group("behavior")
    beh.add_argument("--once", action="store_true",
                     help="Process records already in the file, then exit (no follow).")
    beh.add_argument("--from-start", action="store_true",
                     help="When following, also process records already in the file "
                          "(default: only new ones).")
    beh.add_argument("--poll-interval", type=float, default=2.0,
                     help="Seconds between log polls when following (default: 2).")
    beh.add_argument("--max-blocks", type=int, default=0,
                     help="Cap blocks checked per request (0 = all).")
    args = ap.parse_args()

    redis = Redis(
        namespace=args.namespace,
        kubectl=args.kubectl,
        selector=args.redis_selector,
        pod=args.redis_pod,
        cli_cmd=args.redis_cli_cmd,
    )

    follow = not args.once
    from_start = args.once or args.from_start
    mode = "one-shot" if args.once else "follow"

    if args.router_url:
        print(f"[verify] source=router {args.router_url}/latency_log "
              f"mode={mode} from_start={from_start}")
        source = iter_router_records(
            args.router_url, args.router_last, follow, args.poll_interval, from_start)
    else:
        logs_path = resolve_logs_path(args)
        print(f"[verify] source=log {logs_path} mode={mode} from_start={from_start}")
        source = iter_records(logs_path, follow, args.poll_interval, from_start)

    if follow:
        print("[verify] watching for new requests... (send some with Claude Code; Ctrl+C to stop)")

    n_match = n_missing = n_checked = 0
    try:
        for rec in source:
            try:
                result = verify_record(rec, redis, args.max_blocks)
            except RuntimeError as e:
                print(f"[verify] redis error: {e}", file=sys.stderr)
                continue
            if result is None:
                continue
            n_checked += 1
            line, status = format_result(result)
            print(line, flush=True)
            if status == "match":
                n_match += 1
            else:
                n_missing += 1
    except KeyboardInterrupt:
        print("\n[verify] stopped.")

    print(f"[verify] summary: checked={n_checked} "
          f"hashes_match={n_match} missing={n_missing}")
    if n_missing:
        sys.exit(1)


if __name__ == "__main__":
    main()
