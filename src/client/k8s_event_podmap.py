# k8s_event_podmap.py
#
# Event-driven pod->node snapshot logger for a namespace.
# - Watches `kubectl get events --watch -o json`
# - When relevant scaling/pod lifecycle events occur, debounce and write ONE snapshot
# - Writes JSONL file: one entry for "initial", then "update" entries on changes.

from __future__ import annotations

import json
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _run(cmd: List[str], timeout_s: float) -> str:
    out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, timeout=timeout_s)
    return out.decode("utf-8", "replace")


def _get_podmap_snapshot(namespace: str, kubectl: str, timeout_s: float) -> Dict[str, Any]:
    """
    Snapshot of pod placements + container info.
    """
    raw = _run([kubectl, "-n", namespace, "get", "pods", "-o", "json"], timeout_s=timeout_s)
    obj = json.loads(raw)

    pods: List[Dict[str, Any]] = []
    for it in obj.get("items", []):
        meta = it.get("metadata", {}) or {}
        spec = it.get("spec", {}) or {}
        status = it.get("status", {}) or {}

        containers = []
        for c in (spec.get("containers", []) or []):
            containers.append({"name": c.get("name", ""), "image": c.get("image", "")})

        pods.append(
            {
                "pod": meta.get("name", ""),
                "uid": meta.get("uid", ""),
                "node": spec.get("nodeName", ""),
                "phase": status.get("phase", ""),
                "start_time": status.get("startTime", None),
                "containers": containers,
                "labels": meta.get("labels", {}) or {},
                "owner_refs": meta.get("ownerReferences", []) or [],
            }
        )

    pods.sort(key=lambda p: str(p.get("pod", "")))
    return {
        "namespace": namespace,
        "captured_at_unix": time.time(),
        "pods": pods,
    }


def _event_obj(evt: Dict[str, Any]) -> Tuple[str, str, str, str]:
    """
    Extract (kind, name, reason, message).
    """
    involved = evt.get("involvedObject", {}) or {}
    kind = str(involved.get("kind", "") or "")
    name = str(involved.get("name", "") or "")
    reason = str(evt.get("reason", "") or "")
    msg = str(evt.get("message", "") or "")
    return kind, name, reason, msg


def _is_relevant_event(
    evt: Dict[str, Any],
    *,
    deployment_name: str,
) -> bool:
    """
    Heuristic: capture anything that indicates scaling or pod lifecycle
    for vllm-qwen or its derived ReplicaSets/Pods.
    """
    kind, name, reason, msg = _event_obj(evt)
    s = f"{kind}/{name} {reason} {msg}".lower()

    # If user wants strict filtering, do it by name substring (deployment + replica sets + pods share prefix).
    if deployment_name.lower() not in s:
        # still allow Pod events whose name starts with deployment prefix (common for ReplicaSets/Pods)
        # Example pods: vllm-qwen-85c6766b-72b4j
        if not (kind.lower() == "pod" and name.startswith(deployment_name + "-")):
            return False

    # Scaling & pod lifecycle signals (covers HPA/ReplicaSet/Deployment behaviors)
    keywords = [
        "scalingreplicaset",
        "successfulcreate",
        "successfuldelete",
        "killing",
        "scheduled",
        "pulled",
        "created",
        "started",
        "deleted",
        "preempt",
        "evicted",
    ]
    if any(k in s for k in keywords):
        return True

    # Some clusters use slightly different reason/message combos; include explicit reasons too.
    if reason.lower() in {
        "scalingreplicaset",
        "successfulcreate",
        "successfuldelete",
        "killing",
        "scheduled",
        "pulled",
        "created",
        "started",
        "preempted",
        "evicted",
    }:
        return True

    return False


class EventDrivenPodMapLogger:
    """
    - Starts `kubectl get events --watch -o json` in background
    - When relevant events occur, waits for quiet_window_s since last relevant event,
      then writes ONE snapshot with batched events.
    """

    def __init__(
        self,
        *,
        out_path: Path,
        namespace: str = "vllm",
        deployment_name: str = "vllm-qwen",
        kubectl: str = "kubectl",
        quiet_window_s: float = 8.0,
        snapshot_timeout_s: float = 5.0,
    ) -> None:
        self.out_path = Path(out_path)
        self.namespace = namespace
        self.deployment_name = deployment_name
        self.kubectl = kubectl
        self.quiet_window_s = float(quiet_window_s)
        self.snapshot_timeout_s = float(snapshot_timeout_s)

        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

        self._fh = None  # type: Optional[object]
        self._lock = threading.Lock()

        self._pending_events: List[Dict[str, Any]] = []
        self._last_relevant_ts: Optional[float] = None
        self._initial_written = False

        self._proc: Optional[subprocess.Popen] = None

    def start(self) -> None:
        self.out_path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self.out_path, "a", encoding="utf-8")

        # Write initial snapshot as ONE entry.
        self._write_entry(
            reason="initial",
            events=[],
            snapshot=_get_podmap_snapshot(self.namespace, self.kubectl, self.snapshot_timeout_s),
        )
        self._initial_written = True

        self._thread = threading.Thread(target=self._run, name="event-podmap", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

        # terminate watch process
        p = self._proc
        if p is not None:
            try:
                p.terminate()
            except Exception:
                pass

        t = self._thread
        if t is not None:
            t.join(timeout=5.0)

        # flush any pending events (best-effort)
        self._flush_if_pending(force=True)

        with self._lock:
            fh = self._fh
            self._fh = None
        if fh is not None:
            try:
                fh.flush()
                fh.close()
            except Exception:
                pass

    def _write_entry(self, *, reason: str, events: List[Dict[str, Any]], snapshot: Dict[str, Any]) -> None:
        entry = {
            "ts_unix": time.time(),
            "reason": reason,  # "initial" or "update"
            "namespace": self.namespace,
            "deployment": self.deployment_name,
            "events": events,  # batched relevant events that triggered this snapshot
            "snapshot": snapshot,
        }
        with self._lock:
            fh = self._fh
            if fh is None:
                return
            try:
                fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
                fh.flush()
            except Exception:
                pass

    def _flush_if_pending(self, force: bool = False) -> None:
        if not self._pending_events:
            return
        if not force and self._last_relevant_ts is not None:
            if (time.time() - self._last_relevant_ts) < self.quiet_window_s:
                return

        # Debounce window passed: write ONE update snapshot
        try:
            snap = _get_podmap_snapshot(self.namespace, self.kubectl, self.snapshot_timeout_s)
        except Exception:
            # if snapshot fails, still clear events to avoid infinite growth
            snap = {"namespace": self.namespace, "captured_at_unix": time.time(), "pods": []}

        # Keep entries small: store a compact view of events
        compact_events = []
        for e in self._pending_events[-200:]:
            kind, name, reason, msg = _event_obj(e)
            compact_events.append(
                {
                    "ts": e.get("lastTimestamp") or e.get("eventTime") or e.get("firstTimestamp"),
                    "kind": kind,
                    "name": name,
                    "reason": reason,
                    "message": msg,
                    "type": e.get("type", None),
                }
            )

        self._write_entry(reason="update", events=compact_events, snapshot=snap)

        self._pending_events = []
        self._last_relevant_ts = None

    def _run(self) -> None:
        # Use JSON output; kubectl streams objects separated by newlines.
        cmd = [self.kubectl, "-n", self.namespace, "get", "events", "--watch", "-o", "json"]
        self._proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )

        while not self._stop.is_set():
            p = self._proc
            if p is None or p.stdout is None:
                break

            line = p.stdout.readline()
            if not line:
                # process ended; exit loop
                break

            line = line.strip()
            if not line:
                continue

            # Some kubectl versions may output multiple JSON objects; assume one per line.
            try:
                evt = json.loads(line)
            except Exception:
                continue

            if _is_relevant_event(evt, deployment_name=self.deployment_name):
                self._pending_events.append(evt)
                self._last_relevant_ts = time.time()

            # Periodically check debounce condition even if no new events arrive
            self._flush_if_pending(force=False)

        # final flush
        self._flush_if_pending(force=True)
