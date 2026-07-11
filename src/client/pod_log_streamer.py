#!/usr/bin/env python3
"""
pod_log_streamer.py
~~~~~~~~~~~~~~~~~~~
Managed, in-process version of ``container-logs.sh``.

Streams the logs of every container in a Kubernetes namespace (vLLM, router,
sidecar, ...) into a per-experiment directory:

    <out_dir>/<pod>/<container>.log

One ``kubectl logs -f`` subprocess is spawned per container. A background
watcher thread restarts dead streams and picks up pods that appear after the
run has started (rolling restarts, autoscaling). Everything is best-effort:
if ``kubectl`` is missing or a stream cannot be started, the run continues.

Typical use (matches the other collectors in main.py):

    streamer = PodLogStreamer(out_dir=exp_dir / "vllm-logs", namespace="vllm")
    streamer.start()
    ...                       # run the load
    streamer.stop()           # terminates subprocesses, closes files
"""

from __future__ import annotations

import shutil
import subprocess
import threading
from pathlib import Path
from typing import Dict, List, Optional, Tuple


class PodLogStreamer:
    def __init__(
        self,
        out_dir,
        namespace: str = "vllm",
        tail: int = 1000,
        restart_tail: int = 10,
        restart_interval_s: float = 30.0,
        kubectl: str = "kubectl",
    ) -> None:
        self.out_dir = Path(out_dir)
        self.namespace = namespace
        self.tail = int(tail)
        # On restart we only re-pull a few lines to minimise duplication.
        self.restart_tail = int(restart_tail)
        self.restart_interval_s = float(restart_interval_s)
        self.kubectl = kubectl

        self._procs: Dict[str, subprocess.Popen] = {}
        self._fhs: Dict[str, object] = {}
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._started = False

    # -- discovery -------------------------------------------------------

    def _kubectl_available(self) -> bool:
        return shutil.which(self.kubectl) is not None

    def _list_pod_containers(self) -> List[Tuple[str, str]]:
        """Return [(pod, container), ...] for the namespace (best-effort)."""
        pairs: List[Tuple[str, str]] = []
        try:
            pods = subprocess.run(
                [self.kubectl, "get", "pods", "-n", self.namespace,
                 "-o", "jsonpath={.items[*].metadata.name}"],
                capture_output=True, text=True, timeout=30,
            )
            pod_names = pods.stdout.split()
            for pod in pod_names:
                cont = subprocess.run(
                    [self.kubectl, "get", "pod", pod, "-n", self.namespace,
                     "-o", "jsonpath={.spec.containers[*].name}"],
                    capture_output=True, text=True, timeout=30,
                )
                for c in cont.stdout.split():
                    pairs.append((pod, c))
        except Exception as e:  # noqa: BLE001 - best-effort
            print(f"[pod-logs] WARN: listing pods failed: {e}")
        return pairs

    # -- streams ---------------------------------------------------------

    def _start_stream(self, pod: str, container: str, tail: int) -> None:
        key = f"{pod}__{container}"
        existing = self._procs.get(key)
        if existing is not None and existing.poll() is None:
            return  # still alive

        pod_dir = self.out_dir / pod
        pod_dir.mkdir(parents=True, exist_ok=True)
        logfile = pod_dir / f"{container}.log"
        try:
            fh = open(logfile, "ab")  # append across restarts
        except Exception as e:  # noqa: BLE001
            print(f"[pod-logs] WARN: cannot open {logfile}: {e}")
            return

        try:
            proc = subprocess.Popen(
                [self.kubectl, "logs", "-f", "-n", self.namespace,
                 pod, "-c", container, f"--tail={tail}"],
                stdout=fh, stderr=subprocess.STDOUT,
            )
        except Exception as e:  # noqa: BLE001
            print(f"[pod-logs] WARN: stream start {key}: {e}")
            fh.close()
            return

        # Close the previously-owned handle (if the old proc died) before
        # replacing bookkeeping entries.
        old_fh = self._fhs.get(key)
        if old_fh is not None and old_fh is not fh:
            try:
                old_fh.close()
            except Exception:
                pass
        self._procs[key] = proc
        self._fhs[key] = fh

    # -- lifecycle -------------------------------------------------------

    def start(self) -> bool:
        if self._started:
            return True
        if not self._kubectl_available():
            print("[pod-logs] kubectl not found on PATH; skipping container log capture.")
            return False

        self.out_dir.mkdir(parents=True, exist_ok=True)
        with self._lock:
            for pod, container in self._list_pod_containers():
                self._start_stream(pod, container, self.tail)
            count = len(self._procs)

        if count == 0:
            print(f"[pod-logs] no containers found in namespace '{self.namespace}'.")

        self._started = True
        self._thread = threading.Thread(
            target=self._watch, name="pod-log-streamer", daemon=True
        )
        self._thread.start()
        print(f"[pod-logs] streaming {count} container(s) -> {self.out_dir}")
        return True

    def _watch(self) -> None:
        while not self._stop.wait(self.restart_interval_s):
            with self._lock:
                for pod, container in self._list_pod_containers():
                    key = f"{pod}__{container}"
                    proc = self._procs.get(key)
                    if proc is None:
                        # New pod/container appeared mid-run.
                        self._start_stream(pod, container, self.tail)
                    elif proc.poll() is not None:
                        # Stream died (pod restart, transient error) -> resume.
                        self._start_stream(pod, container, self.restart_tail)

    def stop(self) -> None:
        if not self._started:
            return
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

        with self._lock:
            for proc in self._procs.values():
                try:
                    proc.terminate()
                except Exception:
                    pass
            for proc in self._procs.values():
                try:
                    proc.wait(timeout=5)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass
            for fh in self._fhs.values():
                try:
                    fh.close()
                except Exception:
                    pass
            self._procs.clear()
            self._fhs.clear()

        self._started = False
        print("[pod-logs] stopped.")
