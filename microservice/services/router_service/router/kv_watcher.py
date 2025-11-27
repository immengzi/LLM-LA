# -*- coding: utf-8 -*-
# kv_watcher.py
"""
Background KV watcher:
- Periodically scans Redis kvblock keys.
- Maps pod_name -> pod_ip -> router endpoint URL.
- Calls register_block_owners(block_hash, owners).

Verbosity controlled by config.KV_LOG_KEYS:
  - "off"     : only start/stop + errors
  - "summary" : discovery + scan summary
  - "full"    : per-key logs + summary
"""

import os
import time
import threading
from typing import Dict, Optional

import asyncio
import redis.asyncio as aioredis
from kubernetes import client as k8s_client, config as k8s_config

from .config import get_config
from .kv_aware import register_block_owners

_cfg = get_config()
_KV_LOG_MODE = str(getattr(_cfg, "KV_LOG_KEYS", "summary")).lower()


def _log(msg: str, *, level: str = "summary") -> None:
    if level == "always":
        print(f"[KVWatcher] {msg}")
        return

    if _KV_LOG_MODE == "off":
        return

    if level == "summary":
        print(f"[KVWatcher] {msg}")
        return

    if level == "full" and _KV_LOG_MODE == "full":
        print(f"[KVWatcher] {msg}")


def _discover_pods() -> Dict[str, str]:
    running_in_cluster = os.getenv("KUBERNETES_SERVICE_HOST") is not None

    try:
        if running_in_cluster:
            k8s_config.load_incluster_config()
        else:
            k8s_config.load_kube_config()
    except Exception as e:
        _log(f"Failed to load K8s config: {e}", level="always")
        return {}

    v1 = k8s_client.CoreV1Api()

    try:
        pods = v1.list_namespaced_pod(
            namespace=_cfg.NAMESPACE,
            label_selector=_cfg.LABEL_SELECTOR,
        ).items
    except Exception as e:
        _log(f"list_namespaced_pod failed: {e}", level="always")
        return {}

    out: Dict[str, str] = {}
    for pod in pods:
        if pod.status.phase == "Running" and pod.status.pod_ip:
            out[pod.metadata.name] = pod.status.pod_ip
    return out


def _endpoint_for_pod(pod_name: str, pods: Dict[str, str]) -> Optional[str]:
    # We only care about identity here; HTTP URL is only used in sidecar.
    # So we just use pod_name as the endpoint key.
    return pod_name if pod_name in pods else None



class KVWatcher:
    def __init__(self):
        self.redis_url = f"redis://{_cfg.REDIS_HOST}:{_cfg.REDIS_PORT}"
        self.model_name = _cfg.MODEL_NAME
        self.interval_s = float(_cfg.KV_WATCH_INTERVAL_S)
        self.max_keys = int(_cfg.KV_WATCH_MAX_KEYS)
        self.discovery_interval_s = float(_cfg.KV_DISCOVERY_INTERVAL_S)

        self._stop_evt = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self):
        if self._thread is not None:
            return
        self._stop_evt.clear()
        self._thread = threading.Thread(
            target=self._thread_main,
            daemon=True,
        )
        self._thread.start()
        _log(
            f"Started (redis={self.redis_url}, model={self.model_name}, "
            f"interval_s={self.interval_s}, max_keys={self.max_keys}, "
            f"log_mode={_KV_LOG_MODE})",
            level="always",
        )

    def stop(self):
        self._stop_evt.set()
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None
        _log("Stopped", level="always")

    def _thread_main(self):
        try:
            asyncio.run(self._run_async())
        except Exception as e:
            _log(f"async loop crashed: {e}", level="always")

    async def _run_async(self):
        redis = aioredis.from_url(self.redis_url, decode_responses=True)
        pods: Dict[str, str] = {}
        last_discovery = 0.0

        try:
            while not self._stop_evt.is_set():
                now = time.time()

                if now - last_discovery >= self.discovery_interval_s:
                    pods = _discover_pods()
                    last_discovery = now
                    _log(f"discovered {len(pods)} pods", level="summary")

                if pods:
                    await self._scan_once(redis, pods)

                await asyncio.sleep(self.interval_s)

        finally:
            try:
                await redis.close()
            except Exception:
                pass

    async def _scan_once(self, redis, pods: Dict[str, str]):
        pattern = f"{self.model_name}:kvblock:*"
        seen = 0

        try:
            async for key in redis.scan_iter(match=pattern, count=100):
                mapping = await redis.hgetall(key)
                if not mapping:
                    continue

                try:
                    block_hash = int(key.rsplit(":", 1)[1])
                except Exception:
                    continue

                pod_owners = list(mapping.keys())
                ep_owners = []
                for pod_name in pod_owners:
                    ep = _endpoint_for_pod(pod_name, pods)
                    if ep:
                        ep_owners.append(ep)

                if ep_owners:
                    register_block_owners(block_hash, ep_owners)

                _log(f"key={key} pods={pod_owners} eps={ep_owners}", level="full")

                seen += 1
                if seen >= self.max_keys:
                    break

            if seen > 0:
                _log(
                    f"scan complete: scanned={seen} keys",
                    level="summary",
                )
            else:
                _log("scan complete: no kvblock keys found", level="summary")

        except Exception as e:
            _log(f"scan failed: {e}", level="always")
