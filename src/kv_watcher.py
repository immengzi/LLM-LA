# -*- coding: utf-8 -*-
# kv_watcher.py
"""
Background KV watcher:
- Periodically scans Redis kvblock keys.
- Maps pod_name -> pod_ip -> router endpoint URL.
- Calls notify_kv_update(endpoint_url).
"""

import os
import time
import threading
from typing import Dict, Optional

import asyncio
import redis.asyncio as aioredis
from kubernetes import client as k8s_client, config as k8s_config

from config import get_config
from kv_aware import notify_kv_update, register_block_owners

_cfg = get_config()


# ---------------------------------------------------------------------------
# Kubernetes discovery helpers
# ---------------------------------------------------------------------------

def _discover_pods() -> Dict[str, str]:
    """Return {pod_name: pod_ip} for running vLLM pods."""

    running_in_cluster = os.getenv("KUBERNETES_SERVICE_HOST") is not None

    try:
        if running_in_cluster:
            k8s_config.load_incluster_config()
        else:
            k8s_config.load_kube_config()
    except Exception as e:
        print(f"[KVWatcher] Failed to load K8s config: {e}")
        return {}

    v1 = k8s_client.CoreV1Api()

    try:
        pods = v1.list_namespaced_pod(
            namespace=_cfg.NAMESPACE,
            label_selector=_cfg.LABEL_SELECTOR,
        ).items
    except Exception as e:
        print(f"[KVWatcher] list_namespaced_pod failed: {e}")
        return {}

    out: Dict[str, str] = {}
    for pod in pods:
        if pod.status.phase == "Running" and pod.status.pod_ip:
            out[pod.metadata.name] = pod.status.pod_ip
    return out


def _endpoint_for_pod(pod_name: str, pods: Dict[str, str]) -> Optional[str]:
    ip = pods.get(pod_name)
    if not ip:
        return None
    return f"http://{ip}:{_cfg.VLLM_PORT}"


# ---------------------------------------------------------------------------
# KV watcher
# ---------------------------------------------------------------------------

class KVWatcher:
    def __init__(self):
        self.redis_url = f"redis://{_cfg.REDIS_HOST}:{_cfg.REDIS_PORT}"
        self.model_name = _cfg.MODEL_NAME

        self.interval_s = float(_cfg.KV_WATCH_INTERVAL_S)
        self.max_keys = int(_cfg.KV_WATCH_MAX_KEYS)
        self.discovery_interval_s = float(_cfg.KV_DISCOVERY_INTERVAL_S)

        self._stop_evt = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # -------------------------------------------------------

    def start(self):
        if self._thread is not None:
            return
        self._stop_evt.clear()
        self._thread = threading.Thread(
            target=self._thread_main,
            daemon=True,
        )
        self._thread.start()
        print(f"[KVWatcher] Started (redis={self.redis_url}, model={self.model_name})")

    def stop(self):
        self._stop_evt.set()
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None
        print("[KVWatcher] Stopped")

    # -------------------------------------------------------

    def _thread_main(self):
        try:
            asyncio.run(self._run_async())
        except Exception as e:
            print(f"[KVWatcher] async loop crashed: {e}")

    async def _run_async(self):
        redis = aioredis.from_url(self.redis_url, decode_responses=True)
        pods: Dict[str, str] = {}
        last_discovery = 0.0

        try:
            while not self._stop_evt.is_set():
                now = time.time()

                # Refresh pod set
                if now - last_discovery >= self.discovery_interval_s:
                    pods = _discover_pods()
                    last_discovery = now
                    print(f"[KVWatcher] discovered {len(pods)} pods")

                # Scan Redis only if we know endpoints
                if pods:
                    await self._scan_once(redis, pods)

                await asyncio.sleep(self.interval_s)

        finally:
            try:
                await redis.close()
            except Exception:
                pass

    async def _scan_once(self, redis, pods: Dict[str, str]):
        """
        Scan kvblock:* and update:
          • register_block_owners(block_hash, owners)
          • notify_kv_update(endpoint)

        Includes debug print of all keys seen.
        """
        pattern = f"{self.model_name}:kvblock:*"
        seen = 0
        touched_eps = set()

        try:
            async for key in redis.scan_iter(match=pattern, count=100):
                mapping = await redis.hgetall(key)
                if not mapping:
                    continue

                # key format: served-model:kvblock:<hash>
                try:
                    block_hash = int(key.rsplit(":", 1)[1])
                except Exception:
                    continue

                owners = list(mapping.keys())
                register_block_owners(block_hash, owners)

                print(f"[KVWatcher] key={key} pods={owners}")

                for pod_name in owners:
                    ep = _endpoint_for_pod(pod_name, pods)
                    if ep:
                        notify_kv_update(ep)
                        touched_eps.add(ep)

                seen += 1
                if seen >= self.max_keys:
                    break

            if seen > 0:
                print(
                    f"[KVWatcher] scan complete: scanned={seen} keys, "
                    f"updated_endpoints={len(touched_eps)}"
                )
            else:
                print("[KVWatcher] scan complete: no kvblock keys found")

        except Exception as e:
            print(f"[KVWatcher] scan failed: {e}")
