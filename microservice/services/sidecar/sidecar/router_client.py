# -*- coding: utf-8 -*-
import time
import threading
from typing import Dict, Any

import requests

from .config import get_config
from .local_queue import LocalQueue

_cfg = get_config()


class RouterPullWorker:
    """
    Periodically:
      1) compute 'want' from local inflight vs VLLM_CONCURRENCY
      2) call router /pull
      3) push jobs into local queue

    NOTE: endpoint_id here is the endpoint *identity* used by the router/KV layer,
    i.e. the pod name (must match kv_watcher _endpoint_for_pod).
    """

    def __init__(self, local_q: LocalQueue, endpoint_id: str):
        self.local_q = local_q
        self.endpoint_id = endpoint_id  # pod identity used as 'endpoint' in /pull
        self._stop_evt = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self):
        if self._thread is not None:
            return
        self._stop_evt.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        print(f"[sidecar] RouterPullWorker started (endpoint_id={self.endpoint_id})")

    def stop(self):
        self._stop_evt.set()
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None
        print("[sidecar] RouterPullWorker stopped")

    def _loop(self):
        session = requests.Session()
        max_inflight = _cfg.VLLM_CONCURRENCY

        while not self._stop_evt.is_set():
            pending, inflight = self.local_q.state()

            # Cap purely on in-flight requests; pending is just backlog.
            if inflight < max_inflight:
                want = max_inflight - inflight
            else:
                want = 0

            if want > 0:
                try:
                    resp = session.post(
                        f"{_cfg.ROUTER_URL}/pull",
                        json={"endpoint": self.endpoint_id, "want": want},
                        timeout=1.0,
                    )
                    if resp.ok:
                        data = resp.json()
                        items = data.get("items", [])
                        for item in items:
                            rid = int(item["req_id"])
                            prompt = str(item["prompt"])
                            meta: Dict[str, Any] = item.get("meta") or {}
                            self.local_q.put(rid, prompt, meta)
                    else:
                        print(f"[sidecar] /pull failed: {resp.status_code} {resp.text}")
                except Exception as e:
                    print(f"[sidecar] /pull error: {e}")

            time.sleep(_cfg.PULL_INTERVAL_S)
        session.close()
