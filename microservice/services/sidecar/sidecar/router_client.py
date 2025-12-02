# -*- coding: utf-8 -*-
import threading
from typing import Dict, Any

import requests

from .config import get_config
from .local_queue import LocalQueue

_cfg = get_config()


class RouterPullWorker:
    """
    Event-biased pull helper for a single vLLM pod.

    Responsibilities:
      - Compute capacity from local_q.state() and global BATCH_SIZE.
      - Call router /pull when there is spare capacity.
      - Never exceed BATCH_SIZE = pending + inflight on this pod.
      - No periodic polling loop; pull() is triggered by workers
        (busy-path and idle-poke).

    NOTE: endpoint_id here is the endpoint identity used by the router/KV layer,
    i.e. the pod name (must match kv_watcher _endpoint_for_pod).
    """

    def __init__(self, local_q: LocalQueue, endpoint_id: str):
        self.local_q = local_q
        self.endpoint_id = endpoint_id  # pod identity used as 'endpoint' in /pull
        self._stop_evt = threading.Event()
        self._lock = threading.RLock()
        self._session: requests.Session | None = None

        # Initial “discovery” phase flags
        self._first_success: bool = False
        self._printed_wait_msg: bool = False

    # ---------------- lifecycle ----------------

    def start(self):
        """
        Initialize HTTP session. No background polling thread is started:
        all /pull calls are event-driven via pull_if_capacity().
        """
        if self._session is not None:
            return
        self._stop_evt.clear()
        self._session = requests.Session()
        print(f"[sidecar] RouterPullWorker ready (endpoint_id={self.endpoint_id})")

    def stop(self):
        self._stop_evt.set()
        session, self._session = self._session, None
        if session is not None:
            try:
                session.close()
            except Exception:
                pass
        print("[sidecar] RouterPullWorker stopped")

    # ---------------- core logic ----------------

    def pull_if_capacity(self) -> None:
        """
        Event-biased pull:
          - Check local pending + inflight count.
          - If below BATCH_SIZE, compute `want` and call /pull once.
          - Push any returned jobs into the local queue.

        Thread-safe and cheap to call from multiple worker threads.

        Logging behavior:
          - Before the first successful /pull:
              * do NOT print raw connection errors
              * print at most one:
                  "[sidecar] waiting for first successful /pull from router-service ..."
          - After the first successful /pull:
              * print detailed errors as before
        """
        if self._stop_evt.is_set():
            return

        with self._lock:
            pending, inflight = self.local_q.state()
            batch_size = _cfg.BATCH_SIZE
            total_reserved = pending + inflight

            if total_reserved >= batch_size:
                return

            want = batch_size - total_reserved
            if want <= 0:
                return

            session = self._session
            if session is None:
                # fallback session if start() has not run
                session = requests.Session()
                tmp_session = True
            else:
                tmp_session = False

            try:
                resp = session.post(
                    f"{_cfg.ROUTER_URL}/pull",
                    json={"endpoint": self.endpoint_id, "want": want},
                    timeout=1.0,
                )
                if not resp.ok:
                    # Error response from router
                    if not self._first_success:
                        # Initial phase: only print a single friendly line
                        if not self._printed_wait_msg:
                            print(
                                "[sidecar] waiting for first successful /pull "
                                "from router-service ..."
                            )
                            self._printed_wait_msg = True
                    else:
                        # After first success, show detailed error
                        print(f"[sidecar] /pull failed: {resp.status_code} {resp.text}")
                    return

                data = resp.json()
                items = data.get("items", [])

                # Mark router as alive on first-ever successful /pull (any 200)
                if not self._first_success:
                    print(
                        "[sidecar] first successful /pull from router-service; "
                        "switching to normal logging"
                    )
                    self._first_success = True

                if not items:
                    return

                for item in items:
                    rid = int(item["req_id"])
                    prompt = str(item["prompt"])
                    meta: Dict[str, Any] = item.get("meta") or {}
                    self.local_q.put(rid, prompt, meta)

            except Exception as e:
                # Network / connection-level errors
                if not self._first_success:
                    # Initial phase: just say we're waiting, once
                    if not self._printed_wait_msg:
                        print(
                            "[sidecar] waiting for first successful /pull "
                            "from router-service ..."
                        )
                        self._printed_wait_msg = True
                else:
                    # After first success, show detailed error
                    print(f"[sidecar] /pull error: {e}")
            finally:
                if tmp_session:
                    try:
                        session.close()
                    except Exception:
                        pass
