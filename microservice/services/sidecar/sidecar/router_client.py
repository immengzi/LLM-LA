# sidecar/router_client.py
# -*- coding: utf-8 -*-
import threading
import time
from typing import Dict, Any

import requests
from requests.adapters import HTTPAdapter

from .config import get_config
from .local_queue import LocalQueue
from .metrics import inc_received

_cfg = get_config()


def _make_pooled_session(pool_connections: int, pool_maxsize: int) -> requests.Session:
    """
    Create a requests.Session with a larger urllib3 connection pool.
    This reduces TCP churn and makes connection reuse much more reliable.
    """
    s = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=int(pool_connections),
        pool_maxsize=int(pool_maxsize),
        max_retries=0,
        pool_block=False,
    )
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


class RouterPullWorker:
    """
    Event-biased pull helper for a single vLLM pod.

    Responsibilities:
      - Compute capacity from local_q.state() and global BATCH_SIZE.
      - Call router /pull when there is spare capacity.
      - Never exceed BATCH_SIZE = pending + inflight on this pod.
      - No background polling; pull() is triggered by workers
        (busy-path and idle-poke).

    NOTE: endpoint_id must match what the router sees as the endpoint identity.
    """

    def __init__(self, local_q: LocalQueue, endpoint_id: str):
        self.local_q = local_q
        self.endpoint_id = endpoint_id  # sidecar identity used by router
        self._stop_evt = threading.Event()
        self._lock = threading.RLock()
        self._session: requests.Session | None = None

        # Initial “discovery” flags
        self._first_success: bool = False
        self._printed_wait_msg: bool = False

    # ---------------- lifecycle ----------------

    def start(self):
        """
        Initialize HTTP session. No polling thread, all pulls are event-driven.
        """
        if self._session is not None:
            return
        self._stop_evt.clear()

        # pooled session (no semantic change)
        self._session = _make_pooled_session(
            pool_connections=_cfg.ROUTER_POOL_CONNECTIONS,
            pool_maxsize=_cfg.ROUTER_POOL_MAXSIZE,
        )

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
          - If pending+inflight < BATCH_SIZE, compute want and /pull.
          - Insert returned jobs into the local queue.

        When TRACE_ENABLED=true, also stamps:
          - t_arrive_sidecar_pull
          - sidecar_queue_len_before_pull
          - sidecar_inflight_before_pull
          - sidecar_logical_before_pull
          - sidecar_queue_len_after_pull
          - sidecar_logical_after_pull
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
            tmp_session = False
            if session is None:
                session = _make_pooled_session(
                    pool_connections=_cfg.ROUTER_POOL_CONNECTIONS,
                    pool_maxsize=_cfg.ROUTER_POOL_MAXSIZE,
                )
                tmp_session = True

            try:
                # ---------------------
                # ROUTER /pull request
                # ---------------------
                resp = session.post(
                    f"{_cfg.ROUTER_URL}/pull",
                    json={"endpoint": self.endpoint_id, "want": want, "model": _cfg.MODEL_NAME},
                    timeout=_cfg.ROUTER_PULL_TIMEOUT_S,
                )

                if not resp.ok:
                    # Before first success: suppress raw spam
                    if not self._first_success:
                        if not self._printed_wait_msg:
                            print(
                                "[sidecar] waiting for first successful /pull "
                                "from router-service ..."
                            )
                            self._printed_wait_msg = True
                    else:
                        print(f"[sidecar] /pull failed: {resp.status_code} {resp.text}")
                    return

                data = resp.json()
                items = data.get("items", [])

                if not self._first_success:
                    print("[sidecar] first successful /pull; normal logging enabled.")
                    self._first_success = True

                if _cfg.LOG_LEVEL == "debug" and items:
                    print(f"[sidecar] /pull want={want} got={len(items)} pending={pending} inflight={inflight}")

                if not items:
                    return

                # ---------------------
                # Process returned jobs
                # ---------------------
                now_pull = time.time()

                # Snapshot queue state *before* enqueueing pulled items
                pending_before, inflight_before = self.local_q.state()
                logical_before = pending_before + inflight_before
                queue_len_after = pending_before + len(items)
                logical_after = logical_before + len(items)

                for item in items:
                    rid = str(item["req_id"])
                    prompt = str(item["prompt"])
                    meta: Dict[str, Any] = item.get("meta") or {}

                    # Prom: received (router -> sidecar) for each pulled item
                    inc_received(self.endpoint_id)

                    # Trace injection for pull arrival + queue lengths
                    if getattr(_cfg, "TRACE_ENABLED", False):
                        tr = dict(meta.get("__trace__") or {})
                        tr["t_arrive_sidecar_pull"] = now_pull

                        # Sidecar-local queue lengths at pull time
                        tr["sidecar_queue_len_before_pull"] = pending_before
                        tr["sidecar_inflight_before_pull"] = inflight_before
                        tr["sidecar_logical_before_pull"] = logical_before
                        tr["sidecar_queue_len_after_pull"] = queue_len_after
                        tr["sidecar_logical_after_pull"] = logical_after

                        meta["__trace__"] = tr

                    # Push into local queue
                    self.local_q.put(rid, prompt, meta)

            except Exception as e:
                if not self._first_success:
                    if not self._printed_wait_msg:
                        print(
                            "[sidecar] waiting for first successful /pull "
                            "from router-service ..."
                        )
                        self._printed_wait_msg = True
                else:
                    print(f"[sidecar] /pull error: {e}")

            finally:
                if tmp_session:
                    try:
                        session.close()
                    except Exception:
                        pass
