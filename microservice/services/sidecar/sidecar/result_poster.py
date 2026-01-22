# sidecar/result_poster.py
# -*- coding: utf-8 -*-
import queue
import threading
import time
import random
from typing import Dict, Any, Optional

import requests
from requests.adapters import HTTPAdapter

from .config import get_config

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
        max_retries=0,  # IMPORTANT: no urllib3 retries; we control policy ourselves
        pool_block=False,
    )
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


class ResultPoster:
    """
    Async poster for sidecar -> router /result.

    Key property: vLLM workers never block on router /result.

    Policy:
      - By default: NO RETRY (drop on failure) because you explicitly asked for that.
      - Optional: bounded retry if RESULT_POST_RETRY=true
    """

    def __init__(self, maxsize: int = 100000):
        self._q: "queue.Queue[Dict[str, Any]]" = queue.Queue(maxsize=maxsize)
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._session: Optional[requests.Session] = None

    def start(self):
        if self._thread is not None:
            return
        self._stop.clear()
        self._session = _make_pooled_session(
            pool_connections=_cfg.ROUTER_POOL_CONNECTIONS,
            pool_maxsize=_cfg.ROUTER_POOL_MAXSIZE,
        )
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        print(
            "[sidecar] ResultPoster started "
            f"(retry={_cfg.RESULT_POST_RETRY}, pool_maxsize={_cfg.ROUTER_POOL_MAXSIZE})"
        )

    def stop(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None
        if self._session is not None:
            try:
                self._session.close()
            except Exception:
                pass
            self._session = None
        print("[sidecar] ResultPoster stopped")

    def submit(self, payload: Dict[str, Any]) -> None:
        # No blocking: never stall vLLM workers.
        try:
            self._q.put_nowait(payload)
        except queue.Full:
            rid = payload.get("req_id", "?")
            print(f"[sidecar] ResultPoster queue FULL; dropping result for req_id={rid}")

    def _try_post_once(self, session: requests.Session, url: str, payload: Dict[str, Any]) -> bool:
        try:
            r = session.post(url, json=payload, timeout=_cfg.ROUTER_RESULT_TIMEOUT_S)
            return bool(r.ok)
        except Exception:
            return False

    def _loop(self):
        session = self._session or _make_pooled_session(
            pool_connections=_cfg.ROUTER_POOL_CONNECTIONS,
            pool_maxsize=_cfg.ROUTER_POOL_MAXSIZE,
        )
        url = f"{_cfg.ROUTER_URL}/result"

        while not self._stop.is_set():
            try:
                payload = self._q.get(timeout=0.5)
            except queue.Empty:
                continue

            rid = payload.get("req_id", "?")

            if not _cfg.RESULT_POST_RETRY:
                ok = self._try_post_once(session, url, payload)
                if not ok:
                    print(f"[sidecar] /result post FAILED (no-retry) req_id={rid}")
                try:
                    self._q.task_done()
                except Exception:
                    pass
                continue

            # Optional bounded retry mode
            attempt = 0
            max_tries = max(1, int(_cfg.RESULT_POST_MAX_RETRIES) + 1)  # include first attempt
            while not self._stop.is_set() and attempt < max_tries:
                attempt += 1
                ok = self._try_post_once(session, url, payload)
                if ok:
                    break

                # backoff with jitter
                base = float(_cfg.RESULT_POST_BACKOFF_BASE_S)
                cap = float(_cfg.RESULT_POST_BACKOFF_CAP_S)
                sleep_s = min(cap, base * (2 ** min(attempt, 6)))
                sleep_s *= (0.8 + 0.4 * random.random())
                time.sleep(sleep_s)

            if attempt >= max_tries and not ok:
                print(f"[sidecar] /result post FAILED after retries req_id={rid} tries={attempt}")

            try:
                self._q.task_done()
            except Exception:
                pass
