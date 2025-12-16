# -*- coding: utf-8 -*-
import signal
import threading
import time

import uvicorn

from .config import get_config
from .local_queue import LocalQueue
from .router_client import RouterPullWorker
from .vllm_client import VLLMWorker
from .zmq_subscriber import KVSubscriber
from .api import app, bind_local_queue

_cfg = get_config()


def _run_server():
    uvicorn.run(
        "sidecar.api:app",
        host="0.0.0.0",
        port=_cfg.SIDECAR_PORT,
        log_level="info",
    )


def main():
        local_q = LocalQueue()
        bind_local_queue(local_q)

        mode = (_cfg.SIDECAR_MODE or "pull").lower()

        # For KV-aware routing: endpoint identity = pod name (matches KVWatcher register_block_owners)
        endpoint_id = _cfg.CONTAINER_NAME

        pull_worker = None
        if mode == "pull":
            pull_worker = RouterPullWorker(local_q, endpoint_id)
            pull_worker.start()  # initializes session; this is not a polling thread

        # Concurrency: N workers = N concurrent vLLM requests per pod
        vllm_workers = []
        for _ in range(_cfg.BATCH_SIZE):
            w = VLLMWorker(local_q, pull_worker=pull_worker)
            w.start()
            vllm_workers.append(w)

        kv_sub = KVSubscriber()

        stop_evt = threading.Event()

        def handle_sig(*_args):
            stop_evt.set()

        signal.signal(signal.SIGINT, handle_sig)
        signal.signal(signal.SIGTERM, handle_sig)

        # Start KV subscriber
        kv_sub.start()

        # Start HTTP server in a background thread
        server_thread = threading.Thread(target=_run_server, daemon=True)
        server_thread.start()

        print(
            f"[sidecar] running in {mode.upper()} mode "
            f"(BATCH_SIZE={_cfg.BATCH_SIZE}, port={_cfg.SIDECAR_PORT}, endpoint_id={endpoint_id})"
        )

        try:
            while not stop_evt.is_set():
                time.sleep(0.5)
        finally:
            print("[sidecar] shutting down")

            if pull_worker:
                pull_worker.stop()

            for w in vllm_workers:
                w.stop()

            kv_sub.stop()


if __name__ == "__main__":
    main()
