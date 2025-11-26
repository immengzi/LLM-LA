# -*- coding: utf-8 -*-
import signal
import threading
import time

from .config import get_config
from .local_queue import LocalQueue
from .router_client import RouterPullWorker
from .vllm_client import VLLMWorker
from .zmq_subscriber import KVSubscriber

_cfg = get_config()


def main():
    local_q = LocalQueue()

    # vLLM endpoint that the router should see as "owner" of blocks
    endpoint_url = f"{_cfg.VLLM_URL}"

    pull_worker = RouterPullWorker(local_q, endpoint_url)
    vllm_worker = VLLMWorker(local_q)
    kv_sub = KVSubscriber()

    stop_evt = threading.Event()

    def handle_sig(*_args):
        stop_evt.set()

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    kv_sub.start()
    pull_worker.start()
    vllm_worker.start()

    print("[sidecar] running")
    try:
        while not stop_evt.is_set():
            time.sleep(0.5)
    finally:
        print("[sidecar] shutting down")
        pull_worker.stop()
        vllm_worker.stop()
        kv_sub.stop()


if __name__ == "__main__":
    main()
