# sim_http_health.py
# -*- coding: utf-8 -*-
import sys, time, requests
from config import load_config
from sim_backend_http_shim import configure, endpoints


def main(config_path: str = "sim_distro", timeout_s: float = 10.0):
    configure(config_path)
    cfg = load_config(config_path)
    urls = endpoints()
    print("[SIM] endpoints:", urls)
    deadline = time.time() + timeout_s
    pending = set(urls)
    while pending and time.time() < deadline:
        ok_now = set()
        for url in list(pending):
            try:
                r = requests.get(url + cfg.HEALTH_PATH, timeout=1.0)
                if r.ok:
                    ok_now.add(url)
            except Exception:
                pass
        pending -= ok_now
        if pending:
            time.sleep(0.25)
    if pending:
        print("[SIM] FAIL: unhealthy:", sorted(pending))
        sys.exit(1)
    print("[SIM] OK: all healthy")
    sys.exit(0)


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "sim_distro"
    main(path)
