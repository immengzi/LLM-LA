import os
import time
import requests

# Prometheus URL
PROMETHEUS_URL = os.getenv(
    "PROMETHEUS_URL", "http://localhost:31190"  # NodePort for outside-cluster
)

# Replace Grafana vars with actual values
MODEL_NAME = os.getenv("MODEL_NAME", "served-model")
RATE_INTERVAL = os.getenv("RATE_INTERVAL", "5m")

QUERY = f'rate(vllm:prompt_tokens_total{{model_name="{MODEL_NAME}"}}[{RATE_INTERVAL}])'
INTERVAL_SEC = int(os.getenv("INTERVAL_SEC", "10"))


def query_prometheus(query: str):
    url = f"{PROMETHEUS_URL}/api/v1/query"
    r = requests.get(url, params={"query": query}, timeout=10)
    r.raise_for_status()
    payload = r.json()
    if payload.get("status") != "success":
        raise RuntimeError(f"Prometheus error: {payload}")
    return payload["data"]["result"]


if __name__ == "__main__":
    print(f"Querying {PROMETHEUS_URL} for '{QUERY}' every {INTERVAL_SEC}s …")
    while True:
        try:
            results = query_prometheus(QUERY)
            if not results:
                print(f"[no data] {QUERY}")
            else:
                print(f"\n--- Results for {QUERY} ---")
                for res in results:
                    labels = res.get("metric", {})
                    ts, val = res.get("value", [None, None])
                    inst = labels.get("instance") or labels
                    print(f"{inst} => {val} (ts={ts})")
        except Exception as e:
            print(f"Error: {e}")
        time.sleep(INTERVAL_SEC)
