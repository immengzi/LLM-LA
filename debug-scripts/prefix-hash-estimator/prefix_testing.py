import os
import time
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import string
import re
from urllib.parse import urlparse
import math # Import math for ceiling function

# --- Configuration ---
character_pool = string.ascii_letters + string.digits

BASE_URL = os.environ.get("VLLM_BASE_URL", "http://localhost:8080/v1")
API_KEY   = os.environ.get("OPENAI_API_KEY", "sk-noauth")
MODEL     = os.environ.get("VLLM_MODEL", "qwen-local")

NUM_REQUESTS   = int(os.environ.get("NUM_REQUESTS", "20"))
CONCURRENCY    = int(os.environ.get("CONCURRENCY", "4"))
MAX_TOKENS     = int(os.environ.get("MAX_TOKENS", "64"))
TEMPERATURE    = float(os.environ.get("TEMPERATURE", "0.2"))

SHARED_PREFIX = "Context: " + ("test" * 300) + "\n"
k_length = len(SHARED_PREFIX)

# --- Global lists to store results across all rounds ---
global_latencies = []
global_ok_count = 0
global_total_requests = 0

# --- Request Functions ---

def payload(prefix: str, suffix: str):
    """Generates the OpenAI-compatible payload."""
    return {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": "Be concise."},
            {"role": "user",   "content": prefix + suffix}
        ],
        "max_tokens": MAX_TOKENS,
        "temperature": TEMPERATURE
    }

def do_one(session: requests.Session, idx: int, prefix: str, suffix: str):
    """Performs a single request and returns its stats."""
    url = f"{BASE_URL}/chat/completions"
    headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}
    t0 = time.perf_counter()
    r = session.post(url, headers=headers, data=json.dumps(payload(prefix, suffix)), timeout=120)
    dt = time.perf_counter() - t0
    try:
        r.raise_for_status()
        response_json = r.json()
        if "choices" in response_json and len(response_json["choices"]) > 0 and \
           "message" in response_json["choices"][0] and "content" in response_json["choices"][0]["message"]:
            text = response_json["choices"][0]["message"]["content"]
        else:
            text = f"ERROR: Unexpected response format: {r.text[:200]}"
            raise ValueError("Missing expected keys in response JSON")
        ok = True
    except Exception as e:
        text = f"ERROR: {e} | status={r.status_code}, body={r.text[:200]}"
        ok = False
    return idx, dt, ok, text

# --- Statistics and Metrics Functions ---

def summarize(round_name: str, latencies, ok_cnt, total_reqs_in_round):
    """Prints latency statistics for a specific round."""
    if not latencies:
        print(f"{round_name} summary: ok={ok_cnt}/{total_reqs_in_round} - No successful requests to analyze latency.")
        return

    latencies.sort()
    # Calculate percentiles safely, ensuring indices are within bounds
    # Use math.ceil to round up for index calculation, or handle edge cases
    count = len(latencies)
    p50_idx = min(count - 1, count // 2) # Use median index
    p90_idx = min(count - 1, math.ceil(count * 0.90) - 1)
    p99_idx = min(count - 1, math.ceil(count * 0.99) - 1)

    p50 = latencies[p50_idx]
    p90 = latencies[p90_idx]
    p99 = latencies[p99_idx]

    print(f"{round_name} summary: ok={ok_cnt}/{total_reqs_in_round}  "
          f"min={min(latencies):.2f}s  p50={p50:.2f}s  p90={p90:.2f}s  p99={p99:.2f}s  max={max(latencies):.2f}s")

def summarize_global(latencies, ok_cnt, total_reqs):
    """Prints overall P90/P99 latency statistics across all rounds."""
    print("\n==== Global Summary ====")
    if not latencies:
        print(f"Overall summary: ok={ok_cnt}/{total_reqs} - No successful requests across all rounds.")
        return

    latencies.sort()
    count = len(latencies)
    # Calculate P90 and P99 safely
    p90_idx = min(count - 1, math.ceil(count * 0.90) - 1)
    p99_idx = min(count - 1, math.ceil(count * 0.99) - 1)

    p90 = latencies[p90_idx]
    p99 = latencies[p99_idx]

    print(f"Overall summary: ok={ok_cnt}/{total_reqs}  "
          f"Across all requests -> p90={p90:.2f}s  p99={p99:.2f}s")
    print("========================")


def fetch_and_print_vllm_metrics(round_name: str, debug: bool = False):
    """
    Fetches and prints key metrics from the vLLM /metrics endpoint.
    Calculates prefix cache hit rate and displays specific engine stats.
    """
    print(f"\n--- Metrics after {round_name} ---")

    try:
        parsed_url = urlparse(BASE_URL)
        hostname = parsed_url.hostname
        scheme = parsed_url.scheme
        if not hostname:
            raise ValueError("Could not parse hostname from BASE_URL")
    except Exception as e:
        print(f"  ERROR: Invalid BASE_URL '{BASE_URL}'. {e}")
        print("---------------------------------")
        return

    ports_to_try = []
    if parsed_url.port:
        ports_to_try.append(parsed_url.port)
    ports_to_try.append(8000)
    ports_to_try = sorted(list(set(ports_to_try)))

    metrics_text = None
    metrics_url = ""
    for port in ports_to_try:
        try:
            metrics_url = f"{scheme}://{hostname}:{port}/metrics"
            print(f"  Attempting to fetch metrics from: {metrics_url}")
            r = requests.get(metrics_url, timeout=5)
            r.raise_for_status()
            metrics_text = r.text
            print(f"  Successfully connected to {metrics_url}")
            break
        except requests.exceptions.ConnectionError:
            print(f"  Connection refused at {metrics_url}. (This may be normal).")
        except requests.exceptions.RequestException as e:
            print(f"  Failed to fetch from {metrics_url}: {e}")

    if not metrics_text:
        print("\n  ERROR: Could not connect to the /metrics endpoint on any common port.")
        print(f"  Tried ports: {ports_to_try}")
        print("---------------------------------")
        return

    if debug:
        print("--- RAW METRICS (debug=True) ---")
        print(metrics_text)
        print("--------------------------------")

    print("\n--- Calculated Stats ---")
    try:
        hits_match = re.search(r"^vllm:prefix_cache_hits_total\{.*\}(\s+[\d\.e\+-]+)", metrics_text, re.MULTILINE)
        queries_match = re.search(r"^vllm:prefix_cache_queries_total\{.*\}(\s+[\d\.e\+-]+)", metrics_text, re.MULTILINE)
        if hits_match and queries_match:
            hits = float(hits_match.group(1).strip())
            queries = float(queries_match.group(1).strip())
            if queries > 0:
                hit_rate = (hits / queries) * 100
                print(f"  Prefix Cache Hit Rate: {hit_rate:.2f}%  ({int(hits)} hits / {int(queries)} queries)")
            else:
                print(f"  Prefix Cache Hit Rate: 0 queries, 0 hits.")
        else:
            print(f"  Could not find prefix cache metrics (hits/queries) to calculate hit rate.")
    except Exception as e:
        print(f"  Error while calculating prefix cache stats: {e}")

    print("\n--- Engine Stats Snapshot ---")
    try:
        def get_metric_value(metric_name):
            match = re.search(fr"^{metric_name}\{{.*\}}(\s+[\d\.e\+-]+)", metrics_text, re.MULTILINE)
            if match:
                return float(match.group(1).strip())
            return None

        running_reqs = get_metric_value("vllm:num_requests_running")
        waiting_reqs = get_metric_value("vllm:num_requests_waiting")
        kv_cache_perc = get_metric_value("vllm:kv_cache_usage_perc")
        prompt_tokens_total = get_metric_value("vllm:prompt_tokens_total")
        gen_tokens_total = get_metric_value("vllm:generation_tokens_total")

        stats_line = "Engine 000: "
        if prompt_tokens_total is not None:
             stats_line += f"Total prompt tokens: {int(prompt_tokens_total)}, "
        if gen_tokens_total is not None:
             stats_line += f"Total generation tokens: {int(gen_tokens_total)}, "
        if running_reqs is not None:
            stats_line += f"Running: {int(running_reqs)} reqs, "
        if waiting_reqs is not None:
            stats_line += f"Waiting: {int(waiting_reqs)} reqs, "
        if kv_cache_perc is not None:
            stats_line += f"GPU KV cache usage: {kv_cache_perc * 100:.1f}%"

        stats_line = stats_line.rstrip(', ')
        if len(stats_line) > len("Engine 000: "):
             print(f"  {stats_line}")
        else:
             print("  Could not find specific engine stats (running/waiting reqs, cache usage).")

    except Exception as e:
        print(f"  Error while extracting engine stats: {e}")

    if not debug:
        print("\nKey vLLM Metrics:")
        key_metrics = [
            "vllm:kv_cache_usage_perc",
            "vllm:num_requests_running",
            "vllm:prompt_tokens_total",
            "vllm:generation_tokens_total"
        ]
        found_any = False
        for metric in key_metrics:
            match = re.search(fr"^{metric}\{{.*\}}(\s+[\d\.e\+-]+)", metrics_text, re.MULTILINE)
            if match:
                value_str = match.group(1).strip()
                print(f"  {metric}: {value_str}")
                found_any = True

        if not found_any:
            print(f"  Could not find key vLLM metrics in the output.")
            print(f"  (Searched for: {', '.join(key_metrics)})")

    print("---------------------------------")


# --- Main Execution ---

def run_round(round_name: str, prefixes: list[str]):
    """Runs a concurrent round of requests, prints stats, and updates global lists."""
    global global_latencies, global_ok_count, global_total_requests # Declare modification intent

    print(f"\n==== {round_name} ====")
    round_latencies, round_ok_cnt = [], 0
    suffixes = [f"Q{i}: list three quantum error detection codes." for i in range(NUM_REQUESTS)]

    # Determine how many requests to actually run based on available prefixes
    num_to_run = min(NUM_REQUESTS, len(prefixes))
    if num_to_run < NUM_REQUESTS:
        print(f"Warning: Only {num_to_run} prefixes available for this round, running fewer than {NUM_REQUESTS} requests.")

    global_total_requests += num_to_run # Update global total count

    with requests.Session() as s, ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        futures = [ex.submit(do_one, s, i, prefixes[i], suffixes[i]) for i in range(num_to_run)]

        for fut in as_completed(futures):
            try:
                i, dt, ok, _ = fut.result()
                round_latencies.append(dt)
                round_ok_cnt += int(ok)
                # Append to global lists as well
                global_latencies.append(dt)
                global_ok_count += int(ok)
                print(f"[{round_name}] req={i:02d}  {dt*1000:.1f} ms  {'OK' if ok else 'FAIL'}")
            except Exception as e:
                print(f"[{round_name}] request future failed: {e}")

    summarize(round_name, round_latencies, round_ok_cnt, num_to_run) # Pass actual number run


def main():
    print(f"Target: {BASE_URL}  model={MODEL}  n={NUM_REQUESTS} (per round)  concurrency={CONCURRENCY}")

    debug_mode = False

    # --- Round 1 ---
    hit_prefixes = [SHARED_PREFIX for _ in range(NUM_REQUESTS)]
    run_round("HIT round (identical prefix)", hit_prefixes)
    fetch_and_print_vllm_metrics("HIT round", debug=debug_mode)

    time.sleep(2.0)

    # # --- Round 2 ---
    # miss_prefixes = [''.join(random.choices(character_pool, k=k_length)) for i in range(NUM_REQUESTS)]
    # run_round("MISS round (unique nonce at start -> no reuse)", miss_prefixes)
    # fetch_and_print_vllm_metrics("MISS round", debug=debug_mode)

    # time.sleep(2.0)

    # # --- Round 3 ---
    # late_diverge_prefixes = [SHARED_PREFIX + f" ID={i}" for i in range(NUM_REQUESTS)]
    # run_round("HIT round (nonce after shared prefix -> reuse)", late_diverge_prefixes)
    # fetch_and_print_vllm_metrics("Late Diverge HIT round", debug=debug_mode)

    # --- Final Global Summary ---
    summarize_global(global_latencies, global_ok_count, global_total_requests)

if __name__ == "__main__":
    main()