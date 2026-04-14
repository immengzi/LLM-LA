import os
import time
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import string
import re
from urllib.parse import urlparse
import math
import argparse
import csv
import subprocess
import sys
import os

# --- Configuration (Defaults) ---
character_pool = string.ascii_letters + string.digits

# BASE_URL determines the host and port for API calls
BASE_URL = os.environ.get("VLLM_BASE_URL", "http://localhost:8080/v1")
API_KEY   = os.environ.get("OPENAI_API_KEY", "sk-noauth")
# MODEL will be derived from the docker command or default

NUM_REQUESTS   = int(os.environ.get("NUM_REQUESTS", "20"))
CONCURRENCY    = int(os.environ.get("CONCURRENCY", "4"))
MAX_TOKENS     = int(os.environ.get("MAX_TOKENS", "64"))
TEMPERATURE    = float(os.environ.get("TEMPERATURE", "0.2"))
# SHARED_PREFIX and k_length will be set in main()

# --- Global lists for overall summary ---
global_latencies = []
global_ok_count = 0
global_total_requests = 0

# --- Docker Configuration (These might be updated by args in main) ---
GPU_DEVICE_ID = os.environ.get("GPU_DEVICE_ID", "3")
HOST_MODEL_PATH = os.environ.get("HOST_MODEL_PATH", "/mnt/deepseek-ai/DeepSeek-R1-Distill-Qwen-7B")
SHM_SIZE = os.environ.get("SHM_SIZE", "8g")
VLLM_IMAGE = os.environ.get("VLLM_IMAGE", "vllm/vllm-openai:latest")
SERVED_MODEL_NAME = os.environ.get("SERVED_MODEL_NAME", "deepseek-7b-local")
CONTAINER_MODEL_PATH = "/model"
CONTAINER_PORT = 8000
# HOST_PORT is now derived solely from BASE_URL
try:
    HOST_PORT = urlparse(BASE_URL).port or 8080
except ValueError:
    print(f"Warning: Could not parse port from BASE_URL '{BASE_URL}'. Defaulting host port to 8080.", file=sys.stderr)
    HOST_PORT = 8080

GPU_MEM_UTIL = os.environ.get("GPU_MEM_UTIL", "0.50")
CONTAINER_NAME = "vllm_benchmark_container"

# --- Docker Management Functions ---

def start_vllm_container():
    """Starts the vLLM Docker container in detached mode."""
    # Uses the global variables GPU_DEVICE_ID, HOST_PORT, CONTAINER_PORT, HOST_MODEL_PATH, etc.
    print(f"--- Starting vLLM container '{CONTAINER_NAME}' ---")
    docker_command_list = [
        "docker", "run",
        "--gpus", f'"device={GPU_DEVICE_ID}"', # Use updated global
        "--rm",
        "-d",
        "--name", CONTAINER_NAME,
        "-p", f"{HOST_PORT}:{CONTAINER_PORT}", # Use updated global
        "-v", f"{HOST_MODEL_PATH}:{CONTAINER_MODEL_PATH}:ro", # Use updated global
        "--shm-size", SHM_SIZE,
        "-e", "HF_HUB_DISABLE_TELEMETRY=1",
        "-e", "VLLM_ALLOW_RUNTIME_DOWNLOADS=0",
        "-e", "VLLM_NO_HF_ACCESS=1",
        VLLM_IMAGE,
        "--model", CONTAINER_MODEL_PATH,
        "--trust-remote-code",
        "--served-model-name", SERVED_MODEL_NAME, # Use updated global
        "--port", str(CONTAINER_PORT),
        "--enable-prefix-caching",
        "--gpu-memory-utilization", GPU_MEM_UTIL,
    ]
    docker_command_str = ' '.join(docker_command_list)
    print(f"Running command: {docker_command_str}")
    try:
        result = subprocess.run(docker_command_str, check=True, capture_output=True, text=True, shell=True)
        container_id = result.stdout.strip()
        print(f"Container '{CONTAINER_NAME}' started with ID: {container_id[:12]}...")
        return container_id
    except FileNotFoundError:
        print("ERROR: 'docker' command not found. Is Docker installed and in your PATH?", file=sys.stderr)
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Failed to start Docker container.", file=sys.stderr)
        print(f"Command: {docker_command_str}", file=sys.stderr)
        print(f"Return code: {e.returncode}", file=sys.stderr)
        print(f"Stderr: {e.stderr}", file=sys.stderr)
        print(f"Stdout: {e.stdout}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: An unexpected error occurred starting the container: {e}", file=sys.stderr)
        sys.exit(1)

def wait_for_container_ready(timeout=180):
    """Polls the vLLM health/readiness endpoint until it's ready."""
    # Uses global BASE_URL, HOST_PORT, API_KEY, SERVED_MODEL_NAME
    print(f"Waiting for vLLM server to be ready at {BASE_URL} (using host port {HOST_PORT}, timeout: {timeout}s)...")
    start_time = time.time()
    health_url_base = ""
    try:
        parsed_base = urlparse(BASE_URL)
        health_url_base = f"{parsed_base.scheme}://{parsed_base.hostname}:{HOST_PORT}" # Use HOST_PORT
    except Exception as e:
         print(f"ERROR: Could not construct health check URL from BASE_URL '{BASE_URL}': {e}", file=sys.stderr)
         return False

    health_url = f"{health_url_base}/health"
    models_url = f"{BASE_URL}/models" # Uses BASE_URL directly

    check_url = ""
    # Try health endpoint first
    try:
        r_health = requests.get(health_url, timeout=2)
        if r_health.status_code == 200:
            check_url = health_url
            print("Using /health endpoint for readiness check.")
        else:
            check_url = models_url
            print(f"Could not reach {health_url} (status: {r_health.status_code}), using {models_url} for readiness check.")
    except requests.exceptions.RequestException as e:
         check_url = models_url
         print(f"Could not reach {health_url} ({e}), using {models_url} for readiness check.")

    while time.time() - start_time < timeout:
        try:
            headers = {"Authorization": f"Bearer {API_KEY}"} if check_url == models_url else {}
            response = requests.get(check_url, timeout=5, headers=headers)
            if response.status_code == 200:
                if check_url == models_url:
                     model_list = response.json().get("data", [])
                     # Use the potentially updated global SERVED_MODEL_NAME
                     if any(model.get("id") == SERVED_MODEL_NAME for model in model_list):
                         print("vLLM server is ready and model is loaded!")
                         return True
                     else:
                         print(f"Server up, but model '{SERVED_MODEL_NAME}' not yet listed...")
                else: # /health endpoint just needs 200 OK
                     print("vLLM server is ready!")
                     return True
            else:
                print(f"Received status {response.status_code} from {check_url}...")
        except requests.exceptions.ConnectionError:
            print(".", end='', flush=True)
        except requests.exceptions.RequestException as e:
            print(f"\nWarning during readiness check: {e}")
        time.sleep(2)

    print(f"\nERROR: Timeout waiting for vLLM container at {check_url} to become ready.", file=sys.stderr)
    return False

def stop_vllm_container(container_name):
    """Stops the specified Docker container."""
    # Uses global CONTAINER_NAME passed as argument
    print(f"\n--- Stopping vLLM container '{container_name}' ---")
    try:
        check_run = subprocess.run(["docker", "ps", "-q", "-f", f"name={container_name}"], capture_output=True, text=True)
        if check_run.stdout.strip():
             stop_result = subprocess.run(["docker", "stop", container_name], check=True, capture_output=True, text=True)
             print(f"Container '{container_name}' stopped successfully.")
        else:
             print(f"Container '{container_name}' not found running.")
    except FileNotFoundError:
        print("ERROR: 'docker' command not found.", file=sys.stderr)
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Failed to stop Docker container '{container_name}'.", file=sys.stderr)
        print(f"Stderr: {e.stderr}", file=sys.stderr)
    except Exception as e:
        print(f"ERROR: An unexpected error occurred stopping the container: {e}", file=sys.stderr)

# --- Request Functions ---
def payload(shared_model: str, prefix: str, suffix: str):
    """Generates the OpenAI-compatible payload."""
    # Uses global MAX_TOKENS, TEMPERATURE
    return {
        "model": shared_model,
        "messages": [
            {"role": "system", "content": "Be concise."},
            {"role": "user",   "content": prefix + suffix}
        ],
        "max_tokens": MAX_TOKENS,
        "temperature": TEMPERATURE
    }

def do_one(session: requests.Session, idx: int, prefix: str, suffix: str, shared_model: str):
    """Performs a single request and returns its stats."""
    # Uses global BASE_URL, API_KEY
    url = f"{BASE_URL}/chat/completions"
    headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}
    t0 = time.perf_counter()
    r = session.post(url, headers=headers, data=json.dumps(payload(shared_model, prefix, suffix)), timeout=120)
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
    """Prints latency statistics for a specific round and returns P90, P99."""
    p90, p99 = None, None
    if not latencies:
        print(f"{round_name} summary: ok={ok_cnt}/{total_reqs_in_round} - No successful requests to analyze latency.")
        return p90, p99

    latencies.sort()
    count = len(latencies)
    p50_idx = min(count - 1, count // 2) if count > 0 else 0
    p90_idx = min(count - 1, math.ceil(count * 0.90) - 1) if count > 0 else 0
    p99_idx = min(count - 1, math.ceil(count * 0.99) - 1) if count > 0 else 0

    p50 = latencies[p50_idx] if count > 0 else 0
    p90 = latencies[p90_idx] if count > 0 else 0
    p99 = latencies[p99_idx] if count > 0 else 0

    print(f"{round_name} summary: ok={ok_cnt}/{total_reqs_in_round}  "
          f"min={min(latencies):.2f}s  p50={p50:.2f}s  p90={p90:.2f}s  p99={p99:.2f}s  max={max(latencies):.2f}s")
    return p90, p99

def summarize_global(latencies, ok_cnt, total_reqs):
    """Prints overall P90/P99 latency statistics across all rounds."""
    print("\n==== Global Summary ====")
    if not latencies:
        print(f"Overall summary: ok={ok_cnt}/{total_reqs} - No successful requests across all rounds.")
        print("========================")
        return

    latencies.sort()
    count = len(latencies)
    p90_idx = min(count - 1, math.ceil(count * 0.90) - 1) if count > 0 else 0
    p99_idx = min(count - 1, math.ceil(count * 0.99) - 1) if count > 0 else 0

    p90 = latencies[p90_idx] if count > 0 else 0
    p99 = latencies[p99_idx] if count > 0 else 0

    print(f"Overall summary: ok={ok_cnt}/{total_reqs}  "
          f"Across all requests -> p90={p90:.2f}s  p99={p99:.2f}s")
    print("========================")


def fetch_and_print_vllm_metrics(round_name: str, debug: bool = False):
    """
    Fetches vLLM /metrics, prints stats, and returns the prefix cache hit rate.
    """
    # Uses global BASE_URL, HOST_PORT
    print(f"\n--- Metrics after {round_name} ---")
    hit_rate = None

    try:
        parsed_url = urlparse(BASE_URL)
        hostname = parsed_url.hostname
        scheme = parsed_url.scheme
        metrics_check_port = HOST_PORT # Use derived host port
        if not hostname:
            raise ValueError("Could not parse hostname from BASE_URL")
    except Exception as e:
        print(f"  ERROR: Invalid BASE_URL '{BASE_URL}'. {e}")
        print("---------------------------------")
        return hit_rate

    metrics_url = f"{scheme}://{hostname}:{metrics_check_port}/metrics"
    metrics_text = None
    try:
        print(f"  Attempting to fetch metrics from: {metrics_url}")
        r = requests.get(metrics_url, timeout=5)
        r.raise_for_status()
        metrics_text = r.text
        print(f"  Successfully connected to {metrics_url}")
    except requests.exceptions.RequestException as e:
        print(f"  Failed to fetch metrics from {metrics_url}: {e}")
        print("\n  ERROR: Could not connect to the /metrics endpoint.")
        print(f"  Please check that your vLLM server is running and the")
        print(f"  /metrics endpoint is accessible on port {metrics_check_port}.")
        print("---------------------------------")
        return hit_rate

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
                hit_rate = 0.0
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
    return hit_rate


# --- Run Round Function ---
def run_round(round_name: str, prefixes: list[str], shared_model: str):
    """Runs a concurrent round of requests, prints stats, updates global lists, and returns P90, P99."""
    # Uses global NUM_REQUESTS, CONCURRENCY
    # Modifies global global_latencies, global_ok_count, global_total_requests
    global global_latencies, global_ok_count, global_total_requests

    print(f"\n==== {round_name} ====")
    round_latencies, round_ok_cnt = [], 0
    suffixes = [f"Q{i}: list three quantum error detection codes." for i in range(NUM_REQUESTS)]

    num_to_run = min(NUM_REQUESTS, len(prefixes))
    if num_to_run < NUM_REQUESTS:
        print(f"Warning: Only {num_to_run} prefixes available for this round, running fewer than {NUM_REQUESTS} requests.")
        suffixes = suffixes[:num_to_run]

    global_total_requests += num_to_run

    with requests.Session() as s, ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        futures = [ex.submit(do_one, s, i, prefixes[i], suffixes[i], shared_model) for i in range(num_to_run)]
        for fut in as_completed(futures):
            try:
                i, dt, ok, _ = fut.result()
                round_latencies.append(dt)
                round_ok_cnt += int(ok)
                global_latencies.append(dt)
                global_ok_count += int(ok)
                print(f"[{round_name}] req={i:02d}  {dt*1000:.1f} ms  {'OK' if ok else 'FAIL'}")
            except Exception as e:
                print(f"[{round_name}] request future failed: {e}")

    p90, p99 = summarize(round_name, round_latencies, round_ok_cnt, num_to_run)
    return p90, p99, round_ok_cnt, num_to_run

# --- Save Results Function ---
def save_results_to_csv(results_log: list, filename="vllm_benchmark_results.csv"):
    """Saves the collected results to a CSV file."""
    if not results_log:
        print("\nNo results to save.")
        return

    header = ["Round Name", "Prefix Length", "Hit Rate (%)", "P90 Latency (s)", "P99 Latency (s)"]
    if results_log and not all(k in results_log[0] for k in header):
        print("Warning: Result dictionary keys mismatch expected CSV header. Using keys from first result.")
        header = results_log[0].keys()

    try:
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=header)
            writer.writeheader()
            writer.writerows(results_log)
        print(f"\nResults successfully saved to {filename}")
    except Exception as e:
        print(f"\nERROR: Failed to save results to {filename}: {e}")

# *** MODIFICATION 1: Added new JSON save function ***
# def save_results_to_json(results_log: list, filename="vllm_benchmark_results.json"):
#     """Saves the collected results to a JSON file."""
#     if not results_log:
#         print("\nNo results to save.")
#         return

#     try:
#         with open(filename, 'w', encoding='utf-8') as f:
#             json.dump(results_log, f, indent=4)
#         print(f"\nResults successfully saved to {filename}")
#     except Exception as e:
#         print(f"\nERROR: Failed to save results to {filename}: {e}")

def save_results_to_json(results_log: list, folder_name: str, base_filename="results.json"):
    """Saves the collected results to a JSON file inside a specified folder."""
    if not results_log:
        print("\nNo results to save.")
        return

    full_path = "" # Initialize to provide broader scope for error message
    try:
        # Create the directory if it doesn't exist
        os.makedirs(folder_name, exist_ok=True)
        
        # Construct the full file path
        full_path = os.path.join(folder_name, base_filename)
        
        with open(full_path, 'w', encoding='utf-8') as f:
            json.dump(results_log, f, indent=4)
        print(f"\nResults successfully saved to {full_path}")
    except Exception as e:
        # Use full_path in error, or folder_name if path was never constructed
        location = full_path if full_path else folder_name
        print(f"\nERROR: Failed to save results to {location}: {e}")

# --- Main Application Logic ---
def main(args):
    # --- Declare global variables FIRST (to allow modification) ---
    global GPU_DEVICE_ID, HOST_MODEL_PATH, SERVED_MODEL_NAME, BASE_URL, HOST_PORT

    # --- Update configuration from args ---
    GPU_DEVICE_ID = args.gpu_id
    HOST_MODEL_PATH = args.model_path
    SERVED_MODEL_NAME = args.served_model_name # Update global

    # Re-derive HOST_PORT and BASE_URL in case defaults were used but args weren't
    # or if BASE_URL env var was set without a port initially
    try:
        # Get current BASE_URL (from env or default)
        parsed_base_url_orig = urlparse(BASE_URL)
        # Reconstruct BASE_URL using HOST_PORT (which also defaults to 8080)
        BASE_URL = f"{parsed_base_url_orig.scheme or 'http'}://{parsed_base_url_orig.hostname or 'localhost'}:{HOST_PORT}{parsed_base_url_orig.path or '/v1'}"
    except ValueError:
        print(f"Warning: Could not parse BASE_URL '{BASE_URL}'. Using default http://localhost:{HOST_PORT}/v1", file=sys.stderr)
        BASE_URL = f"http://localhost:{HOST_PORT}/v1"


    # --- Derive SHARED_PREFIX and k_length from args ---
    shared_prefix_content = args.prefix_content * 10000
    shared_prefix = "Context: " + shared_prefix_content + "\n"
    k_length = len(shared_prefix)
    shared_model_for_payload = SERVED_MODEL_NAME # Use the (potentially updated) global

    print(f"Target API: {BASE_URL} (Host Port: {HOST_PORT}) Model Name (in requests): {shared_model_for_payload}")
    print(f"Config: n={NUM_REQUESTS} (per round)  concurrency={CONCURRENCY}")
    print(f"Shared Prefix Length (k_length): {k_length}")

    debug_mode = args.debug
    results_log = []
    container_id = None # Initialize container_id

    try:
        container_id = start_vllm_container()
        if not container_id:
            return

        if not wait_for_container_ready():
            return

        print("\nStarting benchmark rounds...")

        # --- Round 1 ---
        # hit_prefixes = [shared_prefix for _ in range(NUM_REQUESTS)]
        # r1_p90, r1_p99 = run_round("HIT round (identical prefix)", hit_prefixes, shared_model_for_payload)
        # r1_hit_rate = fetch_and_print_vllm_metrics("HIT round", debug=debug_mode)
        # results_log.append({
        #     "Round Name": "HIT (identical)", "Prefix Length": k_length,
        #     "Hit Rate (%)": f"{r1_hit_rate:.2f}" if r1_hit_rate is not None else "N/A",
        #     "P90 Latency (s)": f"{r1_p90:.2f}" if r1_p90 is not None else "N/A",
        #     "P99 Latency (s)": f"{r1_p99:.2f}" if r1_p99 is not None else "N/A"
        # })
        # time.sleep(2.0)

        # --- Round 2 ---
        miss_prefixes = [''.join(random.choices(character_pool, k=k_length)) for i in range(NUM_REQUESTS)]
        # Determine success status
        r2_p90, r2_p99, r2_ok_cnt, r2_num_to_run  = run_round("MISS round (unique prefix)", miss_prefixes, shared_model_for_payload)
        r2_status = "Success" if (r2_ok_cnt == r2_num_to_run and r2_num_to_run > 0) else "Fail"
        r2_hit_rate = fetch_and_print_vllm_metrics("MISS round", debug=debug_mode)
        results_log.append({
            "Round Name": "MISS (unique)", "Prefix Length": k_length,
            "Status": r2_status,                  # <-- ADDED
            "OK Requests": r2_ok_cnt,             # <-- ADDED
            "Total Requests": r2_num_to_run,        # <-- ADDED
            "Hit Rate (%)": f"{r2_hit_rate:.2f}" if r2_hit_rate is not None else "N/A",
            "P90 Latency (s)": f"{r2_p90:.2f}" if r2_p90 is not None else "N/A",
            "P99 Latency (s)": f"{r2_p99:.2f}" if r2_p99 is not None else "N/A"
        })
        time.sleep(2.0)

        # --- Final Summaries ---
        summarize_global(global_latencies, global_ok_count, global_total_requests)

        output_folder_name = args.output_folder
        output_base_filename = args.output_filename
        
        save_results_to_json(results_log,
                             folder_name=output_folder_name,
                             base_filename=f"{output_base_filename}.json")
        
        # # *** MODIFICATION 2: Set filename and call both save functions ***
        # output_folder_name = "prefix_identical"
        # # save_results_to_csv(results_log, filename=f"{output_filename_base}.csv")
        # save_results_to_json(results_log, folder_name=output_folder_name, base_filename="results.json")

    except KeyboardInterrupt:
        print("\nBenchmark interrupted by user.")
    except Exception as e:
        print(f"\nAn unexpected error occurred during the benchmark: {e}")
        # Optionally re-raise the exception for more detail
        # raise
    finally:
        # --- Ensure Container is Stopped ---
        stop_vllm_container(CONTAINER_NAME)


if __name__ == "__main__":
    # --- Argument Parser Setup ---
    parser = argparse.ArgumentParser(description="Start vLLM, benchmark performance, log results, and stop vLLM.")
    parser.add_argument(
        "--prefix-content",
        type=str,
        default="test",
        help="The string content to repeat 300 times for the SHARED_PREFIX (default: 'test')."
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode to print raw metrics from the server."
    )
    # Use initial global values as defaults for args
    parser.add_argument("--gpu-id", type=str, default=GPU_DEVICE_ID, help=f"GPU device ID to use (default: {GPU_DEVICE_ID})")
    parser.add_argument("--model-path", type=str, default=HOST_MODEL_PATH, help=f"Host path to the model directory (default: {HOST_MODEL_PATH})")
    parser.add_argument("--served-model-name", type=str, default=SERVED_MODEL_NAME, help=f"Name vLLM uses for the model (default: {SERVED_MODEL_NAME})")
    parser.add_argument(
        "--output-folder",
        type=str,
        default="prefix_random_ds7B",
        help="The name of the folder to create and save results in (default: 'prefix_identical')."
    )
    parser.add_argument(
        "--output-filename",
        type=str,
        default="results",
        help="The base name for the output JSON and CSV files (without extension, default: 'results')."
    )

    parsed_args = parser.parse_args()

    # Call main with parsed arguments
    main(parsed_args)