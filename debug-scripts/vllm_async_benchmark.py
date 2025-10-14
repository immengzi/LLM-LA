#!/usr/bin/env python3
import os, sys, time, signal, asyncio, subprocess, re
from contextlib import suppress
from difflib import SequenceMatcher

# ====== CONFIG ======
MODEL_PATH = os.environ.get(
    "MODEL_PATH", os.path.abspath("/home/saeid/llm-lb/qwen-test")
)
SERVED_NAME = os.environ.get("SERVED_NAME", "qwen-test")
API_KEY = os.environ.get("VLLM_API_KEY", "token-abc123")
PORT = int(os.environ.get("VLLM_PORT", "8000"))
BASE_URL = f"http://localhost:{PORT}/v1"
TRUST_REMOTE_CODE = os.environ.get("TRUST_REMOTE_CODE", "1") in ("1", "true", "True")

SEED = int(os.environ.get("SEED", "1234"))
MAX_TOKENS = int(os.environ.get("MAX_TOKENS", "128"))
SIM_THRESHOLD = float(os.environ.get("SIM_THRESHOLD", "0.20"))  # 0..1

PROMPTS = [
    "Explain quantum entanglement in simple terms.",
    "Summarize the history of the Roman Empire in 3 bullet points.",
    "Write Python code to reverse a string.",
    "What are the health benefits of meditation?",
    "Translate 'Good morning' into Spanish.",
]

# ====== SANITIZER (strip <think> or use <final>) ======
THINK_BLOCK_RE = re.compile(r"<think>.*?</think>", flags=re.DOTALL | re.IGNORECASE)
TAG_RE = re.compile(r"</?final>|</?think>", flags=re.IGNORECASE)


def sanitize_answer(txt: str) -> str:
    m = re.search(r"<final>(.*?)</final>", txt, flags=re.DOTALL | re.IGNORECASE)
    if m:
        return m.group(1).strip()
    txt = THINK_BLOCK_RE.sub("", txt)
    txt = TAG_RE.sub("", txt)
    return txt.strip()


# ====== TEXT SIMILARITY ======
def similarity(a: str, b: str) -> float:
    """Return ratio in [0,1] using difflib (no extra deps)."""
    return SequenceMatcher(None, a, b).ratio()


# ====== GPU MONITOR ======
class GPUMonitor:
    def __init__(self, interval=0.5):
        self.interval = interval
        self.samples = []
        self.running = False

    def start(self):
        try:
            import threading, pynvml

            pynvml.nvmlInit()
            self.handle = pynvml.nvmlDeviceGetHandleByIndex(0)
            self.running = True

            def _loop():
                while self.running:
                    util = pynvml.nvmlDeviceGetUtilizationRates(self.handle)
                    mem = pynvml.nvmlDeviceGetMemoryInfo(self.handle)
                    self.samples.append((util.gpu, mem.used / 1024**2, time.time()))
                    time.sleep(self.interval)

            threading.Thread(target=_loop, daemon=True).start()
        except Exception as e:
            print(f"[WARN] GPU profiling disabled: {e}")
            self.running = False

    def stop(self):
        self.running = False

    def summary(self):
        if not self.samples:
            return None
        utils = [u for u, _, _ in self.samples]
        mems = [m for _, m, _ in self.samples]
        return {
            "avg_gpu_util": round(sum(utils) / len(utils), 2),
            "peak_gpu_util": max(utils),
            "avg_mem_MB": round(sum(mems) / len(mems), 1),
            "peak_mem_MB": max(mems),
            "n": len(self.samples),
        }


# ====== SERVER LIFECYCLE ======
def start_server():
    if not os.path.isdir(MODEL_PATH):
        print(f"[ERROR] MODEL_PATH missing: {MODEL_PATH}")
        sys.exit(1)
    cmd = [
        sys.executable,
        "-m",
        "vllm.entrypoints.openai.api_server",
        "--model",
        MODEL_PATH,
        "--served-model-name",
        SERVED_NAME,
        "--dtype",
        "auto",
        "--port",
        str(PORT),
        "--api-key",
        API_KEY,
        "--max-num-batched-tokens",
        "8192",
        "--seed",
        str(SEED),
        "--generation-config",
        "vllm",  # avoid HF gen_config overrides
        "--enforce-eager",
        "--tensor-parallel-size",
        "1",
    ]
    if TRUST_REMOTE_CODE:
        cmd.append("--trust-remote-code")

    env = os.environ.copy()
    env.setdefault("CUDA_VISIBLE_DEVICES", "0")
    env.setdefault("VLLM_TORCH_COMPILE", "0")  # ensure compile off
    env.setdefault("TORCHINDUCTOR_FREEZING", "0")
    env.setdefault("CUBLAS_WORKSPACE_CONFIG", ":4096:8")
    env.setdefault("PYTORCH_DETERMINISTIC", "1")
    env.setdefault("TOKENIZERS_PARALLELISM", "false")
    env.setdefault("CUDA_DEVICE_MAX_CONNECTIONS", "1")

    print("Starting vLLM server...")
    print("Command:", " ".join(cmd))
    return subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        preexec_fn=os.setsid if hasattr(os, "setsid") else None,
        env=env,
    )


def stream_logs_until_ready(proc, timeout_s=900):
    import threading, queue, httpx

    q = queue.Queue()

    def _reader():
        for line in iter(proc.stdout.readline, ""):
            sys.stdout.write(line)
            sys.stdout.flush()
            q.put(line)

    threading.Thread(target=_reader, daemon=True).start()
    health_url = f"http://localhost:{PORT}/health"
    models_url = f"http://localhost:{PORT}/v1/models"
    start = time.time()
    with httpx.Client(timeout=5.0) as client:
        while time.time() - start < timeout_s:
            if proc.poll() is not None:
                return False
            with suppress(Exception):
                if client.get(health_url).status_code == 200:
                    r = client.get(
                        models_url, headers={"Authorization": f"Bearer {API_KEY}"}
                    )
                    if r.status_code == 200 and SERVED_NAME in str(r.json()):
                        return True
            time.sleep(1)
    return False


def shutdown_server(proc):
    print("\nShutting down vLLM server...")
    try:
        if hasattr(os, "getpgid"):
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        else:
            proc.terminate()
    except Exception:
        pass
    with suppress(Exception):
        proc.wait(timeout=15)


# ====== REQUEST SETTINGS ======
STOP_TOKENS = ["</final>", "</think>"]  # cut reasoning tags early


# ====== BENCHMARKS ======
def run_sequential(prompts):
    from openai import OpenAI

    client = OpenAI(base_url=BASE_URL, api_key=API_KEY)
    gpu = GPUMonitor()
    gpu.start()
    t0 = time.time()
    outs = []
    for p in prompts:
        resp = client.chat.completions.create(
            model=SERVED_NAME,
            messages=[{"role": "user", "content": p}],
            temperature=0.0,
            top_p=1.0,
            seed=SEED,
            max_tokens=MAX_TOKENS,
            stop=STOP_TOKENS,
        )
        txt = resp.choices[0].message.content
        outs.append(sanitize_answer(txt))
    total = time.time() - t0
    gpu.stop()
    print(f"\nSequential runtime: {total:.2f}s")
    return outs, total, gpu.summary()


async def _one_call(async_client, p):
    resp = await async_client.chat.completions.create(
        model=SERVED_NAME,
        messages=[{"role": "user", "content": p}],
        temperature=0.0,
        top_p=1.0,
        seed=SEED,
        max_tokens=MAX_TOKENS,
        stop=STOP_TOKENS,
    )
    txt = resp.choices[0].message.content
    return sanitize_answer(txt)


def run_concurrent(prompts):
    from openai import AsyncOpenAI

    async def _run():
        async_client = AsyncOpenAI(base_url=BASE_URL, api_key=API_KEY)
        gpu = GPUMonitor()
        gpu.start()
        t0 = time.time()
        outs = await asyncio.gather(*(_one_call(async_client, p) for p in prompts))
        total = time.time() - t0
        gpu.stop()
        return list(outs), total, gpu.summary()

    return asyncio.run(_run())


# ====== MAIN ======
def main():
    proc = start_server()
    try:
        if not stream_logs_until_ready(proc):
            sys.exit(1)

        seq_out, seq_time, seq_gpu = run_sequential(PROMPTS)
        con_out, con_time, con_gpu = run_concurrent(PROMPTS)

        # Similarity check with threshold
        sims = []
        all_passed = True
        for i, (a, b) in enumerate(zip(seq_out, con_out), 1):
            s = similarity(a, b)
            sims.append(s)
            if s < SIM_THRESHOLD:
                print(f"[FAIL] Prompt #{i}: similarity={s:.2%} < {SIM_THRESHOLD:.0%}")
                print(f"  SEQ: {a}\n  CON: {b}\n")
                all_passed = False
            else:
                print(f"[OK]   Prompt #{i}: similarity={s:.2%}")

        if not all_passed:
            raise AssertionError(
                "Some sequential vs concurrent outputs differ too much!"
            )

        # Summary
        speedup = seq_time / con_time if con_time > 0 else float("inf")
        avg_sim = sum(sims) / len(sims) if sims else 0.0
        min_sim = min(sims) if sims else 0.0

        print("\n===== SUMMARY =====")
        print(f"Requests:               {len(PROMPTS)}")
        print(f"Max tokens:             {MAX_TOKENS}")
        print(f"Similarity threshold:   {SIM_THRESHOLD:.0%}")
        print(f"Avg / Min similarity:   {avg_sim:.2%} / {min_sim:.2%}")
        print(
            f"Determinism:            temp=0, top_p=1, seed={SEED}, --enforce-eager, compile=off"
        )
        print(f"Sequential runtime:     {seq_time:.2f}s")
        print(f"Concurrent runtime:     {con_time:.2f}s")
        print(f"Speedup (seq/conc):     {speedup:.2f}×")
        if seq_gpu:
            print(
                f"Seq GPU (avg/peak %):   {seq_gpu['avg_gpu_util']} / {seq_gpu['peak_gpu_util']} | Mem MB avg/peak: {seq_gpu['avg_mem_MB']} / {seq_gpu['peak_mem_MB']} (n={seq_gpu['n']})"
            )
        if con_gpu:
            print(
                f"Conc GPU (avg/peak %):  {con_gpu['avg_gpu_util']} / {con_gpu['peak_gpu_util']} | Mem MB avg/peak: {con_gpu['avg_mem_MB']} / {con_gpu['peak_mem_MB']} (n={con_gpu['n']})"
            )
        print("====================\n")

    finally:
        shutdown_server(proc)


if __name__ == "__main__":
    main()
