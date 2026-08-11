#!/usr/bin/env python3
"""Docker-only fixed-budget Prefill/Decode rebalancer (verification backend).

Implements the same transition-plan semantics as the Kubernetes
``pd_rebalancer.py`` controller, but executes against owned Docker containers
instead of Kubernetes Deployments. It is the runtime-verification backend for
the A3 Docker lab; it never touches containers outside the ``lzm-dynpd-``
prefix and never uses NPUs outside the configured allocation (default 0-7).

Transition order (fixed budget):

    P2,D1 -> P1,D1 -> P1,D2
    P1,D2 -> P1,D1 -> P2,D1

The proxy's /instances/add is asynchronous: after adding an instance this
executor waits for the proxy healthcheck topology before performing the next
operation, so a source role is never torn down before the target role is
visible to the proxy.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

from pd_rebalancer import ModelConfig, Replicas, transition_plan


DEFAULT_PREFIX = "lzm-dynpd"
DEFAULT_NPUS = list(range(8))
DEFAULT_IMAGE = "quay.nju.edu.cn/ascend/vllm-ascend:v0.22.1rc1-a3"
DEFAULT_MODEL_DIR = "/mnt/sdb/models/Qwen3-0.6B"
DEFAULT_MODEL_NAME = "Qwen3-0.6B"
DEFAULT_SERVED_MODEL_NAME = "qwen3-0.6b-pd-lab"
DEFAULT_LOCAL_IP = "192.168.0.223"
DEFAULT_NIC = "enp23s0f3"
DEFAULT_PROXY_PORT = 19090
DEFAULT_SLOTS = [
    {"npu": 0, "prefill_http": 19100, "prefill_kv": 19300, "decode_http": 19200, "decode_kv": 19400},
    {"npu": 1, "prefill_http": 19101, "prefill_kv": 19301, "decode_http": 19201, "decode_kv": 19401},
    {"npu": 2, "prefill_http": 19102, "prefill_kv": 19302, "decode_http": 19202, "decode_kv": 19402},
]
DEFAULT_READY_TIMEOUT = 1800
DEFAULT_TENSOR_PARALLEL_SIZE = 1
DEFAULT_MAX_MODEL_LEN = 4096
DEFAULT_MAX_NUM_BATCHED_TOKENS = 4096
DEFAULT_MAX_NUM_SEQS = 16
DEFAULT_GPU_MEMORY_UTILIZATION = 0.60
DEFAULT_ENFORCE_EAGER = True


@dataclass(frozen=True)
class Slot:
    npu: int
    prefill_http: int
    prefill_kv: int
    decode_http: int
    decode_kv: int


@dataclass(frozen=True)
class Config:
    prefix: str
    image: str
    model_dir: str
    model_name: str
    served_model_name: str
    tensor_parallel_size: int
    max_model_len: int
    max_num_batched_tokens: int
    max_num_seqs: int
    gpu_memory_utilization: float
    enforce_eager: bool
    local_ip: str
    nic: str
    proxy_port: int
    slots: tuple[Slot, ...]
    allowed_npus: frozenset[int]
    min_prefill: int
    min_decode: int
    ready_timeout: float

    @property
    def max_total(self) -> int:
        return len(self.slots)

    def model_config(self) -> ModelConfig:
        return ModelConfig(
            name="docker-pd",
            prefill_deployment="",
            decode_deployment="",
            min_prefill=self.min_prefill,
            min_decode=self.min_decode,
            max_total=self.max_total,
        )


def _as_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "on")
    return default


def load_config() -> Config:
    raw: dict[str, Any] = {}
    path = os.environ.get("LZM_DYNPD_CONFIG")
    if path:
        with open(path, encoding="utf-8") as handle:
            raw = json.load(handle)

    allowed = frozenset(int(n) for n in raw.get("allowed_npus", DEFAULT_NPUS))
    if not allowed.issubset(set(range(8))):
        raise ValueError("allowed_npus must be a subset of 0-7 on this host")

    slots = tuple(
        Slot(
            npu=int(item["npu"]),
            prefill_http=int(item["prefill_http"]),
            prefill_kv=int(item["prefill_kv"]),
            decode_http=int(item["decode_http"]),
            decode_kv=int(item["decode_kv"]),
        )
        for item in raw.get("slots", DEFAULT_SLOTS)
    )
    for slot in slots:
        if slot.npu not in allowed:
            raise ValueError(f"slot npu {slot.npu} is outside the allowed allocation {sorted(allowed)}")

    tensor_parallel_size = int(raw.get("tensor_parallel_size", DEFAULT_TENSOR_PARALLEL_SIZE))
    max_model_len = int(raw.get("max_model_len", DEFAULT_MAX_MODEL_LEN))
    max_num_batched_tokens = int(raw.get("max_num_batched_tokens", DEFAULT_MAX_NUM_BATCHED_TOKENS))
    max_num_seqs = int(raw.get("max_num_seqs", DEFAULT_MAX_NUM_SEQS))
    gpu_memory_utilization = float(raw.get("gpu_memory_utilization", DEFAULT_GPU_MEMORY_UTILIZATION))
    enforce_eager = _as_bool(raw.get("enforce_eager", DEFAULT_ENFORCE_EAGER), DEFAULT_ENFORCE_EAGER)
    if tensor_parallel_size < 1:
        raise ValueError("tensor_parallel_size must be >= 1")
    if max_model_len < 1:
        raise ValueError("max_model_len must be >= 1")
    if max_num_batched_tokens < 1:
        raise ValueError("max_num_batched_tokens must be >= 1")
    if max_num_seqs < 1:
        raise ValueError("max_num_seqs must be >= 1")
    if not 0.0 < gpu_memory_utilization <= 1.0:
        raise ValueError("gpu_memory_utilization must be within (0, 1]")

    return Config(
        prefix=raw.get("prefix", DEFAULT_PREFIX),
        image=raw.get("image", DEFAULT_IMAGE),
        model_dir=raw.get("model_dir", DEFAULT_MODEL_DIR),
        model_name=raw.get("model_name", DEFAULT_MODEL_NAME),
        served_model_name=raw.get("served_model_name", DEFAULT_SERVED_MODEL_NAME),
        tensor_parallel_size=tensor_parallel_size,
        max_model_len=max_model_len,
        max_num_batched_tokens=max_num_batched_tokens,
        max_num_seqs=max_num_seqs,
        gpu_memory_utilization=gpu_memory_utilization,
        enforce_eager=enforce_eager,
        local_ip=raw.get("local_ip", DEFAULT_LOCAL_IP),
        nic=raw.get("nic", DEFAULT_NIC),
        proxy_port=int(raw.get("proxy_port", DEFAULT_PROXY_PORT)),
        slots=slots,
        allowed_npus=allowed,
        min_prefill=int(raw.get("min_prefill", 1)),
        min_decode=int(raw.get("min_decode", 1)),
        ready_timeout=float(raw.get("ready_timeout", DEFAULT_READY_TIMEOUT)),
    )


def docker(*args: str, check: bool = True, timeout: int = 600) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        ["docker", *args],
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if check and result.returncode != 0:
        raise RuntimeError(f"docker {' '.join(args)} failed: {result.stderr.strip()}")
    return result


def role_container(prefix: str, role: str, slot: int) -> str:
    short = "p" if role == "prefill" else "d"
    return f"{prefix}-{short}{slot}"


def owned_container_names(prefix: str) -> list[str]:
    pattern = re.compile(rf"^{re.escape(prefix)}-(p\d+|d\d+|proxy)$")
    result = docker("ps", "-a", "--format", "{{.Names}}", check=False)
    return [line.strip() for line in result.stdout.splitlines() if pattern.match(line.strip())]


def occupied_slots(config: Config) -> list[tuple[str, Slot]]:
    occupied: list[tuple[str, Slot]] = []
    slots_by_npu = {slot.npu: slot for slot in config.slots}
    for name in owned_container_names(config.prefix):
        match = re.fullmatch(rf"{re.escape(config.prefix)}-(p|d)(\d+)", name)
        if not match:
            continue
        role = "prefill" if match.group(1) == "p" else "decode"
        slot_index = int(match.group(2))
        if slot_index < len(config.slots):
            occupied.append((role, config.slots[slot_index]))
    return occupied


def current_counts(config: Config) -> Replicas:
    counts = {"prefill": 0, "decode": 0}
    for role, _ in occupied_slots(config):
        counts[role] += 1
    return Replicas(prefill=counts["prefill"], decode=counts["decode"])


def select_removals(occupied: list[tuple[str, Slot]], role: str, target: int) -> list[Slot]:
    matching = [slot for used_role, slot in occupied if used_role == role]
    if role == "prefill":
        matching.sort(key=lambda slot: slot.npu, reverse=True)
    else:
        matching.sort(key=lambda slot: slot.npu)
    return matching[: max(0, len(matching) - target)]


def select_additions(config: Config, occupied: list[tuple[str, Slot]], role: str, count: int) -> list[Slot]:
    used = {slot.npu for _, slot in occupied}
    free = [slot for slot in config.slots if slot.npu not in used]
    free.sort(key=lambda slot: slot.npu, reverse=(role == "decode"))
    return free[:count]


def http_get(url: str, timeout: float = 5.0) -> int:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return int(response.status)
    except (urllib.error.URLError, OSError):
        return -1


def http_post_json(url: str, payload: dict[str, Any], timeout: float = 30.0) -> dict[str, Any]:
    data = json.dumps(payload).encode()
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read())


def wait_http(url: str, timeout: float, label: str) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if http_get(url) == 200:
            print(f"ready: {label} {url}", flush=True)
            return
        time.sleep(5)
    raise TimeoutError(f"{label} did not become ready at {url} within {timeout}s")


def wait_topology(config: Config, expected: Replicas, timeout: Optional[float] = None) -> None:
    timeout = timeout or config.ready_timeout
    health_url = f"http://127.0.0.1:{config.proxy_port}/healthcheck"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(health_url, timeout=5) as response:
                health = json.loads(response.read())
            prefill = int(health.get("prefill_instances", -1))
            decode = int(health.get("decode_instances", -1))
            if prefill == expected.prefill and decode == expected.decode:
                print(
                    f"proxy topology reached P{expected.prefill},D{expected.decode}",
                    flush=True,
                )
                return
        except (urllib.error.URLError, OSError, ValueError, json.JSONDecodeError):
            pass
        time.sleep(5)
    raise TimeoutError(
        f"proxy did not reach P{expected.prefill},D{expected.decode} within {timeout}s"
    )


def proxy_running(config: Config) -> bool:
    return f"{config.prefix}-proxy" in owned_container_names(config.prefix)


def proxy_add(config: Config, role: str, slot: Slot) -> None:
    port = slot.prefill_http if role == "prefill" else slot.decode_http
    response = http_post_json(
        f"http://127.0.0.1:{config.proxy_port}/instances/add",
        {"type": role, "instances": [f"127.0.0.1:{port}"]},
    )
    print(f"proxy add {role} 127.0.0.1:{port}: {response}", flush=True)


def proxy_remove(config: Config, role: str, slot: Slot) -> None:
    port = slot.prefill_http if role == "prefill" else slot.decode_http
    response = http_post_json(
        f"http://127.0.0.1:{config.proxy_port}/instances/remove",
        {"type": role, "instances": [f"127.0.0.1:{port}"]},
    )
    print(f"proxy remove {role} 127.0.0.1:{port}: {response}", flush=True)


def wait_port_free(port: int, timeout: float = 300) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = subprocess.run(
            ["ss", "-ltn"],
            check=False,
            capture_output=True,
            text=True,
        )
        if f":{port} " not in result.stdout:
            return
        time.sleep(3)
    raise TimeoutError(f"port {port} still listening after {timeout}s")


def kv_config(config: Config, role: str, slot: Slot) -> str:
    kv_role = "kv_producer" if role == "prefill" else "kv_consumer"
    kv_port = slot.prefill_kv if role == "prefill" else slot.decode_kv
    engine_id = role_container(config.prefix, role, config.slots.index(slot))
    return json.dumps(
        {
            "kv_connector": "MooncakeConnectorV1",
            "kv_role": kv_role,
            "kv_port": str(kv_port),
            "engine_id": engine_id,
            "kv_connector_extra_config": {
                "use_ascend_direct": True,
                "prefill": {"dp_size": 1, "tp_size": config.tensor_parallel_size},
                "decode": {"dp_size": 1, "tp_size": config.tensor_parallel_size},
            },
        }
    )


def run_role(config: Config, role: str, slot: Slot) -> None:
    index = config.slots.index(slot)
    name = role_container(config.prefix, role, index)
    http_port = slot.prefill_http if role == "prefill" else slot.decode_http
    kv_port = slot.prefill_kv if role == "prefill" else slot.decode_kv

    vllm_args: list[str] = [
        "vllm",
        "serve",
        f"/models/{config.model_name}",
        "--host",
        "127.0.0.1",
        "--port",
        str(http_port),
        "--tensor-parallel-size",
        str(config.tensor_parallel_size),
        "--served-model-name",
        config.served_model_name,
        "--max-model-len",
        str(config.max_model_len),
        "--max-num-batched-tokens",
        str(config.max_num_batched_tokens),
        "--max-num-seqs",
        str(config.max_num_seqs),
        "--gpu-memory-utilization",
        str(config.gpu_memory_utilization),
        "--trust-remote-code",
        "--kv-transfer-config",
        kv_config(config, role, slot),
    ]
    if config.enforce_eager:
        vllm_args.append("--enforce-eager")
    if role == "decode":
        vllm_args.append("--no-enable-prefix-caching")

    run_args: list[str] = [
        "run",
        "-d",
        "--name",
        name,
        "--restart",
        "no",
        "--network",
        "host",
        "--ipc",
        "host",
        "--shm-size",
        "128g",
        "--privileged",
        "--device",
        "/dev/davinci_manager",
        "--device",
        "/dev/devmm_svm",
        "--device",
        "/dev/hisi_hdc",
    ]
    for npu in range(8):
        run_args += ["--device", f"/dev/davinci{npu}"]
    run_args += [
        "-v",
        f"{config.model_dir}:/models/{config.model_name}:ro",
        "-v",
        "/usr/local/Ascend/driver:/usr/local/Ascend/driver:ro",
        "-v",
        "/usr/local/dcmi:/usr/local/dcmi:ro",
        "-v",
        "/usr/local/bin/npu-smi:/usr/local/bin/npu-smi:ro",
        "-v",
        "/etc/ascend_install.info:/etc/ascend_install.info:ro",
        "-v",
        "/usr/local/sbin:/usr/local/sbin:ro",
        "-e",
        f"ASCEND_RT_VISIBLE_DEVICES={slot.npu}",
        "-e",
        f"HCCL_IF_IP={config.local_ip}",
        "-e",
        f"GLOO_SOCKET_IFNAME={config.nic}",
        "-e",
        f"TP_SOCKET_IFNAME={config.nic}",
        "-e",
        f"HCCL_SOCKET_IFNAME={config.nic}",
        "-e",
        "HCCL_OP_EXPANSION_MODE=AIV",
        "-e",
        "PYTORCH_NPU_ALLOC_CONF=expandable_segments:True",
        "-e",
        "OMP_NUM_THREADS=1",
        "-e",
        "PYTHONHASHSEED=0",
        config.image,
        "bash",
        "-lc",
        'export LD_LIBRARY_PATH=/usr/local/lib:/usr/local/Ascend/ascend-toolkit/latest/python/site-packages/mooncake:${LD_LIBRARY_PATH:-}; exec "$@"',
        "bash",
        *vllm_args,
    ]
    print(f"starting {name}: role={role} npu={slot.npu} http={http_port} kv={kv_port}", flush=True)
    docker(*run_args)
    wait_http(
        f"http://127.0.0.1:{http_port}/v1/models",
        config.ready_timeout,
        f"{name}",
    )


def run_proxy(config: Config) -> None:
    name = f"{config.prefix}-proxy"
    base_prefill = config.slots[0].prefill_http
    base_decode = config.slots[-1].decode_http
    run_args = [
        "run",
        "-d",
        "--name",
        name,
        "--restart",
        "no",
        "--network",
        "host",
        config.image,
        "python",
        "/vllm-workspace/vllm-ascend/examples/disaggregated_prefill_v1/load_balance_proxy_server_example.py",
        "--host",
        "127.0.0.1",
        "--port",
        str(config.proxy_port),
        "--workers",
        "1",
        "--prefiller-hosts",
        "127.0.0.1",
        "--prefiller-ports",
        str(base_prefill),
        "--decoder-hosts",
        "127.0.0.1",
        "--decoder-ports",
        str(base_decode),
        "--log-level",
        "INFO",
    ]
    print(
        f"starting {name} proxy=127.0.0.1:{config.proxy_port} "
        f"prefill={base_prefill} decode={base_decode}",
        flush=True,
    )
    docker(*run_args)
    wait_http(
        f"http://127.0.0.1:{config.proxy_port}/healthcheck",
        config.ready_timeout,
        name,
    )


def remove_role(config: Config, role: str, slot: Slot) -> None:
    index = config.slots.index(slot)
    name = role_container(config.prefix, role, index)
    if proxy_running(config):
        proxy_remove(config, role, slot)
    docker("rm", "-f", name)
    port = slot.prefill_http if role == "prefill" else slot.decode_http
    wait_port_free(port)
    print(f"removed {name} (role={role} npu={slot.npu})", flush=True)


def add_role(config: Config, role: str, slot: Slot) -> None:
    run_role(config, role, slot)
    if proxy_running(config):
        proxy_add(config, role, slot)


def ensure_base(config: Config) -> Replicas:
    """Bring up P1,D1 plus the proxy, which the plan may then grow/shrink."""
    occupied = occupied_slots(config)
    used = {slot.npu for _, slot in occupied}
    if not any(role == "prefill" for role, _ in occupied):
        base = next(slot for slot in config.slots if slot.npu not in used and slot.npu in config.allowed_npus)
        add_role(config, "prefill", base)
        used.add(base.npu)
    if not any(role == "decode" for role, _ in occupied):
        base = next(
            slot
            for slot in reversed(config.slots)
            if slot.npu not in used and slot.npu in config.allowed_npus
        )
        add_role(config, "decode", base)
    if not proxy_running(config):
        run_proxy(config)
    expected = current_counts(config)
    wait_topology(config, expected)
    return expected


def apply_target(config: Config, target: Replicas) -> None:
    if target.prefill < config.min_prefill or target.decode < config.min_decode:
        raise ValueError("target violates the configured minimum replicas")
    if target.prefill + target.decode > config.max_total:
        raise ValueError("target exceeds the fixed Prefill/Decode replica budget")

    current = ensure_base(config)
    plan = transition_plan(current, target, config.model_config())
    print(f"transition plan from P{current.prefill},D{current.decode} -> P{target.prefill},D{target.decode}: {plan}", flush=True)

    expected = current
    for role, final in plan:
        while getattr(expected, role) > final:
            removals = select_removals(occupied_slots(config), role, final)
            for slot in removals:
                remove_role(config, role, slot)
                expected = current_counts(config)
                wait_topology(config, expected)
        while getattr(expected, role) < final:
            additions = select_additions(config, occupied_slots(config), role, final - getattr(expected, role))
            for slot in additions:
                add_role(config, role, slot)
                expected = current_counts(config)
                wait_topology(config, expected)

    final_counts = current_counts(config)
    wait_topology(config, final_counts)
    print(
        f"apply complete: P{final_counts.prefill},D{final_counts.decode}",
        flush=True,
    )


def smoke(config: Config, label: str, max_tokens: int = 32) -> dict[str, Any]:
    payload = {
        "model": config.served_model_name,
        "messages": [{"role": "user", "content": "Reply with exactly: dynamic PD is working."}],
        "temperature": 0,
        "max_tokens": max_tokens,
        "stream": False,
    }
    request = urllib.request.Request(
        f"http://127.0.0.1:{config.proxy_port}/v1/chat/completions",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", "X-Request-Id": f"lzm-dynpd-{label}"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=300) as response:
        body = json.loads(response.read())
    print(json.dumps(body, ensure_ascii=False), flush=True)
    return body


def cleanup(config: Config) -> None:
    names = owned_container_names(config.prefix)
    if not names:
        print("no owned containers to remove", flush=True)
        return
    for name in sorted(names, reverse=True):
        docker("rm", "-f", name)
        print(f"removed {name}", flush=True)


def status(config: Config) -> None:
    names = owned_container_names(config.prefix)
    counts = current_counts(config)
    print(json.dumps(
        {
            "containers": names,
            "prefill": counts.prefill,
            "decode": counts.decode,
            "proxy_port": config.proxy_port,
        },
        indent=2,
    ))


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Docker-only P/D rebalancer (verification backend)")
    parser.add_argument("--config", help="path to JSON config (or set LZM_DYNPD_CONFIG)")
    subparsers = parser.add_subparsers(dest="command", required=True)

    apply_parser = subparsers.add_parser("apply", help="transition to a target replica split")
    apply_parser.add_argument("--prefill", type=int, required=True)
    apply_parser.add_argument("--decode", type=int, required=True)

    smoke_parser = subparsers.add_parser("smoke", help="send one fixed-X-Request-Id request through the proxy")
    smoke_parser.add_argument("--label", required=True)
    smoke_parser.add_argument("--max-tokens", type=int, default=32)

    subparsers.add_parser("status", help="show owned containers and current counts")
    subparsers.add_parser("cleanup", help="remove only owned containers (lzm-dynpd-*)")

    args = parser.parse_args(list(argv) if argv is not None else None)
    if args.config:
        os.environ["LZM_DYNPD_CONFIG"] = args.config
    config = load_config()

    if args.command == "apply":
        apply_target(config, Replicas(prefill=args.prefill, decode=args.decode))
    elif args.command == "smoke":
        smoke(config, args.label, args.max_tokens)
    elif args.command == "status":
        status(config)
    elif args.command == "cleanup":
        cleanup(config)
    return 0


if __name__ == "__main__":
    sys.exit(main())
