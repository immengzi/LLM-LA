#!/bin/bash

nic_name="enp67s0f5"

# === 请填写以下IP ===
local_ip="本机IP"      # 例如: 10.1.2.3
node0_ip="主节点IP"    # 例如: 10.1.2.1
# =====================

export HCCL_OP_EXPANSION_MODE="AIV"

export HCCL_IF_IP=$local_ip

export GLOO_SOCKET_IFNAME=$nic_name

export TP_SOCKET_IFNAME=$nic_name

export HCCL_SOCKET_IFNAME=$nic_name


export OMP_PROC_BIND=false

export OMP_NUM_THREADS=16

export VLLM_USE_V1=1

export HCCL_BUFFSIZE=200

export PYTORCH_NPU_ALLOC_CONF=expandable_segments:True

export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH

# export VLLM_LOGGING_LEVEL=DEBUG

timestamp=$(date "+%Y%m%d%H%M%S")

# export VLLM_ASCEND_ENABLE_FLASHCOMM1=1


export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH

export PYTHONHASHSEED=0

export PYTHONPATH=$PYTHONPATH:/vllm-workspace/vllm

export MOONCAKE_CONFIG_PATH="/workspace/glm5/mooncake.json"

export ASCEND_BUFFER_POOL=4:8


vllm serve /workspace/models/GLM-5-w4a8-mtp-QuaRot \
  --served-model-name glm-5 \
  --host 0.0.0.0 \
  --port 8077 \
  --data-parallel-size 2 \
  --data-parallel-size-local 1 \
  --data-parallel-address $node0_ip \
  --data-parallel-rpc-port 13389 \
  --tensor-parallel-size 8 \
  --quantization ascend \
  --seed 1024 \
  --enable-expert-parallel \
  --max-num-seqs 32 \
  --max-num-batched-tokens 4096 \
  --trust-remote-code \
  --gpu-memory-utilization 0.95 \
  --compilation-config '{"cudagraph_mode": "FULL_DECODE_ONLY"}' \
  --additional-config '{"multistream_overlap_shared_expert":true}' \
  --speculative-config '{"num_speculative_tokens": 3, "method": "deepseek_mtp"}' \
  --tool-call-parser glm47 \
  --reasoning-parser glm45 \
  --enable-auto-tool-choice \
  --api-key 密码 \
  --kv-transfer-config \
  '{
    "kv_connector": "AscendStoreConnector",
    "kv_role": "kv_both",
    "kv_connector_extra_config": {
        "lookup_rpc_port":"1",
        "backend": "mooncake"
    }
  }' \
  --kv-events-config \
  '{
    "enable_kv_cache_events": true,
    "publisher": "zmq",
    "endpoint": "tcp://*:5557",
    "topic": "kv@glm5@'${local_ip}'"
  }'