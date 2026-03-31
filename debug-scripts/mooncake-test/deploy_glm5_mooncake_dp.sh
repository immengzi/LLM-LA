#!/bin/bash

# ============================================================================
# GLM-5 Mooncake 部署脚本 - Data Parallel 2 架构
# ============================================================================
# 架构说明:
#   - Data Parallel Size: 2 (两个数据并行实例)
#   - Tensor Parallel Size: 8
#   - KV Role: kv_both (每个节点同时是producer和consumer)
#   - 这不是传统P-D分离，而是数据并行+KV传输
#
# 使用方法:
#   1. 填写 local_ip 和 node0_ip
#   2. 主节点和从节点都运行此脚本（从节点会自动加入DP组）
# ============================================================================

nic_name="enp67s0f1"

# === 请填写以下IP ===
local_ip="本机IP"      # 例如: 10.1.2.3
node0_ip="主节点IP"    # 例如: 10.1.2.1
# =====================

# 检查是否为从节点
IS_WORKER=false
if [ "$local_ip" != "$node0_ip" ]; then
    IS_WORKER=true
fi

echo "========================================"
echo "  GLM-5 Mooncake DP2 Deployment"
echo "========================================"
echo "  Local IP:  $local_ip"
echo "  Node0 IP:  $node0_ip"
echo "  Is Worker: $IS_WORKER"
echo "========================================"

# NCCL/Ascend环境变量
export HCCL_OP_EXPANSION_MODE="AIV"
export HCCL_IF_IP=$local_ip
export GLOO_SOCKET_IFNAME=$nic_name
export TP_SOCKET_IFNAME=$nic_name
export HCCL_SOCKET_IFNAME=$nic_name

# vLLM环境变量
export OMP_PROC_BIND=false
export OMP_NUM_THREADS=16
export VLLM_USE_V1=1
export HCCL_BUFFSIZE=200
export PYTORCH_NPU_ALLOC_CONF=expandable_segments:True
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
export PYTHONHASHSEED=0
export PYTHONPATH=$PYTHONPATH:/vllm-workspace/vllm
export MOONCAKE_CONFIG_PATH="/workspace/glm5/mooncake.json"
export ASCEND_BUFFER_POOL=4:8

# Mooncake仅主节点启动 (从节点不需要)
if [ "$IS_WORKER" = "false" ]; then
    echo "[Main Node] Starting mooncake_master..."
    # 检查mooncake是否已启动
    if ! pgrep -f "mooncake_master" > /dev/null; then
        mkdir -p /workspace/glm5/mooncake_logs
        nohup mooncake_master \
            --port 50088 \
            --eviction_high_watermark_ratio 0.9 \
            --eviction_ratio 0.1 \
            2>&1 | split -b 5M -d -a 5 - /workspace/glm5/mooncake_logs/logs_ &
        echo "[Main Node] mooncake_master started"
    else
        echo "[Main Node] mooncake_master already running"
    fi
fi

timestamp=$(date "+%Y%m%d%H%M%S")
log_file="deploy_glm_dp2_mooncake_${local_ip}_${timestamp}.log"

echo "[vLLM] Starting vLLM server..."

# 判断是否为从节点
if [ "$IS_WORKER" = "true" ]; then
    # 从节点配置 (--headless --data-parallel-start-rank 1)
    vllm serve /workspace/models/GLM-5-w4a8-mtp-QuaRot \
        --served-model-name glm-5 \
        --host 0.0.0.0 \
        --port 8077 \
        --headless \
        --data-parallel-size 2 \
        --data-parallel-size-local 1 \
        --data-parallel-start-rank 1 \
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
        --api-key ZhongRuanChuangXin! \
        --kv-transfer-config '{
            "kv_connector": "AscendStoreConnector",
            "kv_role": "kv_both",
            "kv_connector_extra_config": {
                "lookup_rpc_port":"1",
                "backend": "mooncake"
            }
        }' \
        --kv-events-config '{
            "enable_kv_cache_events": true,
            "publisher": "zmq",
            "endpoint": "tcp://*:5557",
            "topic": "kv@glm5@'${local_ip}'"
        }' \
        2>&1 | tee ${log_file}
else
    # 主节点配置
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
        --api-key ZhongRuanChuangXin! \
        --kv-transfer-config '{
            "kv_connector": "AscendStoreConnector",
            "kv_role": "kv_both",
            "kv_connector_extra_config": {
                "lookup_rpc_port":"1",
                "backend": "mooncake"
            }
        }' \
        --kv-events-config '{
            "enable_kv_cache_events": true,
            "publisher": "zmq",
            "endpoint": "tcp://*:5557",
            "topic": "kv@glm5@'${local_ip}'"
        }' \
        2>&1 | tee ${log_file}
fi