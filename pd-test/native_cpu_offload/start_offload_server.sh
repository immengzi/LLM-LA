#!/bin/bash

# vLLM Server Startup Script
# This script starts a vLLM server with KV cache offloading on Ascend NPU

set -e  # Exit on error

# Configuration variables
CONTAINER_NAME="vllm-offload"
# IMAGE_NAME="vllm-ascend:kv-event-update"
IMAGE_NAME="quay.io/ascend/vllm-ascend:v0.14.0rc1"
# IMAGE_NAME="vllm-offload-0.14"
MODEL_PATH="/mnt/nvme1/haiting_jd/models/qwen3-8b"
PORT=10000
DEVICE_ID=0
ZMQ_PORT=5557

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored messages
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if container already exists
if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    log_warn "Container '${CONTAINER_NAME}' already exists."
    read -p "Do you want to remove it and start fresh? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        log_info "Removing existing container..."
        docker rm -f ${CONTAINER_NAME}
    else
        log_error "Aborted. Please remove or rename the existing container."
        exit 1
    fi
fi

# Check if model path exists
if [ ! -d "${MODEL_PATH}" ]; then
    log_error "Model path does not exist: ${MODEL_PATH}"
    exit 1
fi

log_info "Starting vLLM server container..."

docker run -it \
    --name ${CONTAINER_NAME} \
    --net=host \
    --device /dev/davinci0 \
    --device /dev/davinci_manager \
    --device /dev/devmm_svm \
    --device /dev/hisi_hdc \
    -v /usr/local/Ascend/driver:/usr/local/Ascend/driver:ro \
    -v ${MODEL_PATH}:/model \
    -e ASCEND_VISIBLE_DEVICES=${DEVICE_ID} \
    -e PYTHONPATH=/vllm-workspace/vllm:/vllm-workspace/vllm-ascend \
    -e PYTHONHASHSEED=0 \
    ${IMAGE_NAME} \
    python3 -m vllm.entrypoints.openai.api_server \
    --host 0.0.0.0 \
    --port ${PORT} \
    --model /model \
    --served-model-name qwen3-8b \
    --tensor-parallel-size 1 \
    --gpu-memory-utilization 0.7 \
    --max-model-len 4792 \
    --trust-remote-code \
    --disable-log-requests \
    --block-size 128 \
    --enable-prefix-caching \
    --prefix-caching-hash-algo sha256_cbor \
    --kv-events-config '{"enable_kv_cache_events": true, "publisher": "zmq", "endpoint": "tcp://*:'${ZMQ_PORT}'", "topic": "kv@'${CONTAINER_NAME}'@served-model"}' \
    --kv-transfer-config '{"kv_connector": "OffloadingConnector", "kv_role": "kv_both", "kv_connector_extra_config": {"num_cpu_blocks": 8192, "caching_hash_algo": "sha256_cbor", "spec_name": "NPUOffloadingSpec", "spec_module_path": "vllm_ascend.kv_offload.npu"}}'

# docker run -it \
#     --name ${CONTAINER_NAME} \
#     --net=host \
#     --device /dev/davinci7 \
#     --device /dev/davinci_manager \
#     --device /dev/devmm_svm \
#     --device /dev/hisi_hdc \
#     -v /usr/local/Ascend/driver:/usr/local/Ascend/driver:ro \
#     -v ${MODEL_PATH}:/model \
#     -e ASCEND_VISIBLE_DEVICES=${DEVICE_ID} \
#     -e PYTHONPATH=/vllm-workspace/vllm:/vllm-workspace/vllm-ascend \
#     ${IMAGE_NAME} \
#     python3 -m vllm.entrypoints.openai.api_server \
#     --host 0.0.0.0 \
#     --port ${PORT} \
#     --model /model \
#     --served-model-name qwen3-8b \
#     --tensor-parallel-size 1 \
#     --gpu-memory-utilization 0.7 \
#     --max-model-len 4792 \
#     --trust-remote-code \
#     --disable-log-requests \
#     --block-size 128 \
#     --enable-prefix-caching \
#     --kv-events-config '{"enable_kv_cache_events": true, "publisher": "zmq", "endpoint": "tcp://*:'${ZMQ_PORT}'", "topic": "kv@'${CONTAINER_NAME}'@served-model"}' \
#     --kv-transfer-config '{"kv_connector": "OffloadingConnector", "kv_role": "kv_both", "kv_connector_extra_config": {"num_cpu_blocks": 8192, "spec_name": "NPUOffloadingSpec", "spec_module_path": "vllm_ascend.kv_offload.npu"}}'
