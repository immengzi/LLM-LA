#!/bin/bash
set -euo pipefail

TAR_PATH="${1:-/mnt/nvme1/saeid/images/quay.io_ascend_vllm-ascend_v0.10.1rc1.tar}"
# TAR_PATH="${1:-images/busybox.tar}"

echo "📦 Importing image from: $TAR_PATH ..."
docker load -i "$TAR_PATH"

echo "✅ Loaded image from $TAR_PATH"
