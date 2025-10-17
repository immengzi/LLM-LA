#!/bin/bash
set -euo pipefail

TAR_PATH="${1:-images/quay.io_ascend_vllm-ascend_v0.10.1rc1.tar}"

echo "📦 Importing image from: $TAR_PATH ..."

# Import the image into Docker
sudo docker load -i "$TAR_PATH"

echo "✅ Loaded image from $TAR_PATH"
