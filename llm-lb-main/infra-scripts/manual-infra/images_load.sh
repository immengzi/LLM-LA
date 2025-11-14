#!/bin/bash
set -euo pipefail

TAR_PATH="${1:-images/docker.io_vllm_vllm-openai_latest.tar}"

echo "📦 Importing image from: $TAR_PATH ..."
sudo microk8s ctr --debug images import "$TAR_PATH"

echo "✅ Loaded image from $TAR_PATH"
