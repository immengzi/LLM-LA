#!/bin/bash
set -e

SRC_IMAGE="${1:-docker.io/vllm/vllm-openai:latest}"
OUT_DIR="images"
mkdir -p "$OUT_DIR"

OUT_TAR="$OUT_DIR/$(echo "$SRC_IMAGE" | tr '/:' '_').tar"

rm -f "$OUT_TAR"
sudo microk8s ctr images pull --platform linux/amd64 "$SRC_IMAGE"
sudo microk8s ctr images export "$OUT_TAR" "$SRC_IMAGE"

echo "✅ Saved to $OUT_TAR"
