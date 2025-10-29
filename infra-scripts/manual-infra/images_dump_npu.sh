#!/bin/bash
set -e

SRC_IMAGE="${1:-quay.io/ascend/vllm-ascend:v0.10.1rc1}"
# SRC_IMAGE="${1:-busybox}"
OUT_DIR="images"
mkdir -p "$OUT_DIR"

OUT_TAR="$OUT_DIR/$(echo "$SRC_IMAGE" | tr '/:' '_').tar"

rm -f "$OUT_TAR"

echo "🔹 Pulling $SRC_IMAGE ..."
docker pull "$SRC_IMAGE"

echo "🔹 Saving to $OUT_TAR ..."
docker save -o "$OUT_TAR" "$SRC_IMAGE"

echo "✅ Saved to $OUT_TAR"
