#!/bin/bash
set -e

SRC_IMAGE="${1:-quay.io/ascend/vllm-ascend:v0.11.0rc0}"

OUT_DIR="/mnt/nvme1/saeid/images"
mkdir -p "$OUT_DIR"

OUT_TAR="$OUT_DIR/$(echo "$SRC_IMAGE" | tr '/:' '_').tar"
rm -f "$OUT_TAR"

echo "🔹 Pulling image: $SRC_IMAGE ..."
docker pull "$SRC_IMAGE"

echo "🔹 Saving image to: $OUT_TAR ..."
docker save -o "$OUT_TAR" "$SRC_IMAGE"

echo "✅ Successfully saved: $OUT_TAR"
