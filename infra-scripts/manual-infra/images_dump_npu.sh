#!/bin/bash
set -e

SRC_IMAGE="${1:-quay.io/ascend/vllm-ascend:v0.10.1rc1}"
OUT_DIR="images"
mkdir -p "$OUT_DIR"

OUT_TAR="$OUT_DIR/$(echo "$SRC_IMAGE" | tr '/:' '_').tar"

# Remove any existing tar file
rm -f "$OUT_TAR"

# Pull the image using Docker
docker pull "$SRC_IMAGE"

# Save the image to a tarball
docker save "$SRC_IMAGE" -o "$OUT_TAR"

echo "✅ Saved to $OUT_TAR"
