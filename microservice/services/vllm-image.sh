#!/bin/bash
set -e

# REGISTRY="7.242.102.243:32000"
# REGISTRY="localhost:32000"
REGISTRY="reg.local:32000"

SOURCE_IMAGE="quay.io/ascend/vllm-ascend"
SOURCE_TAG="v0.11.0rc0"

TARGET_IMAGE="ascend/vllm-ascend"
TARGET_TAG="v0.11.0rc0"

echo "=== Pulling from upstream ==="
docker pull ${SOURCE_IMAGE}:${SOURCE_TAG}

echo "=== Retagging for cluster registry ==="
docker tag ${SOURCE_IMAGE}:${SOURCE_TAG} ${REGISTRY}/${TARGET_IMAGE}:${TARGET_TAG}

echo "=== Pushing to cluster registry (no proxy) ==="
env -u http_proxy -u https_proxy -u HTTP_PROXY -u HTTPS_PROXY -u NO_PROXY -u no_proxy \
  docker push ${REGISTRY}/${TARGET_IMAGE}:${TARGET_TAG}

echo "=== Done ==="