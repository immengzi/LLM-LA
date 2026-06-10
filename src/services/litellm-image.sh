#!/bin/bash
set -e

REGISTRY="reg.local:32000"

SOURCE_IMAGE="ghcr.io/berriai/litellm"
SOURCE_TAG="main-stable"

TARGET_IMAGE="litellm"
TARGET_TAG="main-stable"

echo "=== Pulling from upstream ==="
docker pull ${SOURCE_IMAGE}:${SOURCE_TAG}

echo "=== Retagging for cluster registry ==="
docker tag ${SOURCE_IMAGE}:${SOURCE_TAG} ${REGISTRY}/${TARGET_IMAGE}:${TARGET_TAG}

echo "=== Pushing to cluster registry (no proxy) ==="
env -u http_proxy -u https_proxy -u HTTP_PROXY -u HTTPS_PROXY -u NO_PROXY -u no_proxy \
  docker push ${REGISTRY}/${TARGET_IMAGE}:${TARGET_TAG}

echo "=== Done ==="