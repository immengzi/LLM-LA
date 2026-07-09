#!/bin/bash
# Pull postgres:16-alpine from Docker Hub, tag for the cluster registry, and push.
# Used by the BooM key-affinity benchmarking Helm template (76-boom-bench-postgres.yaml).
#
# Usage:
#   ./push-postgres.sh                    # default: postgres:16-alpine
#   ./push-postgres.sh 15-alpine          # custom tag
#
set -euo pipefail

REGISTRY="${REGISTRY:-reg.local:32000}"
SOURCE_IMAGE="postgres"
SOURCE_TAG="${1:-16-alpine}"
TARGET_IMAGE="postgres"
TARGET_TAG="${SOURCE_TAG}"

FULL_SOURCE="${SOURCE_IMAGE}:${SOURCE_TAG}"
FULL_TARGET="${REGISTRY}/${TARGET_IMAGE}:${TARGET_TAG}"

echo "=== Pulling ${FULL_SOURCE} ==="
docker pull "${FULL_SOURCE}"

echo "=== Tagging as ${FULL_TARGET} ==="
docker tag "${FULL_SOURCE}" "${FULL_TARGET}"

echo "=== Pushing to cluster registry (no proxy) ==="
env -u http_proxy -u https_proxy -u HTTP_PROXY -u HTTPS_PROXY -u NO_PROXY -u no_proxy \
  docker push "${FULL_TARGET}"

echo "=== Done ==="
echo "Image: ${FULL_TARGET}"
echo "Helm template uses: postgres:${TARGET_TAG}"
echo "(global.imageRegistry=${REGISTRY} prefixes automatically)"
