#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Shared, cluster-agnostic registry + proxy resolution (bz/yz/...).
# Override with REGISTRY=... / PROXY_URL=... / TAG=... if needed.
source "$SCRIPT_DIR/../build-common.sh"

IMAGE="kv-router"

docker build \
  -f Dockerfile \
  "${PROXY_BUILD_ARGS[@]}" \
  -t ${IMAGE}:${TAG} .

docker tag ${IMAGE}:${TAG} ${PUSH_REGISTRY}/${IMAGE}:${TAG}

# push without proxy (PUSH_REGISTRY=localhost:32000 is insecure-allowed on nodes)
env -u http_proxy -u https_proxy -u HTTP_PROXY -u HTTPS_PROXY -u NO_PROXY -u no_proxy \
  docker push ${PUSH_REGISTRY}/${IMAGE}:${TAG}

echo "pushed ${PUSH_REGISTRY}/${IMAGE}:${TAG}  (cluster pulls it as ${REGISTRY}/${IMAGE}:${TAG})"
