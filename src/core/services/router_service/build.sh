#!/bin/bash
# Build + push kv-router.
#
#   ./build.sh                              # auto-detect direct vs proxy
#   PROXY_URL='http://user:pass@proxy:8080' ./build.sh
#                                           # force HTTP(S) proxy for the build
#   PROXY_URL='' ./build.sh                 # force no proxy
#   TAG=dev ./build.sh
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Shared, cluster-agnostic registry + proxy resolution.
source "$SCRIPT_DIR/../build-common.sh"

IMAGE="kv-router"

docker_build \
  -f Dockerfile \
  -t ${IMAGE}:${TAG} .

docker tag ${IMAGE}:${TAG} ${PUSH_REGISTRY}/${IMAGE}:${TAG}

# push without proxy (PUSH_REGISTRY=localhost:32000 is insecure-allowed on nodes)
docker_push_noproxy ${PUSH_REGISTRY}/${IMAGE}:${TAG}

echo "pushed ${PUSH_REGISTRY}/${IMAGE}:${TAG}  (cluster pulls it as ${REGISTRY}/${IMAGE}:${TAG})"
