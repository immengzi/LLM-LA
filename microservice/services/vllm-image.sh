#!/bin/bash
set -e

# REGISTRY="7.242.102.243:32000"
REGISTRY="localhost:32000"
SOURCE_IMAGE="quay.io/ascend/vllm-ascend"
SOURCE_TAG="v0.11.0rc0"
TARGET_IMAGE="vllm-ascend"
TARGET_TAG="v0.11.0rc0"

# pull from quay
docker pull ${SOURCE_IMAGE}:${SOURCE_TAG}

# retag for local registry
docker tag ${SOURCE_IMAGE}:${SOURCE_TAG} ${REGISTRY}/${TARGET_IMAGE}:${TARGET_TAG}

# push without proxy
env -u http_proxy -u https_proxy -u HTTP_PROXY -u HTTPS_PROXY -u NO_PROXY -u no_proxy \
  docker push ${REGISTRY}/${TARGET_IMAGE}:${TARGET_TAG}
