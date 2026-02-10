#!/bin/bash
set -e

# REGISTRY="7.242.102.243:32000"
REGISTRY="localhost:32000"
IMAGE="vllm-cpu-hash"
TAG="latest"

docker build \
  -f Dockerfile \
  -t ${IMAGE}:${TAG} .

docker tag ${IMAGE}:${TAG} ${REGISTRY}/${IMAGE}:${TAG}

# push without proxy
env -u http_proxy -u https_proxy -u HTTP_PROXY -u HTTPS_PROXY -u NO_PROXY -u no_proxy \
  docker push ${REGISTRY}/${IMAGE}:${TAG}
