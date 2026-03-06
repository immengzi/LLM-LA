#!/bin/bash
set -e

PROXY_URL="http://peulerosweb:EulerOS_123@172.18.100.92:8080"
# REGISTRY="7.242.102.243:32000"
# REGISTRY="localhost:32000"
REGISTRY="reg.local:32000"
IMAGE="kv-router"
TAG="latest"

docker build \
  -f Dockerfile \
  --build-arg HTTP_PROXY="$PROXY_URL" \
  --build-arg HTTPS_PROXY="$PROXY_URL" \
  -t ${IMAGE}:${TAG} .

docker tag ${IMAGE}:${TAG} ${REGISTRY}/${IMAGE}:${TAG}

# push without proxy
env -u http_proxy -u https_proxy -u HTTP_PROXY -u HTTPS_PROXY -u NO_PROXY -u no_proxy \
  docker push ${REGISTRY}/${IMAGE}:${TAG}
