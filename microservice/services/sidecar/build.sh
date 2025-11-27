#!/bin/bash
set -e

PROXY_URL="http://peulerosweb:EulerOS_123@172.18.100.92:8080"

docker build \
  -f Dockerfile \
  --build-arg HTTP_PROXY="$PROXY_URL" \
  --build-arg HTTPS_PROXY="$PROXY_URL" \
  -t kv-sidecar:latest .
