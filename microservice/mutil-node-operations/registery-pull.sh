#!/bin/bash
set -e

# REGISTRY=7.242.102.243:32000
# REGISTRY=localhost:32000
REGISTRY=reg.local:32000


# redis
docker pull ${REGISTRY}/redis:7-alpine

# vllm-ascend (mirrored from quay.io)
docker pull ${REGISTRY}/ascend/vllm-ascend:v0.11.0rc0

# vllm-ascend (mirrored from quay.io)
docker pull ${REGISTRY}/ascend/quay.io/ascend/vllm-ascend:glm5-openeuler

# kv-router
docker pull ${REGISTRY}/kv-router:latest

# kv-sidecar
docker pull ${REGISTRY}/kv-sidecar:latest

# vllm-cpu-hash
docker pull ${REGISTRY}/vllm-cpu-hash:latest


# containerd part
ctr -n k8s.io images pull --plain-http ${REGISTRY}/redis:7-alpine
ctr -n k8s.io images pull --plain-http ${REGISTRY}/ascend/vllm-ascend:v0.11.0rc0
ctr -n k8s.io images pull --plain-http ${REGISTRY}/ascend/quay.io/ascend/vllm-ascend:glm5-openeuler
ctr -n k8s.io images pull --plain-http ${REGISTRY}/kv-router:latest
ctr -n k8s.io images pull --plain-http ${REGISTRY}/kv-sidecar:latest
ctr -n k8s.io images pull --plain-http ${REGISTRY}/vllm-cpu-hash:latest