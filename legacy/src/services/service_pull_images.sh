#!/bin/bash

# Pull Ascend vLLM server image
# docker pull quay.io/ascend/vllm-ascend:v0.11.0rc0

# Pull Redis server
docker pull redis:7-alpine

# Pull CPU vLLM base image for hash service
docker pull openeuler/vllm-cpu:latest

# Pull slim Python base image for KV listener
docker pull python:3.10-slim
