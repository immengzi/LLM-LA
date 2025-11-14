#!/usr/bin/env bash
set -euo pipefail
export NSYS_CUDA_INSTALL_PATH=/usr/local/cuda-12.1
export CUDA_VISIBLE_DEVICES=0
sudo systemctl stop nvidia-dcgm dcgm-exporter 2>/dev/null || true

./profile_nsys.sh matmul_practice.py
# ./profile_ncu.sh  matmul_practice.py
sleep 30
./profile_nsys.sh resnet_infer.py
# ./profile_ncu.sh  resnet_infer.py
sleep 30
./profile_nsys.sh hf_decode.py
# ./profile_ncu.sh  hf_decode.py
