#!/usr/bin/env bash
# -------------------------------------------------------
# Node labelling commands for vLLM pod scheduling.
#
# Uses a unified "avoid=vllm" label that works for ALL
# vLLM deployment modes:
#   - Single-model  (40-vllm.yaml)
#   - Multi-model   (41-vllm-multi.yaml)
#   - Data Parallel  (43-vllm-lws.yaml)
#
# Nodes labelled "avoid=vllm" will NOT receive any vLLM pods.
# -------------------------------------------------------

# 7) TAINT ONLY VLLM PODS
# Prevent vLLM pods from scheduling on a specific node:

kubectl label node <node-name> avoid=vllm

# Example: prevent vLLM on node1 (control-plane)
# kubectl label node node1 avoid=vllm

# -------------------------------------------------------

# 8) REMOVE TAINT (allow vLLM pods again)

kubectl label node <node-name> avoid-

# -------------------------------------------------------

# 9) LIST NODES WITH THE TAINT

kubectl get nodes -l avoid=vllm

# -------------------------------------------------------

# 10) MIGRATE FROM OLD LABEL (avoid=vllm-qwen)
# If you previously used "avoid=vllm-qwen", update to the
# unified label:

# kubectl label node <node-name> avoid=vllm --overwrite

# Or remove old and set new:
# kubectl label node <node-name> avoid-
# kubectl label node <node-name> avoid=vllm

# -------------------------------------------------------

# Look for:
# - taints
# - tolerations
# - nodeSelector
# - nodeAffinity
# - insufficient CPU/memory

# -------------------------------------------------------
# Quick check: which nodes can run vLLM?
# (nodes WITHOUT the avoid=vllm label)

# kubectl get nodes --show-labels | grep -v avoid=vllm
# -------------------------------------------------------
