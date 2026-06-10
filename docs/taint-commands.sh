#!/usr/bin/env bash
# -------------------------------------------------------
# Node labelling commands for vLLM pod scheduling.
#
# Uses a unified "avoid=vllm" label that works for ALL
# vLLM deployment modes:
#   - Single-model, Multi-model, Data Parallel (40-vllm-unified.yaml)
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

kubectl label node node1 avoid=vllm
kubectl label node node2 avoid=vllm
kubectl label node node5 avoid=vllm
kubectl label node node6 avoid=vllm
kubectl label node node7 avoid=vllm
kubectl label node node8 avoid=vllm

kubectl label node node1 avoid-
kubectl label node node2 avoid-
kubectl label node node5 avoid-
kubectl label node node6 avoid-
kubectl label node node7 avoid-
kubectl label node node8 avoid-
