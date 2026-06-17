#!/usr/bin/env bash
# -------------------------------------------------------
# Node labelling commands for vLLM pod scheduling.
#
# Uses a unified "avoid=vllm" label that works for ALL
# vLLM deployment modes:
#   - Single-model, Multi-model, Data Parallel (40-vllm-unified.yaml)
#
# Nodes labelled "avoid=vllm" will NOT receive any prod vLLM pods.
# Nodes labelled "avoid=vllm-shadow" will NOT receive any shadow vLLM pods.
# -------------------------------------------------------

# 7) TAINT ONLY VLLM PODS
# Prevent prod vLLM pods from scheduling on a specific node:

kubectl label node <node-name> avoid=vllm

# Example: prevent prod vLLM on node1 (control-plane)
# kubectl label node node1 avoid=vllm

# -------------------------------------------------------

# 8) REMOVE TAINT (allow prod vLLM pods again)

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
# Quick check: which nodes can run prod vLLM?
# (nodes WITHOUT the avoid=vllm label)

# kubectl get nodes --show-labels | grep -v avoid=vllm
# -------------------------------------------------------

# =======================================================
# PROD NODE LABELS
# =======================================================
# Nodes that should NOT run prod vLLM:
kubectl label node node3 avoid=vllm --overwrite
kubectl label node node4 avoid=vllm --overwrite
kubectl label node node5 avoid=vllm --overwrite
kubectl label node node6 avoid=vllm --overwrite

# Remove prod exclusion (allow prod vLLM on these nodes):
# kubectl label node node3 avoid-
# kubectl label node node4 avoid-
# kubectl label node node5 avoid-
# kubectl label node node6 avoid-

# =======================================================
# SHADOW DEPLOYMENT LABELS
# =======================================================
# Shadow vLLM avoids nodes with avoid=vllm-shadow.
# Shadow vLLM targets nodes with vllm-pool=shadow via nodeSelector.

# Prevent shadow vLLM from landing on prod nodes:
kubectl label node node1 avoid=vllm-shadow --overwrite
kubectl label node node2 avoid=vllm-shadow --overwrite
kubectl label node node7 avoid=vllm-shadow --overwrite
kubectl label node node8 avoid=vllm-shadow --overwrite

# Mark shadow vLLM target nodes:
kubectl label node node3 vllm-pool=shadow --overwrite
kubectl label node node4 vllm-pool=shadow --overwrite

# -------------------------------------------------------
# LIST ALL LABELS
# -------------------------------------------------------
kubectl get nodes -l avoid=vllm              # prod-excluded: node3,4,5,6
kubectl get nodes -l avoid=vllm-shadow       # shadow-excluded: node1,2,7,8
kubectl get nodes -l vllm-pool=shadow        # shadow vLLM targets: node3,4

# -------------------------------------------------------
# REMOVE SHADOW LABELS (tear down shadow isolation)
# -------------------------------------------------------
# kubectl label node node1 node2 node7 node8 avoid=vllm-shadow-
# kubectl label node node3 node4 vllm-pool-
# kubectl label node node3 avoid-
