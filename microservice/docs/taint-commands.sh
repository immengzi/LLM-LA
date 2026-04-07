#!/usr/bin/env bash
# ============================================================
# Kubernetes Taints – Quick Operations Cheat Sheet
# All commands below are ready to copy/paste.
# Comments explain what each command does.
# ============================================================

# ------------------------------------------------------------
# 1) LIST TAINTS
# ------------------------------------------------------------

# Show taints for all nodes (fast overview)
kubectl get nodes -o custom-columns=NAME:.metadata.name,TAINTS:.spec.taints

# Show taints for a specific node (human readable)
kubectl describe node <node-name> | sed -n '/Taints:/,/^$/p'

# Show taints for a specific node (exact JSON output)
kubectl get node <node-name> -o jsonpath='{.spec.taints}{"\n"}'


# ------------------------------------------------------------
# 2) ADD A TAINT
# ------------------------------------------------------------
# General format:
# kubectl taint nodes <node> <key>=<value>:<effect>
#
# Effects:
#   NoSchedule        -> New pods will NOT schedule
#   PreferNoSchedule  -> Avoid scheduling if possible
#   NoExecute         -> Evicts existing pods (unless tolerated)

# Example: Only allow redis pods (with toleration) on worker1
kubectl taint nodes worker1 dedicated=redis:NoSchedule

# Example: Mark node for maintenance (evict pods)
kubectl taint nodes worker1 maintenance=true:NoExecute


# ------------------------------------------------------------
# 2.1) TAINT AN ENTIRE NODE (BLOCK EVERYTHING)
# ------------------------------------------------------------
# This is what you asked for: taint a node so NOTHING schedules on it.

# Most common: hard block scheduling
kubectl taint nodes <node-name> blocked=true:NoSchedule

# If you also want to evict all currently running pods:
kubectl taint nodes <node-name> blocked=true:NoExecute

# If you want to temporarily cordon + taint (common maintenance flow):
kubectl cordon <node-name>
kubectl taint nodes <node-name> blocked=true:NoSchedule

# To taint ALL nodes in the cluster:
kubectl taint nodes --all blocked=true:NoSchedule

# To taint ALL nodes with eviction:
kubectl taint nodes --all blocked=true:NoExecute


# ------------------------------------------------------------
# 3) REMOVE A TAINT
# ------------------------------------------------------------
# IMPORTANT: Add a trailing "-" to remove
# General format:
# kubectl taint nodes <node> <key>[=<value>]:<effect>-

# Example: Remove redis restriction
kubectl taint nodes worker1 dedicated=redis:NoSchedule-

# Example: Remove maintenance taint
kubectl taint nodes worker1 maintenance=true:NoExecute-

# Remove the "block everything" taint
kubectl taint nodes <node-name> blocked=true:NoSchedule-
kubectl taint nodes <node-name> blocked=true:NoExecute-

# Remove that taint from ALL nodes
kubectl taint nodes --all blocked=true:NoSchedule-


# ------------------------------------------------------------
# 4) CLUSTER-WIDE OPERATIONS (ALL NODES)
# ------------------------------------------------------------

# Remove control-plane taint so workloads can run on master nodes
kubectl taint nodes --all node-role.kubernetes.io/control-plane:NoSchedule-
kubectl taint nodes --all node-role.kubernetes.io/master:NoSchedule-

# Remove ALL taints from ALL nodes (⚠️ use carefully)
kubectl get nodes -o name | xargs -I{} kubectl patch {} -p '{"spec":{"taints":[]}}'


# ------------------------------------------------------------
# 5) VERIFY AFTER CHANGES
# ------------------------------------------------------------

# Confirm taints are removed or updated
kubectl get nodes -o custom-columns=NAME:.metadata.name,TAINTS:.spec.taints


# ------------------------------------------------------------
# 6) DEBUGGING: WHY A POD DID NOT SCHEDULE
# ------------------------------------------------------------

# Check pod scheduling events
kubectl describe pod <pod-name> -n <namespace> | sed -n '/Events:/,$p'

# ------------------------------------------------------------
# 7) TAINT ONLY VLLM PODS
# ------------------------------------------------------------

kubectl label node <node-name> avoid=vllm-qwen

# ------------------------------------------------------------
# 8) REMOVE TAINT ONLY VLLM PODS
# ------------------------------------------------------------

kubectl label node <node-name> avoid-

# ------------------------------------------------------------
# 8) LIST THE TAINTS
# ------------------------------------------------------------

kubectl get nodes -l avoid=vllm-qwen

# Look for:
# - taints
# - tolerations
# - nodeSelector
# - nodeAffinity
# - insufficient CPU/memory
# ============================================================
