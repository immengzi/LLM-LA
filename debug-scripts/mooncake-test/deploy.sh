#!/bin/bash
# ============================================================
# 部署脚本：Mooncake Master + vLLM 4-replica Workers
# 使用方式：bash deploy.sh
# ============================================================
set -e

NAMESPACE="vllm"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "===> [1/6] 创建 Namespace..."
kubectl create namespace ${NAMESPACE} --dry-run=client -o yaml | kubectl apply -f -

echo "===> [2/6] 部署 Mooncake Master（含 ConfigMap）..."
kubectl apply -f "${SCRIPT_DIR}/mooncake-master.yaml"

echo "===> [3/6] 等待 Mooncake Master Ready..."
kubectl rollout status deployment/mooncake-master -n ${NAMESPACE} --timeout=120s

echo "===> [4/6] 部署 vLLM StatefulSet（含 ConfigMap + Service）..."
kubectl apply -f "${SCRIPT_DIR}/vllm-statefulset.yaml"

echo "===> [5/6] 等待 vLLM Pods Ready（模型加载需要几分钟）..."
kubectl rollout status statefulset/vllm -n ${NAMESPACE} --timeout=600s

echo "===> [6/6] 部署状态检查..."
echo ""
kubectl get pods -n ${NAMESPACE} -o wide
echo ""

# ============================================================
# 验证命令速查
# ============================================================
echo "=========================================="
echo "✅ 部署完成！验证命令："
echo "=========================================="
echo ""
echo "# 1. 查看所有 Pod 状态"
echo "kubectl get pods -n ${NAMESPACE} -o wide"
echo ""
echo "# 2. 查看 Mooncake Master 日志（确认启动成功）"
echo "kubectl logs -n ${NAMESPACE} deployment/mooncake-master -f"
echo ""
echo "# 3. 查看 vLLM worker 日志（以 vllm-0 为例）"
echo "kubectl logs -n ${NAMESPACE} vllm-0 -f"
echo ""
echo "# 4. 检查 Mooncake KV connector 注册情况"
echo "kubectl logs -n ${NAMESPACE} vllm-0 | grep -i 'mooncake\|kv_connector\|AscendStore'"
echo ""
echo "# 5. 检查 HCCL 网络配置是否生效"
echo "kubectl logs -n ${NAMESPACE} vllm-0 | grep -i 'HCCL\|hccl'"
echo ""
echo "# 6. 测试推理端点"
echo "kubectl port-forward svc/vllm-svc 8000:8000 -n ${NAMESPACE} &"
echo "curl http://localhost:8000/v1/models"
echo "curl http://localhost:8000/health"
echo ""
echo "# 7. 查看 Prometheus 指标（含 engine_index）"
echo "curl -s http://localhost:8000/metrics | grep -E 'engine|cache|kv'"
echo ""
echo "# 8. 获取所有 worker Pod IP（用于 llm-lb 路由配置）"
echo "kubectl get pods -n ${NAMESPACE} -l app=vllm \\"
echo "  -o jsonpath='{range .items[*]}{.metadata.name}{\"\t\"}{.status.podIP}{\"\n\"}{end}'"
echo ""
echo "# 9. 查看 mooncake.json 是否正确挂载"
echo "kubectl exec -n ${NAMESPACE} vllm-0 -- cat /workspace/glm5/mooncake.json"
echo ""
echo "# 10. 删除重新部署"
echo "kubectl delete -f vllm-statefulset.yaml && kubectl delete -f mooncake-master.yaml"
