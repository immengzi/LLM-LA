kubectl delete pvc qwen-local-pvc -n vllm
kubectl delete pv qwen-local-pv
helm upgrade vllm ./vllm-kv-stack -n vllm \
    --set modelVolume.create=true \
    --set deploy.vllm=false \
    --set deploy.router=false \
    --set deploy.redis=false \
    --set deploy.cpuHash=false