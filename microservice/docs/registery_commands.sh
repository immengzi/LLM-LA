# list all repositories in the registry
curl http://10.175.113.44:32000/v2/_catalog

# list tags for redis
curl http://10.175.113.44:32000/v2/redis/tags/list

# list tags for kv-router
curl http://10.175.113.44:32000/v2/kv-router/tags/list

# list tags for kv-sidecar
curl http://10.175.113.44:32000/v2/kv-sidecar/tags/list

# list tags for vllm-cpu-hash
curl http://10.175.113.44:32000/v2/vllm-cpu-hash/tags/list

# list tags for vllm-ascend (namespaced repo)
curl http://10.175.113.44:32000/v2/ascend/vllm-ascend/tags/list

# quick registry health check
curl http://10.175.113.44:32000/v2/