sudo docker build -f Dockerfile.zmq_subscriber -t kv_cache_event_listener .
sudo docker build -f Dockerfile.arm.cpu -t vllm-arm64:vllm .
sudo docker build -f Dockerfile.check -t vllm-check .
