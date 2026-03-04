# start the vllm server #
```bash
./start_offload_server.sh
```
# Start the kv event subscriber #
This script treck the KV event from vllm and report the hash value and location medium (CPU or NPU) of KV cache block
```bash
python test_subscriber_final.py
```
# Start the request sending and the kv event is updated from the kv event subscriber #
```bash
python test_request_sending.py
```