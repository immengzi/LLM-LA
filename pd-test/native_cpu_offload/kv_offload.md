### start the vllm server ###
./start_offload_server.sh
### Start the kv event subscriber ###
python test_subscriber_final.py
### Start the request sending and the kv event is updated from the kv event subscriber ###
python test_request_sending.py