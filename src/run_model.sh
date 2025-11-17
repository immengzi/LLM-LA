docker run --rm --privileged -p 8000:8000 -v /home/saeid/llm-lb/src/tiny-model:/model:ro openeuler/vllm-cpu:latest --model /model --tokenizer /model  --dtype float32 --port 8000
