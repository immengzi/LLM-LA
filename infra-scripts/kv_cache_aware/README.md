# Structure

kv_cache_aware/
├── docker-compose.yml
├──test/
└── src/
    ├── Dockerfile
    ├── requirements.txt
    └── subscriber.py


# Installation
## Start fresh with the new configuration
docker-compose up --build -d

## Stop and remove the old containers
docker-compose down


# proxy
export http_proxy=http://127.0.0.1:3128
export https_proxy=http://127.0.0.1:3128


# redis 

redis-cli FLUSHALL

``` docker exec -it vllm_redis_db redis-cli ```
127.0.0.1:6379> KEYS *
1) "3145758617836760132"
2) "5399216822010870386"
3) "10585917922193254562"
4) "16504201776199779812"
5) "8891539575463638423"

127.0.0.1:6379> GET 3145758617836760132
"vllm-1"

## Agent listening to KV events for vllm engine running on port 8002 and storing hash value to redis

``` docker logs -f vllm_listener_2 ```
--- Starting Listener for [vllm-2] ---
Connecting to vLLM at: tcp://vllm-2:5557
Connecting to Redis at: redis
Successfully connected to Redis.
Listening for KV cache events on topic 'kv-events' from 'vllm-2'...
[vllm-2] Received event batch at 1761315782.6034305:
  - BlockStored(block_hashes=[14362763363878074650], parent_block_hash=None, token_ids=[9707, 11, 758, 264, 1879, 1380, 20443, 11229, 702, 67228, 3738, 16694, 11, 8232, 374, 17779, 1119, 1378, 47652, 13, 3776, 3108, 13605, 429, 15235, 1265, 2569, 11, 22573, 15024, 323, 21777, 13, 576, 1008, 42346, 429, 21941, 748, 14269, 323, 15659, 28500, 1969, 7146, 304, 2524, 13, 362, 3598, 11004, 47182, 979, 264, 53891, 15235, 11, 12875, 315, 3259, 1181, 1828, 11181, 11, 12033, 311, 8645, 2176, 11067, 13, 44052, 279, 30208, 54767, 429, 89665, 11, 279, 5766, 6398, 11, 323, 279, 41735, 15917, 315, 10693, 12645, 311, 10279, 279, 3853, 315, 2784, 69270, 13, 3555, 8573, 979, 279, 1555, 1948, 3738, 323, 5662, 1501, 1723, 30, 2585, 1558, 279, 15235, 37580, 438, 432, 46210, 504, 1181, 1828, 11181, 30, 3555, 3476, 1558, 279, 15235, 1486, 304], block_size=128, lora_id=None, medium='GPU')
  -> Stored 1 hash->container mappings for 'vllm-2'.


## Test by sending a prompt to vllm-engine running at port 8002

```
curl -v -X POST http://localhost:8002/generate \
  -H "Content-Type: application/json" \
  -d '{
        "prompt": "Dive into the complexities of human communication, focusing on how emotions shape interactions across different circumstances—whether in moments of happiness, loss, or disagreement. Explore the role of non-verbal cues like body language, facial expressions, and eye contact in conveying meaning, and how tone and word choice further influence the message being communicated. Examine how cultural backgrounds, societal norms, and the rise of digital communication impact the way we connect with others. In your exploration, consider how empathy, openness, and vulnerability foster deeper connections. Share personal examples where communication either strengthened or created distance between individuals, and provide strategies for enhancing understanding in these exchanges. Explore the depths of human emotion and connection, examining how people communicate in diverse situations—whether in moments of joy, sorrow, or conflict. How do subtle body language cues, tone of voice, and word choice influence interactions? Consider the impact of cultural differences, social contexts, and technology on these exchanges. In your analysis, discuss how empathy, understanding, and vulnerability can build stronger relationships. Reflect on personal experiences where communication either deepened or hindered a connection, and offer insights into improving these dynamics.",
        "max_tokens": 100,
        "temperature": 0
      }'
```

## Hash computation of a prompt

```python test/vllm_hash_generator.py --prompt "Dive into the complexities of human communication, focusing on how emotions shape interactions across different circumstances—whether in moments of happiness, loss, or disagreement. Explore the role of non-verbal cues like body language, facial expressions, and eye contact in conveying meaning, and how tone and word choice further influence the message being communicated. Examine how cultural backgrounds, societal norms, and the rise of digital communication impact the way we connect with others. In your exploration, consider how empathy, openness, and vulnerability foster deeper connections. Share personal examples where communication either strengthened or created distance between individuals, and provide strategies for enhancing understanding in these exchanges. Explore the depths of human emotion and connection, examining how people communicate in diverse situations—whether in moments of joy, sorrow, or conflict. How do subtle body language cues, tone of voice, and word choice influence interactions? Consider the impact of cultural differences, social contexts, and technology on these exchanges. In your analysis, discuss how empathy, understanding, and vulnerability can build stronger relationships. Reflect on personal experiences where communication either deepened or hindered a connection, and offer insights into improving these dynamics." --tokenizer-path /mnt/storage1/amardeep/qwen-test/```
