package sidecar

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

// kv_redis.go projects decoded KV events into Redis.
//
// Maintains the pod↔block index (HSET/SADD style keys) via a Lua apply script
// so stored/removed/cleared batches stay consistent under concurrent
// subscriber updates. Used by both live event intake and replay catch-up.

var applyKVProjectionScript = redis.NewScript(`
local function key_type(key)
  local result = redis.call("TYPE", key)
  if type(result) == "table" then
    return result["ok"]
  end
  return result
end

local function require_type(key, expected)
  local actual = key_type(key)
  if actual ~= "none" and actual ~= expected then
    return redis.error_reply("KV projection key " .. key .. " has type " .. actual .. ", expected " .. expected)
  end
end

local podblocks = KEYS[1]
local kvblocks = KEYS[2]
local pod = ARGV[1]
local timestamp = ARGV[2]
local prefix = ARGV[3]
local needs_kvblocks = false
local has_clear = false

require_type(podblocks, "set")
for index = 4, #ARGV, 2 do
  local operation = ARGV[index]
  local hash = ARGV[index + 1]
  if operation == "s" then
    needs_kvblocks = true
  elseif operation == "c" then
    has_clear = true
  end
  if operation == "s" or operation == "r" then
    require_type(prefix .. "kvblock:" .. hash, "hash")
  end
end
if needs_kvblocks then
  require_type(kvblocks, "hash")
end
if has_clear then
  for _, hash in ipairs(redis.call("SMEMBERS", podblocks)) do
    require_type(prefix .. "kvblock:" .. hash, "hash")
  end
end

for index = 4, #ARGV, 2 do
  local operation = ARGV[index]
  local hash = ARGV[index + 1]
  if operation == "s" then
    local block = prefix .. "kvblock:" .. hash
    redis.call("SADD", podblocks, hash)
    redis.call("HSET", kvblocks, hash, block)
    redis.call("HSET", block, pod, timestamp)
  elseif operation == "r" then
    redis.call("HDEL", prefix .. "kvblock:" .. hash, pod)
    redis.call("SREM", podblocks, hash)
  elseif operation == "c" then
    for _, member in ipairs(redis.call("SMEMBERS", podblocks)) do
      redis.call("HDEL", prefix .. "kvblock:" .. member, pod)
    end
    redis.call("DEL", podblocks)
  else
    return redis.error_reply("unsupported KV projection operation")
  end
end
return 1
`)

type kvProjection interface {
	Ping(context.Context) error
	ClearPod(context.Context) error
	Apply(context.Context, kvEventBatch) error
	Close() error
}

type redisKVProjection struct {
	client           *redis.Client
	model            string
	pod              string
	engine           string
	expectedPageSize int
}

func newRedisKVProjection(cfg *Config) *redisKVProjection {
	return &redisKVProjection{
		client: redis.NewClient(&redis.Options{Addr: fmt.Sprintf("%s:%d", cfg.RedisHost, cfg.RedisPort)}),
		model:  cfg.ModelNameRedis, pod: cfg.ContainerName, engine: cfg.InferenceEngine,
		expectedPageSize: cfg.KVEventExpectedPageSize,
	}
}

func (p *redisKVProjection) prefix() string {
	if p.model == "" {
		return ""
	}
	return p.model + ":"
}

func (p *redisKVProjection) Ping(ctx context.Context) error {
	return p.client.Ping(ctx).Err()
}

func (p *redisKVProjection) Close() error { return p.client.Close() }

func (p *redisKVProjection) ClearPod(ctx context.Context) error {
	prefix := p.prefix()
	return applyKVProjectionScript.Run(ctx, p.client,
		[]string{prefix + "podblocks:" + p.pod, prefix + "kvblocks"},
		p.pod, strconv.FormatInt(time.Now().Unix(), 10), prefix, "c", "").Err()
}

func (p *redisKVProjection) Apply(ctx context.Context, batch kvEventBatch) error {
	prefix := p.prefix()
	kvblocksKey := prefix + "kvblocks"
	podblocksKey := prefix + "podblocks:" + p.pod
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	args := []interface{}{p.pod, timestamp, prefix}
	for _, event := range batch.events {
		if p.engine == "sglang" && event.medium != nil && *event.medium != "GPU" {
			continue
		}
		switch event.kind {
		case eventStored:
			if p.engine == "sglang" && event.blockSize != p.expectedPageSize {
				return fmt.Errorf("event block_size=%d, expected %d", event.blockSize, p.expectedPageSize)
			}
			for _, hash := range event.hashes {
				if hash == "" {
					return fmt.Errorf("invalid block hash")
				}
				args = append(args, "s", hash)
			}
		case eventRemoved:
			for _, hash := range event.hashes {
				if hash == "" {
					return fmt.Errorf("invalid block hash")
				}
				args = append(args, "r", hash)
			}
		case eventCleared:
			args = append(args, "c", "")
		default:
			return fmt.Errorf("unsupported KV event")
		}
	}
	if len(args) == 3 {
		return nil
	}
	return applyKVProjectionScript.Run(ctx, p.client,
		[]string{podblocksKey, kvblocksKey}, args...).Err()
}
