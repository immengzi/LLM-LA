package sidecar

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/go-zeromq/zmq4"
)

// KVSubscriber listens for KV-cache events from the vLLM ZMQ publisher
// and registers block ownership in Redis.
type KVSubscriber struct {
	cfg *Config
	rdb *redis.Client
}

func NewKVSubscriber(cfg *Config, rdb *redis.Client) *KVSubscriber {
	return &KVSubscriber{cfg: cfg, rdb: rdb}
}

// Run subscribes to the vLLM ZMQ PUB socket for KV events and writes to Redis.
func (s *KVSubscriber) Run(ctx context.Context) {
	addr := fmt.Sprintf("tcp://%s:%d", s.cfg.VllmHost, s.cfg.VllmSubPort)
	log.Printf("[kv-sub] connecting to %s", addr)

	sub := zmq4.NewSub(ctx)
	defer sub.Close()

	if err := sub.Dial(addr); err != nil {
		log.Printf("[kv-sub] dial error: %v (KV subscriber disabled)", err)
		return
	}

	if err := sub.SetOption(zmq4.OptionSubscribe, ""); err != nil {
		log.Printf("[kv-sub] subscribe error: %v", err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msg, err := sub.Recv()
		if err != nil {
			log.Printf("[kv-sub] recv error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		if len(msg.Frames) < 1 {
			continue
		}

		payload := msg.Frames[len(msg.Frames)-1]
		s.processEvent(ctx, payload)
	}
}

type kvEvent struct {
	BlockHashes []int64 `msgpack:"block_hashes"`
	Event       string  `msgpack:"event"`
}

func (s *KVSubscriber) processEvent(ctx context.Context, data []byte) {
	var ev kvEvent
	if err := msgpack.Unmarshal(data, &ev); err != nil {
		log.Printf("[kv-sub] unmarshal error: %v", err)
		return
	}

	pipe := s.rdb.Pipeline()
	endpoint := s.cfg.ContainerName

	for _, hash := range ev.BlockHashes {
		key := fmt.Sprintf("%s:kvblock:%s", s.cfg.ModelNameRedis, strconv.FormatInt(hash, 10))
		if ev.Event == "evict" {
			pipe.HDel(ctx, key, endpoint)
		} else {
			pipe.HSet(ctx, key, endpoint, "1")
			pipe.Expire(ctx, key, 5*time.Minute)
		}
	}

	if _, err := pipe.Exec(ctx); err != nil {
		log.Printf("[kv-sub] redis pipeline error: %v", err)
	}
}
