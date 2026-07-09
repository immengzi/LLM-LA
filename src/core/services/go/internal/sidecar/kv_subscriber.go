package sidecar

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-zeromq/zmq4"
	"github.com/vmihailenco/msgpack/v5"
)

// KVSubscriber subscribes to vLLM KV-cache events over ZMQ and mirrors block
// ownership into Redis. It is a faithful port of
// src/core/services/sidecar/sidecar/zmq_subscriber.py.
//
// vLLM publishes msgpack-encoded, msgspec array_like tagged structs:
//
//	payload = [ ts (float), events ]
//	event   = [ tag (str), ...fields ]   where tag is "BlockStored" |
//	          "BlockRemoved" | "AllBlocksCleared"
//
//	BlockStored:      ["BlockStored", block_hashes, parent_block_hash,
//	                   token_ids, block_size, lora_id]
//	BlockRemoved:     ["BlockRemoved", block_hashes]
//	AllBlocksCleared: ["AllBlocksCleared"]
//
// Wire frames from the publisher: [topic, seq_bytes, payload]; we subscribe to
// the "kv@" topic prefix.
type KVSubscriber interface {
	Start(ctx context.Context) error
	Stop()
}

type zmqKVSubscriber struct {
	cfg    *Config
	rdb    *redis.Client
	sock   zmq4.Socket
	cancel context.CancelFunc
	done   chan struct{}
}

func NewKVSubscriber(cfg *Config) KVSubscriber {
	rdb := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d", cfg.RedisHost, cfg.RedisPort),
	})
	return &zmqKVSubscriber{
		cfg:  cfg,
		rdb:  rdb,
		done: make(chan struct{}),
	}
}

func (s *zmqKVSubscriber) Start(ctx context.Context) error {
	cctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	sub := zmq4.NewSub(cctx)
	addr := fmt.Sprintf("tcp://%s:%d", s.cfg.VLLMHost, s.cfg.VLLMSubPort)
	if err := sub.Dial(addr); err != nil {
		cancel()
		return err
	}
	if err := sub.SetOption(zmq4.OptionSubscribe, "kv@"); err != nil {
		cancel()
		return err
	}
	s.sock = sub
	log.Printf("[KV-SUB] started (host=%s, port=%d, pod=%s, model=%s)",
		s.cfg.VLLMHost, s.cfg.VLLMSubPort, s.cfg.ContainerName, s.cfg.ModelNameRedis)

	go s.loop(cctx)
	return nil
}

func (s *zmqKVSubscriber) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
	if s.sock != nil {
		_ = s.sock.Close()
	}
	select {
	case <-s.done:
	case <-time.After(2 * time.Second):
	}
	_ = s.rdb.Close()
	log.Println("[KV-SUB] stopped")
}

func (s *zmqKVSubscriber) loop(ctx context.Context) {
	defer close(s.done)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		msg, err := s.sock.Recv()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			log.Printf("[KV-SUB] ZMQ recv error: %v", err)
			time.Sleep(time.Second)
			continue
		}
		// Frames: [topic, seq_bytes, payload]
		if len(msg.Frames) != 3 {
			log.Printf("[KV-SUB] unexpected frame count: %d (expected 3)", len(msg.Frames))
			continue
		}
		payload := msg.Frames[2]
		if err := s.handlePayload(ctx, payload); err != nil {
			log.Printf("[KV-SUB] error handling KV batch: %v", err)
		}
	}
}

func (s *zmqKVSubscriber) handlePayload(ctx context.Context, payload []byte) error {
	// Decode top-level array: [ts, events].
	var top []msgpack.RawMessage
	if err := msgpack.Unmarshal(payload, &top); err != nil {
		return fmt.Errorf("decode error (msgpack KVEventBatch): %w", err)
	}
	if len(top) < 2 {
		return nil
	}
	var events []msgpack.RawMessage
	if err := msgpack.Unmarshal(top[1], &events); err != nil {
		return fmt.Errorf("decode events: %w", err)
	}

	prefix := ""
	if s.cfg.ModelNameRedis != "" {
		prefix = s.cfg.ModelNameRedis + ":"
	}
	kvblocksKey := prefix + "kvblocks"
	podblocksKey := fmt.Sprintf("%spodblocks:%s", prefix, s.cfg.ContainerName)
	pod := s.cfg.ContainerName
	ts := strconv.FormatInt(time.Now().Unix(), 10)

	pipe := s.rdb.Pipeline()

	for _, evRaw := range events {
		var ev []msgpack.RawMessage
		if err := msgpack.Unmarshal(evRaw, &ev); err != nil || len(ev) == 0 {
			continue
		}
		var tag string
		if err := msgpack.Unmarshal(ev[0], &tag); err != nil {
			continue
		}
		switch tag {
		case "BlockStored":
			if len(ev) < 2 {
				continue
			}
			for _, bh := range decodeHashList(ev[1]) {
				kvblockKey := prefix + "kvblock:" + bh
				pipe.HSet(ctx, kvblockKey, pod, ts)
				pipe.SAdd(ctx, podblocksKey, bh)
				pipe.HSet(ctx, kvblocksKey, bh, kvblockKey)
			}
		case "BlockRemoved":
			if len(ev) < 2 {
				continue
			}
			for _, bh := range decodeHashList(ev[1]) {
				kvblockKey := prefix + "kvblock:" + bh
				pipe.HDel(ctx, kvblockKey, pod)
				pipe.SRem(ctx, podblocksKey, bh)
			}
		case "AllBlocksCleared":
			members, err := s.rdb.SMembers(ctx, podblocksKey).Result()
			if err == nil {
				for _, bh := range members {
					kvblockKey := prefix + "kvblock:" + bh
					pipe.HDel(ctx, kvblockKey, pod)
				}
			}
			pipe.Del(ctx, podblocksKey)
		}
	}

	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return fmt.Errorf("redis error: %w", err)
	}
	return nil
}

// decodeHashList decodes a msgpack list of block hashes (ints, possibly
// exceeding int64) into canonical decimal strings.
func decodeHashList(raw msgpack.RawMessage) []string {
	var nums []msgpack.RawMessage
	if err := msgpack.Unmarshal(raw, &nums); err != nil {
		return nil
	}
	out := make([]string, 0, len(nums))
	for _, n := range nums {
		out = append(out, decodeIntString(n))
	}
	return out
}

// decodeIntString decodes a single msgpack integer into its decimal string,
// handling both signed (int64) and unsigned (uint64) encodings.
func decodeIntString(raw msgpack.RawMessage) string {
	var u uint64
	if err := msgpack.Unmarshal(raw, &u); err == nil {
		return strconv.FormatUint(u, 10)
	}
	var i int64
	if err := msgpack.Unmarshal(raw, &i); err == nil {
		return strconv.FormatInt(i, 10)
	}
	return ""
}
