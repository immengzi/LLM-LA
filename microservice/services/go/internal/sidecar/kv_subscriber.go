package sidecar

import (
	"context"
	"log"
)

type KVSubscriber interface {
	Start(ctx context.Context) error
	Stop()
}

type stubKVSubscriber struct{}

func NewKVSubscriber(_ *Config) KVSubscriber {
	log.Println("[kv_subscriber] KV subscriber disabled (no ZMQ) — stub active")
	return &stubKVSubscriber{}
}

func (s *stubKVSubscriber) Start(_ context.Context) error { return nil }
func (s *stubKVSubscriber) Stop()                         {}

// KVEventBatch represents a batch of KV cache events from vLLM.
// Retained for documentation/future ZMQ implementation.
type KVEventBatch struct {
	Ts     float64   `msgpack:"ts"`
	Events []KVEvent `msgpack:"events"`
}

type KVEvent struct {
	Type            string   `msgpack:"type"`
	BlockHashes     []string `msgpack:"block_hashes,omitempty"`
	ParentBlockHash string   `msgpack:"parent_block_hash,omitempty"`
	TokenIDs        []int    `msgpack:"token_ids,omitempty"`
	BlockSize       int      `msgpack:"block_size,omitempty"`
	LoraID          string   `msgpack:"lora_id,omitempty"`
}

// Redis key patterns (for future ZMQ implementation):
//   BlockStored:
//     HSET  {model}:kvblock:{hash}    pod_name ts
//     SADD  {model}:podblocks:{pod}   hash
//     HSET  {model}:kvblocks          hash kvblock:{hash}
//   BlockRemoved:
//     HDEL  {model}:kvblock:{hash}    pod_name
//     SREM  {model}:podblocks:{pod}   hash
//   AllBlocksCleared:
//     iterate {model}:podblocks:{pod}, HDEL each kvblock, DEL podblocks set
