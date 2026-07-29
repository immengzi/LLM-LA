package sidecar

import (
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/vmihailenco/msgpack/v5"
)

type kvEventKind uint8

const (
	eventStored kvEventKind = iota
	eventRemoved
	eventCleared
)

type kvEvent struct {
	kind      kvEventKind
	hashes    []string
	blockSize int
	medium    *string
}

type kvEventBatch struct {
	events []kvEvent
	rank   *int
}

func decodeSequence(raw []byte) (uint64, error) {
	if len(raw) != 8 {
		return 0, fmt.Errorf("sequence frame must be 8 bytes, got %d", len(raw))
	}
	return binary.BigEndian.Uint64(raw), nil
}

func encodeSequence(sequence uint64) []byte {
	raw := make([]byte, 8)
	binary.BigEndian.PutUint64(raw, sequence)
	return raw
}

func decodeBatch(payload []byte, dpSize int) (kvEventBatch, error) {
	var top []msgpack.RawMessage
	if err := msgpack.Unmarshal(payload, &top); err != nil {
		return kvEventBatch{}, fmt.Errorf("malformed msgpack KVEventBatch: %w", err)
	}
	if len(top) != 2 && len(top) != 3 {
		return kvEventBatch{}, fmt.Errorf("KVEventBatch must have 2 or 3 fields, got %d", len(top))
	}
	var rawEvents []msgpack.RawMessage
	if err := msgpack.Unmarshal(top[1], &rawEvents); err != nil {
		return kvEventBatch{}, fmt.Errorf("decode events: %w", err)
	}
	batch := kvEventBatch{events: make([]kvEvent, 0, len(rawEvents))}
	if len(top) == 3 && !isMsgpackNil(top[2]) {
		var rank int
		if err := msgpack.Unmarshal(top[2], &rank); err != nil || rank < 0 || rank >= dpSize {
			return kvEventBatch{}, fmt.Errorf("invalid attn_dp_rank")
		}
		batch.rank = &rank
	}
	for _, raw := range rawEvents {
		event, err := decodeEvent(raw)
		if err != nil {
			return kvEventBatch{}, err
		}
		batch.events = append(batch.events, event)
	}
	return batch, nil
}

func validateAndFilterBatch(cfg *Config, batch kvEventBatch) (kvEventBatch, error) {
	if cfg.InferenceEngine != "sglang" {
		return batch, nil
	}
	filtered := kvEventBatch{rank: batch.rank, events: make([]kvEvent, 0, len(batch.events))}
	for _, event := range batch.events {
		if event.medium != nil && *event.medium != "GPU" {
			continue
		}
		if event.kind == eventStored && event.blockSize != cfg.KVEventExpectedPageSize {
			return kvEventBatch{}, fmt.Errorf("event block_size=%d, expected %d", event.blockSize, cfg.KVEventExpectedPageSize)
		}
		filtered.events = append(filtered.events, event)
	}
	return filtered, nil
}

func decodeEvent(raw msgpack.RawMessage) (kvEvent, error) {
	var fields []msgpack.RawMessage
	if err := msgpack.Unmarshal(raw, &fields); err != nil || len(fields) == 0 {
		return kvEvent{}, fmt.Errorf("malformed KV event")
	}
	var tag string
	if err := msgpack.Unmarshal(fields[0], &tag); err != nil {
		return kvEvent{}, fmt.Errorf("KV event tag is not a string")
	}
	switch tag {
	case "BlockStored":
		if len(fields) != 6 && len(fields) != 7 {
			return kvEvent{}, fmt.Errorf("BlockStored must have 6 or 7 fields")
		}
		var blockSize int
		if err := msgpack.Unmarshal(fields[4], &blockSize); err != nil {
			return kvEvent{}, fmt.Errorf("invalid BlockStored block_size")
		}
		medium, err := decodeOptionalString(fields, 6)
		if err != nil {
			return kvEvent{}, err
		}
		return kvEvent{kind: eventStored, hashes: decodeHashList(fields[1]), blockSize: blockSize, medium: medium}, nil
	case "BlockRemoved":
		if len(fields) != 2 && len(fields) != 3 {
			return kvEvent{}, fmt.Errorf("BlockRemoved must have 2 or 3 fields")
		}
		medium, err := decodeOptionalString(fields, 2)
		if err != nil {
			return kvEvent{}, err
		}
		return kvEvent{kind: eventRemoved, hashes: decodeHashList(fields[1]), medium: medium}, nil
	case "AllBlocksCleared":
		if len(fields) != 1 {
			return kvEvent{}, fmt.Errorf("AllBlocksCleared must have one field")
		}
		return kvEvent{kind: eventCleared}, nil
	default:
		return kvEvent{}, fmt.Errorf("unsupported KV event %q", tag)
	}
}

func decodeOptionalString(fields []msgpack.RawMessage, index int) (*string, error) {
	if len(fields) <= index || isMsgpackNil(fields[index]) {
		return nil, nil
	}
	var value string
	if err := msgpack.Unmarshal(fields[index], &value); err != nil {
		return nil, fmt.Errorf("event medium is not a string")
	}
	return &value, nil
}

func isMsgpackNil(raw []byte) bool { return len(raw) == 1 && raw[0] == 0xc0 }

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
