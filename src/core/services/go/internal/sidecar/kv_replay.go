package sidecar

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	"github.com/go-zeromq/zmq4"
)

var replayEnd = bytes.Repeat([]byte{0xff}, 8)

type replayBatch struct {
	sequence    uint64
	batch       kvEventBatch
	identity    [sha256.Size]byte
	hasIdentity bool
}

type replayCollector interface {
	Collect(context.Context, publisherEndpoint, uint64, int) (bool, []replayBatch, error)
}

type zmqReplayCollector struct {
	timeout time.Duration
}

func (c zmqReplayCollector) Collect(parent context.Context, publisher publisherEndpoint, start uint64, dpSize int) (bool, []replayBatch, error) {
	ctx, cancel := context.WithTimeout(parent, c.timeout)
	defer cancel()
	dealer := zmq4.NewDealer(ctx)
	defer dealer.Close()
	if err := dealer.Dial(publisher.replayURL); err != nil {
		return false, nil, err
	}
	if err := dealer.Send(zmq4.NewMsgFrom([]byte{}, encodeSequence(start))); err != nil {
		return false, nil, err
	}
	var batches []replayBatch
	for {
		reply, err := dealer.Recv()
		if err != nil {
			if ctx.Err() != nil {
				return false, nil, nil
			}
			return false, nil, err
		}
		frames := reply.Frames
		if len(frames) == 3 && len(frames[0]) == 0 {
			frames = frames[1:]
		}
		if len(frames) != 2 {
			return false, nil, fmt.Errorf("invalid replay frame count: %d", len(reply.Frames))
		}
		if bytes.Equal(frames[0], replayEnd) {
			return true, batches, nil
		}
		sequence, err := decodeSequence(frames[0])
		if err != nil {
			return false, nil, err
		}
		batch, err := decodeBatch(frames[1], dpSize)
		if err != nil {
			return false, nil, err
		}
		identity, err := normalizedPayloadIdentity(frames[1])
		if err != nil {
			return false, nil, err
		}
		batches = append(batches, replayBatch{
			sequence: sequence, batch: batch, identity: identity, hasIdentity: true,
		})
	}
}
