package gateway

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	"github.com/go-zeromq/zmq4"
)

// ResultPublisher is the Go analogue of router/pubsub.py ResultPublisher:
// a ZeroMQ PUB socket that broadcasts completed results for the async_pubsub
// transport. Wire format mirrors Python exactly:
//
//	[ topic_bytes, json_bytes ]
//	topic_bytes = "{topic}.{run_id or 'default'}"
//	json_bytes  = compact JSON of the payload
//
// Delivery is best-effort: PUB drops when there are no subscribers.
type ResultPublisher struct {
	cfg   *Config
	mu    sync.Mutex
	sock  zmq4.Socket
	start bool
}

func NewResultPublisher(cfg *Config) (*ResultPublisher, error) {
	return &ResultPublisher{cfg: cfg}, nil
}

func (p *ResultPublisher) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.start {
		return nil
	}
	sock := zmq4.NewPub(context.Background())
	if p.cfg.ResultsZMQHWM > 0 {
		// Best-effort: bound in-memory queueing for slow subscribers.
		_ = sock.SetOption(zmq4.OptionHWM, p.cfg.ResultsZMQHWM)
	}
	if err := sock.Listen(p.cfg.ResultsZMQBind); err != nil {
		return err
	}
	p.sock = sock
	p.start = true
	return nil
}

func (p *ResultPublisher) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.start {
		return
	}
	_ = p.sock.Close()
	p.sock = nil
	p.start = false
}

func (p *ResultPublisher) Publish(payload map[string]interface{}) {
	runID := "default"
	if v, ok := payload["run_id"].(string); ok && v != "" {
		runID = v
	}
	topic := p.cfg.ResultsZMQTopic + "." + runID
	body, err := json.Marshal(payload)
	if err != nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.start || p.sock == nil {
		return
	}
	msg := zmq4.NewMsgFrom([]byte(topic), body)
	if err := p.sock.Send(msg); err != nil {
		log.Printf("[router] pubsub publish failed: %v", err)
	}
}
