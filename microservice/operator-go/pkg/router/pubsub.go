package router

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	"github.com/go-zeromq/zmq4"
	"github.com/vllmkv/operator/pkg/models"
)

// ResultPublisher publishes completed results over ZMQ PUB.
type ResultPublisher struct {
	mu   sync.Mutex
	pub  zmq4.Socket
	cfg  *Config
	live bool
}

func NewResultPublisher(cfg *Config) *ResultPublisher {
	return &ResultPublisher{cfg: cfg}
}

func (p *ResultPublisher) Start(ctx context.Context) error {
	if !p.cfg.IsAsyncPub() {
		return nil
	}

	pub := zmq4.NewPub(ctx)
	if err := pub.Listen(p.cfg.ResultsZMQBind); err != nil {
		return err
	}

	p.mu.Lock()
	p.pub = pub
	p.live = true
	p.mu.Unlock()

	log.Printf("[zmq-pub] listening on %s topic=%s", p.cfg.ResultsZMQBind, p.cfg.ResultsZMQTopic)
	return nil
}

func (p *ResultPublisher) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pub != nil {
		p.pub.Close()
		p.live = false
	}
}

// Publish sends a result to ZMQ subscribers.
func (p *ResultPublisher) Publish(result *models.Result, runID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.live {
		return
	}

	topic := p.cfg.ResultsZMQTopic
	if runID != "" {
		topic = topic + "." + runID
	} else {
		topic = topic + ".default"
	}

	payload := map[string]interface{}{
		"req_id": result.ReqID,
		"result": result.Result,
	}
	if result.Endpoint != "" {
		payload["endpoint"] = result.Endpoint
	}
	if runID != "" {
		payload["run_id"] = runID
	}

	data, err := json.Marshal(payload)
	if err != nil {
		log.Printf("[zmq-pub] marshal error: %v", err)
		return
	}

	msg := zmq4.NewMsgFrom([]byte(topic), data)
	if err := p.pub.Send(msg); err != nil {
		log.Printf("[zmq-pub] send error: %v", err)
	}
}
