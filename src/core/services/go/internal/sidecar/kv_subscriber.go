package sidecar

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/go-zeromq/zmq4"
	"github.com/vmihailenco/msgpack/v5"
)

type SubscriberStatus struct {
	Ready           bool   `json:"ready"`
	Healthy         bool   `json:"healthy"`
	FailClosed      bool   `json:"fail_closed"`
	Phase           string `json:"phase"`
	Detail          string `json:"detail"`
	CacheVisibility string `json:"cache_visibility"`
}

type KVSubscriber interface {
	Start(context.Context) error
	Stop()
	Status() SubscriberStatus
	Ready() bool
}

type streamResyncRequired struct{ error }

type liveMessage struct {
	endpoint string
	frames   [][]byte
	err      error
}

type zmqKVSubscriber struct {
	cfg        *Config
	projection kvProjection
	replay     replayCollector
	topic      string

	statusMu sync.RWMutex
	status   SubscriberStatus
	cancel   context.CancelFunc
	done     chan struct{}
	stopOnce sync.Once

	lastSequence        map[string]uint64
	lastPayloadIdentity map[string][sha256.Size]byte
	epochZeroIdentity   map[string][sha256.Size]byte
	bootstrapWatermark  map[string]uint64
	publishers          map[string]publisherEndpoint
}

func NewKVSubscriber(cfg *Config) KVSubscriber {
	return newKVSubscriber(cfg, newRedisKVProjection(cfg),
		zmqReplayCollector{timeout: time.Duration(cfg.KVEventDiscoveryTimeoutS * float64(time.Second))})
}

func newKVSubscriber(cfg *Config, projection kvProjection, replay replayCollector) *zmqKVSubscriber {
	return &zmqKVSubscriber{
		cfg: cfg, projection: projection, replay: replay,
		topic: resolveTopic(cfg.InferenceEngine, cfg.KVEventTopic, cfg.ContainerName, cfg.ModelNameRedis),
		done:  make(chan struct{}), lastSequence: map[string]uint64{},
		lastPayloadIdentity: map[string][sha256.Size]byte{}, epochZeroIdentity: map[string][sha256.Size]byte{},
		bootstrapWatermark: map[string]uint64{}, publishers: map[string]publisherEndpoint{},
		status: SubscriberStatus{FailClosed: true, Phase: "stopped", CacheVisibility: "none"},
	}
}

func (s *zmqKVSubscriber) setStatus(status SubscriberStatus) {
	s.statusMu.Lock()
	s.status = status
	s.statusMu.Unlock()
}

func (s *zmqKVSubscriber) Status() SubscriberStatus {
	s.statusMu.RLock()
	defer s.statusMu.RUnlock()
	return s.status
}

func (s *zmqKVSubscriber) Ready() bool { return s.Status().Ready }

func (s *zmqKVSubscriber) Start(parent context.Context) error {
	if s.cancel != nil {
		return nil
	}
	ctx, cancel := context.WithCancel(parent)
	s.cancel = cancel
	s.setStatus(SubscriberStatus{FailClosed: true, Phase: "starting", CacheVisibility: "none"})
	go s.loop(ctx)
	return nil
}

func (s *zmqKVSubscriber) Stop() {
	s.stopOnce.Do(func() {
		if s.cancel != nil {
			s.cancel()
			<-s.done
		}
		clearCtx, clearCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer clearCancel()
		if err := s.invalidate(clearCtx, "subscriber shutdown"); err != nil {
			log.Printf("[KV-SUB] shutdown ownership clear failed: %v", err)
		}
		_ = s.projection.Close()
		s.setStatus(SubscriberStatus{FailClosed: true, Phase: "stopped", CacheVisibility: "none"})
	})
}

func (s *zmqKVSubscriber) loop(ctx context.Context) {
	defer close(s.done)
	for ctx.Err() == nil {
		delay := 250 * time.Millisecond
		for ctx.Err() == nil {
			s.setStatus(SubscriberStatus{FailClosed: true, Phase: "invalidating", Detail: "clearing prior Redis ownership", CacheVisibility: "none"})
			if err := s.invalidate(ctx, "subscriber startup"); err == nil {
				break
			} else {
				s.setStatus(SubscriberStatus{FailClosed: true, Phase: "invalidating", Detail: "Redis invalidation failed: " + err.Error(), CacheVisibility: "none"})
			}
			if waitContext(ctx, delay) {
				break
			}
			delay = minDuration(delay*2, 5*time.Second)
		}
		if ctx.Err() != nil {
			break
		}

		delay = 250 * time.Millisecond
		var publishers []publisherEndpoint
		var pending []pendingEndpoint
		for ctx.Err() == nil {
			s.setStatus(SubscriberStatus{FailClosed: true, Phase: "discovering", Detail: "waiting for KV publisher discovery", CacheVisibility: "none"})
			var err error
			publishers, pending, err = initialPublishers(ctx, s.cfg, discoveryHTTPClient(s.cfg), s.topic)
			if err == nil {
				log.Printf("[KV-SUB] discovered %d publisher endpoint(s)", len(publishers))
				break
			}
			s.setStatus(SubscriberStatus{FailClosed: true, Phase: "discovering", Detail: "KV discovery failed: " + err.Error(), CacheVisibility: "none"})
			if waitContext(ctx, delay) {
				break
			}
			delay = minDuration(delay*2, 5*time.Second)
		}
		if ctx.Err() != nil {
			break
		}
		if err := s.runSession(ctx, publishers, pending); err != nil && ctx.Err() == nil {
			log.Printf("[KV-SUB] session failed: %v", err)
			s.setStatus(SubscriberStatus{FailClosed: true, Phase: "failed", Detail: "KV subscriber session failed: " + err.Error(), CacheVisibility: "none"})
			waitContext(ctx, 250*time.Millisecond)
		}
	}
	s.setStatus(SubscriberStatus{FailClosed: true, Phase: "stopped", CacheVisibility: "none"})
}

func (s *zmqKVSubscriber) runSession(parent context.Context, publishers []publisherEndpoint, pending []pendingEndpoint) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	s.publishers = map[string]publisherEndpoint{}
	messages := make(chan liveMessage, 32)
	s.setStatus(SubscriberStatus{FailClosed: true, Phase: "connecting", CacheVisibility: "none"})
	for _, publisher := range publishers {
		if err := s.attach(ctx, publisher, messages); err != nil {
			return err
		}
	}

	visibility := "full"
	if s.cfg.InferenceEngine == "sglang" {
		s.setStatus(SubscriberStatus{FailClosed: true, Phase: "bootstrapping", CacheVisibility: "none"})
		for _, publisher := range publishers {
			result, err := s.bootstrap(ctx, publisher.eventURL)
			if err != nil {
				return err
			}
			if !result.available {
				visibility = "live_only"
			} else if !result.complete && visibility == "full" {
				visibility = "truncated"
			}
		}
	}
	if err := s.projection.Ping(ctx); err != nil {
		return err
	}
	detail := ""
	if visibility != "full" {
		detail = "replay unavailable or truncated; visibility is conservative"
	}
	s.setStatus(SubscriberStatus{Ready: true, Healthy: true, Phase: "ready", Detail: detail, CacheVisibility: visibility})

	retryTicker := time.NewTicker(10 * time.Second)
	defer retryTicker.Stop()
	redisProbeInterval := s.redisProbeInterval()
	redisTicker := time.NewTicker(redisProbeInterval)
	defer redisTicker.Stop()
	retryCount := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case message := <-messages:
			if message.err != nil {
				return message.err
			}
			if !s.consumeFrames(ctx, message.endpoint, message.frames) {
				return fmt.Errorf("event stream invalidated")
			}
		case <-redisTicker.C:
			probeCtx, probeCancel := context.WithTimeout(ctx, minDuration(redisProbeInterval, 2*time.Second))
			err := s.projection.Ping(probeCtx)
			probeCancel()
			if err != nil {
				s.setStatus(SubscriberStatus{
					FailClosed: true, Phase: "failed",
					Detail: "Redis probe failed: " + err.Error(), CacheVisibility: "none",
				})
				return fmt.Errorf("Redis probe failed: %w", err)
			}
		case <-retryTicker.C:
			if s.cfg.InferenceEngine == "sglang" || len(pending) == 0 || retryCount >= 90 {
				continue
			}
			retryCount++
			stillPending := pending[:0]
			for _, item := range pending {
				ips, err := net.DefaultResolver.LookupHost(ctx, item.host)
				if err != nil || len(ips) == 0 {
					stillPending = append(stillPending, item)
					continue
				}
				publisher := publisherEndpoint{rank: item.rank,
					eventURL:  fmt.Sprintf("tcp://%s:%d", ips[0], item.port),
					replayURL: fmt.Sprintf("tcp://%s:%d", s.cfg.InferenceHost, s.cfg.KVEventReplayPort)}
				if err := s.attach(ctx, publisher, messages); err != nil {
					stillPending = append(stillPending, item)
				}
			}
			pending = stillPending
		}
	}
}

func (s *zmqKVSubscriber) redisProbeInterval() time.Duration {
	seconds := s.cfg.KVRedisProbeIntervalS
	if seconds <= 0 {
		seconds = 1
	}
	if seconds > 5 {
		seconds = 5
	}
	return time.Duration(seconds * float64(time.Second))
}

func (s *zmqKVSubscriber) attach(ctx context.Context, publisher publisherEndpoint, out chan<- liveMessage) error {
	sub := zmq4.NewSub(ctx)
	if err := sub.Dial(publisher.eventURL); err != nil {
		return err
	}
	if err := sub.SetOption(zmq4.OptionSubscribe, s.topic); err != nil {
		sub.Close()
		return err
	}
	s.publishers[publisher.eventURL] = publisher
	go func() {
		defer sub.Close()
		for {
			msg, err := sub.Recv()
			if err != nil {
				if ctx.Err() == nil {
					select {
					case out <- liveMessage{endpoint: publisher.eventURL, err: err}:
					case <-ctx.Done():
					}
				}
				return
			}
			select {
			case out <- liveMessage{endpoint: publisher.eventURL, frames: msg.Frames}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}

func (s *zmqKVSubscriber) consumeFrames(ctx context.Context, endpoint string, frames [][]byte) bool {
	if err := s.processFrames(ctx, endpoint, frames); err != nil {
		s.setStatus(SubscriberStatus{FailClosed: true, Phase: "invalidating", Detail: "event stream error: " + err.Error(), CacheVisibility: "none"})
		var resync streamResyncRequired
		if !errors.As(err, &resync) {
			if clearErr := s.invalidate(ctx, "event stream error: "+err.Error()); clearErr != nil {
				s.setStatus(SubscriberStatus{FailClosed: true, Phase: "invalidating", Detail: "Redis invalidation failed: " + clearErr.Error(), CacheVisibility: "none"})
			}
		}
		return false
	}
	return true
}

func (s *zmqKVSubscriber) processFrames(ctx context.Context, endpoint string, frames [][]byte) error {
	if len(frames) != 3 {
		return fmt.Errorf("unexpected frame count: %d", len(frames))
	}
	topic := string(frames[0])
	matches := topic == s.topic
	if s.cfg.InferenceEngine != "sglang" {
		matches = strings.HasPrefix(topic, s.topic)
	}
	if !matches {
		return fmt.Errorf("unexpected topic %q", topic)
	}
	sequence, err := decodeSequence(frames[1])
	if err != nil {
		return err
	}
	batch, err := decodeBatch(frames[2], s.cfg.DPSize)
	if err != nil {
		return err
	}
	payloadIdentity, err := normalizedPayloadIdentity(frames[2])
	if err != nil {
		return err
	}
	if watermark, ok := s.bootstrapWatermark[endpoint]; ok {
		if sequence <= watermark {
			var known [sha256.Size]byte
			knownExists := false
			if sequence == 0 {
				known, knownExists = s.epochZeroIdentity[endpoint]
			} else if previous, exists := s.lastSequence[endpoint]; exists && sequence == previous {
				known, knownExists = s.lastPayloadIdentity[endpoint]
			}
			if knownExists && payloadIdentity != known {
				return s.publisherEpochChanged(ctx, endpoint, sequence)
			}
			return nil
		}
		delete(s.bootstrapWatermark, endpoint)
	}
	previous, exists := s.lastSequence[endpoint]
	if !exists {
		if err := s.applyBatch(ctx, batch); err != nil {
			return err
		}
		s.lastSequence[endpoint] = sequence
		s.rememberPayload(endpoint, sequence, payloadIdentity)
		return nil
	}
	if sequence == previous {
		if known, ok := s.lastPayloadIdentity[endpoint]; !ok || payloadIdentity != known {
			return s.publisherEpochChanged(ctx, endpoint, sequence)
		}
		return nil
	}
	if sequence < previous {
		reason := fmt.Sprintf("publisher restart at %s: %d -> %d", endpoint, previous, sequence)
		if err := s.invalidate(ctx, reason); err != nil {
			return err
		}
		return streamResyncRequired{fmt.Errorf("%s", reason)}
	}
	if sequence == previous+1 {
		if err := s.applyBatch(ctx, batch); err != nil {
			return err
		}
		s.lastSequence[endpoint] = sequence
		s.rememberPayload(endpoint, sequence, payloadIdentity)
		return nil
	}
	if s.replayGap(ctx, endpoint, previous+1, sequence) {
		current, ok := s.lastSequence[endpoint]
		if ok && current >= sequence {
			return nil
		}
		if ok && current == sequence-1 {
			if err := s.applyBatch(ctx, batch); err != nil {
				return err
			}
			s.lastSequence[endpoint] = sequence
			s.rememberPayload(endpoint, sequence, payloadIdentity)
			return nil
		}
	}
	reason := fmt.Sprintf("unrecoverable sequence gap at %s: %d -> %d", endpoint, previous, sequence)
	if err := s.invalidate(ctx, reason); err != nil {
		return err
	}
	return streamResyncRequired{fmt.Errorf("%s", reason)}
}

type replayResult struct {
	available    bool
	complete     bool
	lastSequence *uint64
}

func (s *zmqKVSubscriber) bootstrap(ctx context.Context, endpoint string) (replayResult, error) {
	publisher, ok := s.publishers[endpoint]
	if !ok || publisher.replayURL == "" {
		return replayResult{}, nil
	}
	available, batches, err := s.replay.Collect(ctx, publisher, 0, s.cfg.DPSize)
	if err != nil {
		return replayResult{}, err
	}
	if !available {
		log.Printf("[KV-SUB] bootstrap replay unavailable endpoint=%s", endpoint)
		return replayResult{}, nil
	}
	if len(batches) == 0 {
		log.Printf("[KV-SUB] bootstrap replay empty endpoint=%s", endpoint)
		return replayResult{available: true, complete: true}, nil
	}
	complete := batches[0].sequence == 0
	for index := 1; index < len(batches); index++ {
		complete = complete && batches[index].sequence == batches[index-1].sequence+1
	}
	last := batches[len(batches)-1].sequence
	if complete {
		for _, replayed := range batches {
			if err := s.applyBatch(ctx, replayed.batch); err != nil {
				return replayResult{}, err
			}
			s.lastSequence[endpoint] = replayed.sequence
			identity, err := replayPayloadIdentity(replayed)
			if err != nil {
				return replayResult{}, err
			}
			s.rememberPayload(endpoint, replayed.sequence, identity)
		}
	} else {
		s.lastSequence[endpoint] = last
		delete(s.lastPayloadIdentity, endpoint)
	}
	s.bootstrapWatermark[endpoint] = last
	log.Printf(
		"[KV-SUB] bootstrap replay endpoint=%s available=%v complete=%v last_sequence=%d batches=%d",
		endpoint, true, complete, last, len(batches),
	)
	return replayResult{available: true, complete: complete, lastSequence: &last}, nil
}

func (s *zmqKVSubscriber) replayGap(ctx context.Context, endpoint string, start, live uint64) bool {
	publisher, ok := s.publishers[endpoint]
	if !ok {
		return false
	}
	available, batches, err := s.replay.Collect(ctx, publisher, start, s.cfg.DPSize)
	if err != nil || !available {
		return false
	}
	target := live - 1
	expected := start
	var validated []kvEventBatch
	var targetIdentity [sha256.Size]byte
	hasTargetIdentity := false
	for _, replayed := range batches {
		if expected > target {
			break
		}
		if replayed.sequence != expected {
			return false
		}
		filtered, err := validateAndFilterBatch(s.cfg, replayed.batch)
		if err != nil {
			return false
		}
		validated = append(validated, filtered)
		if replayed.sequence == target {
			targetIdentity, err = replayPayloadIdentity(replayed)
			if err != nil {
				return false
			}
			hasTargetIdentity = true
		}
		expected++
	}
	if expected <= target {
		return false
	}
	combined := kvEventBatch{}
	for _, batch := range validated {
		combined.events = append(combined.events, batch.events...)
	}
	if err := s.projection.Apply(ctx, combined); err != nil {
		return false
	}
	s.lastSequence[endpoint] = target
	if hasTargetIdentity {
		s.rememberPayload(endpoint, target, targetIdentity)
	}
	return true
}

func (s *zmqKVSubscriber) invalidate(ctx context.Context, reason string) error {
	if err := s.projection.ClearPod(ctx); err != nil {
		return err
	}
	s.lastSequence = map[string]uint64{}
	s.lastPayloadIdentity = map[string][sha256.Size]byte{}
	s.epochZeroIdentity = map[string][sha256.Size]byte{}
	s.bootstrapWatermark = map[string]uint64{}
	log.Printf("[KV-SUB] invalidated pod ownership: %s", reason)
	return nil
}

func (s *zmqKVSubscriber) rememberPayload(endpoint string, sequence uint64, identity [sha256.Size]byte) {
	s.lastPayloadIdentity[endpoint] = identity
	if sequence == 0 {
		s.epochZeroIdentity[endpoint] = identity
	}
}

func (s *zmqKVSubscriber) publisherEpochChanged(ctx context.Context, endpoint string, sequence uint64) error {
	previous, exists := s.lastSequence[endpoint]
	previousText := "none"
	if exists {
		previousText = fmt.Sprintf("%d", previous)
	}
	reason := fmt.Sprintf("publisher epoch changed at %s: sequence %d (previous %s)", endpoint, sequence, previousText)
	if err := s.invalidate(ctx, reason); err != nil {
		return err
	}
	return streamResyncRequired{fmt.Errorf("publisher epoch changed at %s: sequence %d", endpoint, sequence)}
}

func batchIdentity(batch kvEventBatch) ([sha256.Size]byte, error) {
	normalized := make([]any, 0, len(batch.events)+1)
	if batch.rank == nil {
		normalized = append(normalized, nil)
	} else {
		normalized = append(normalized, *batch.rank)
	}
	for _, event := range batch.events {
		var medium any
		if event.medium != nil {
			medium = *event.medium
		}
		normalized = append(normalized, []any{uint8(event.kind), event.hashes, event.blockSize, medium})
	}
	encoded, err := msgpack.Marshal(normalized)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("encode normalized KV batch identity: %w", err)
	}
	return sha256.Sum256(encoded), nil
}

func normalizedPayloadIdentity(payload []byte) ([sha256.Size]byte, error) {
	var normalized any
	if err := msgpack.Unmarshal(payload, &normalized); err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("decode KV payload identity: %w", err)
	}
	encoded, err := msgpack.Marshal(normalized)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("encode normalized KV payload identity: %w", err)
	}
	return sha256.Sum256(encoded), nil
}

func replayPayloadIdentity(replayed replayBatch) ([sha256.Size]byte, error) {
	if replayed.hasIdentity {
		return replayed.identity, nil
	}
	// Unit-test collectors and compatibility implementations may construct
	// replay batches directly; their decoded form remains deterministic.
	return batchIdentity(replayed.batch)
}

func (s *zmqKVSubscriber) applyBatch(ctx context.Context, batch kvEventBatch) error {
	filtered, err := validateAndFilterBatch(s.cfg, batch)
	if err != nil {
		return err
	}
	return s.projection.Apply(ctx, filtered)
}

func waitContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return true
	case <-timer.C:
		return false
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
