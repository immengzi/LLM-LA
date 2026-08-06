package sidecar

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/go-zeromq/zmq4"
	"github.com/vmihailenco/msgpack/v5"
)

type fakeProjection struct {
	mu         sync.Mutex
	applied    []kvEventBatch
	clearCount int
	clearErr   error
	pingCount  int
	pingErrAt  int
}

func (p *fakeProjection) Ping(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pingCount++
	if p.pingErrAt > 0 && p.pingCount >= p.pingErrAt {
		return fmt.Errorf("Redis unavailable")
	}
	return nil
}
func (p *fakeProjection) ClearPod(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.clearCount++
	return p.clearErr
}
func (p *fakeProjection) Apply(_ context.Context, batch kvEventBatch) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.applied = append(p.applied, batch)
	return nil
}
func (p *fakeProjection) Close() error { return nil }

type fakeReplay struct {
	available bool
	batches   []replayBatch
	err       error
	starts    []uint64
}

func (r *fakeReplay) Collect(_ context.Context, _ publisherEndpoint, start uint64, _ int) (bool, []replayBatch, error) {
	r.starts = append(r.starts, start)
	return r.available, r.batches, r.err
}

func testSGLangSubscriber(projection *fakeProjection, replay *fakeReplay) *zmqKVSubscriber {
	cfg := &Config{
		InferenceEngine: "sglang", InferenceHost: "engine", KVEventPort: 5557,
		KVEventReplayPort: 5558, KVEventTopic: "topic", KVEventExpectedPageSize: 16,
		DPSize: 2, ContainerName: "pod", ModelNameRedis: "model",
	}
	return newKVSubscriber(cfg, projection, replay)
}

func packedBatch(t *testing.T, events []any, rank ...any) []byte {
	t.Helper()
	if events == nil {
		events = []any{}
	}
	value := []any{123.5, events}
	value = append(value, rank...)
	raw, err := msgpack.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func storedEvent(hashes []uint64, size int, medium ...any) []any {
	event := []any{"BlockStored", hashes, nil, []int{1, 2}, size, nil}
	return append(event, medium...)
}

func removedEvent(hashes []uint64, medium ...any) []any {
	event := []any{"BlockRemoved", hashes}
	return append(event, medium...)
}

func liveFrames(topic string, sequence uint64, payload []byte) [][]byte {
	return [][]byte{[]byte(topic), encodeSequence(sequence), payload}
}

// TestDecodeIntString covers signed, unsigned, and >int64 block hashes.
func TestDecodeIntString(t *testing.T) {
	cases := []struct {
		name string
		val  interface{}
		want string
	}{
		{"small", uint64(42), "42"},
		{"zero", uint64(0), "0"},
		{"max_int64", uint64(9223372036854775807), "9223372036854775807"},
		{"above_int64", uint64(18446744073709551615), "18446744073709551615"},
	}
	for _, c := range cases {
		raw, err := msgpack.Marshal(c.val)
		if err != nil {
			t.Fatalf("%s: marshal: %v", c.name, err)
		}
		if got := decodeIntString(raw); got != c.want {
			t.Fatalf("%s: decodeIntString = %q, want %q", c.name, got, c.want)
		}
	}
}

// TestDecodeHashList verifies a msgpack list of block hashes decodes to
// canonical decimal strings, preserving order and large values.
func TestDecodeHashList(t *testing.T) {
	in := []uint64{1, 2, 9223372036854775808, 18446744073709551615}
	raw, err := msgpack.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := decodeHashList(raw)
	want := []string{"1", "2", "9223372036854775808", "18446744073709551615"}
	if len(got) != len(want) {
		t.Fatalf("decodeHashList len = %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("decodeHashList[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestDecodeHashListInvalid returns nil on non-list input.
func TestDecodeHashListInvalid(t *testing.T) {
	raw, _ := msgpack.Marshal("not-a-list")
	if got := decodeHashList(raw); got != nil {
		t.Fatalf("decodeHashList(non-list) = %v, want nil", got)
	}
}

func TestOfficialFramesDecodeRankAndGPUEvents(t *testing.T) {
	projection := &fakeProjection{}
	sub := testSGLangSubscriber(projection, &fakeReplay{})
	payload := packedBatch(t, []any{
		storedEvent([]uint64{1<<63 + 5}, 16, "GPU"),
		removedEvent([]uint64{7}, "GPU"),
	}, 1)
	if err := sub.processFrames(context.Background(), "tcp://engine:5558", liveFrames("topic", 9, payload)); err != nil {
		t.Fatal(err)
	}
	if sub.lastSequence["tcp://engine:5558"] != 9 || len(projection.applied) != 1 {
		t.Fatalf("sequence/projection mismatch: %v %+v", sub.lastSequence, projection.applied)
	}
	events := projection.applied[0].events
	if len(events) != 2 || events[0].hashes[0] != "9223372036854775813" {
		t.Fatalf("decoded events = %+v", events)
	}
}

func TestSGLangFiltersNonGPUAndRejectsPageMismatch(t *testing.T) {
	projection := &fakeProjection{}
	sub := testSGLangSubscriber(projection, &fakeReplay{})
	payload := packedBatch(t, []any{
		storedEvent([]uint64{1}, 16, "CPU_PINNED"),
		removedEvent([]uint64{2}, "DISK"),
		storedEvent([]uint64{3}, 16, "GPU"),
	})
	if err := sub.processFrames(context.Background(), "endpoint", liveFrames("topic", 0, payload)); err != nil {
		t.Fatal(err)
	}
	if got := projection.applied[0].events; len(got) != 1 || got[0].hashes[0] != "3" {
		t.Fatalf("filtered events = %+v", got)
	}
	bad := packedBatch(t, []any{storedEvent([]uint64{4}, 32, "GPU")})
	if sub.consumeFrames(context.Background(), "other", liveFrames("topic", 0, bad)) {
		t.Fatal("wrong page size should fail closed")
	}
	if projection.clearCount != 1 {
		t.Fatalf("ownership clear count = %d, want 1", projection.clearCount)
	}
}

func TestMalformedFramesFailClosed(t *testing.T) {
	cases := [][][]byte{
		{[]byte("topic"), []byte("payload")},
		liveFrames("wrong", 0, packedBatch(t, nil)),
		{[]byte("topic"), []byte{0}, packedBatch(t, nil)},
		liveFrames("topic", 0, []byte("not-msgpack")),
	}
	for index, frames := range cases {
		projection := &fakeProjection{}
		sub := testSGLangSubscriber(projection, &fakeReplay{})
		sub.setStatus(SubscriberStatus{Ready: true, Healthy: true, Phase: "ready"})
		if sub.consumeFrames(context.Background(), "endpoint", frames) {
			t.Fatalf("case %d unexpectedly succeeded", index)
		}
		if projection.clearCount != 1 || sub.Status().Ready || !sub.Status().FailClosed {
			t.Fatalf("case %d did not fail closed: clears=%d status=%+v", index, projection.clearCount, sub.Status())
		}
	}
}

func TestGapReplayIsPerPublisher(t *testing.T) {
	projection := &fakeProjection{}
	replay := &fakeReplay{available: true, batches: []replayBatch{
		{sequence: 4, batch: kvEventBatch{}},
	}}
	sub := testSGLangSubscriber(projection, replay)
	endpoint := "tcp://engine:5557"
	sub.publishers[endpoint] = publisherEndpoint{eventURL: endpoint, replayURL: "tcp://engine:5558"}
	if err := sub.processFrames(context.Background(), endpoint, liveFrames("topic", 3, packedBatch(t, nil))); err != nil {
		t.Fatal(err)
	}
	if err := sub.processFrames(context.Background(), endpoint, liveFrames("topic", 5, packedBatch(t, nil))); err != nil {
		t.Fatal(err)
	}
	if len(replay.starts) != 1 || replay.starts[0] != 4 || sub.lastSequence[endpoint] != 5 || projection.clearCount != 0 {
		t.Fatalf("gap repair mismatch: starts=%v seq=%v clears=%d", replay.starts, sub.lastSequence, projection.clearCount)
	}
}

func TestGapReplayIgnoresOverrunBeyondTriggeringLiveSequence(t *testing.T) {
	projection := &fakeProjection{}
	replay := &fakeReplay{available: true, batches: []replayBatch{
		{sequence: 4, batch: kvEventBatch{events: []kvEvent{{kind: eventRemoved, hashes: []string{"4"}}}}},
		{sequence: 5, batch: kvEventBatch{events: []kvEvent{{kind: eventRemoved, hashes: []string{"replayed-live"}}}}},
		{sequence: 6, batch: kvEventBatch{events: []kvEvent{{kind: eventRemoved, hashes: []string{"future"}}}}},
	}}
	sub := testSGLangSubscriber(projection, replay)
	endpoint := "tcp://engine:5557"
	sub.publishers[endpoint] = publisherEndpoint{eventURL: endpoint, replayURL: "tcp://engine:5558"}
	if err := sub.processFrames(context.Background(), endpoint, liveFrames("topic", 3, packedBatch(t, nil))); err != nil {
		t.Fatal(err)
	}
	if err := sub.processFrames(context.Background(), endpoint, liveFrames("topic", 5,
		packedBatch(t, []any{removedEvent([]uint64{5}, "GPU")}))); err != nil {
		t.Fatal(err)
	}
	if err := sub.processFrames(context.Background(), endpoint, liveFrames("topic", 6,
		packedBatch(t, []any{removedEvent([]uint64{6}, "GPU")}))); err != nil {
		t.Fatal(err)
	}
	if sub.lastSequence[endpoint] != 6 || projection.clearCount != 0 {
		t.Fatalf("overrun replay corrupted watermark: seq=%d clears=%d", sub.lastSequence[endpoint], projection.clearCount)
	}
	for _, applied := range projection.applied {
		for _, event := range applied.events {
			for _, hash := range event.hashes {
				if hash == "replayed-live" || hash == "future" {
					t.Fatalf("replay beyond live-1 was applied: %+v", projection.applied)
				}
			}
		}
	}
}

func TestGapReplayValidatesRequiredRangeBeforeMutation(t *testing.T) {
	cases := []struct {
		name    string
		batches []replayBatch
	}{
		{
			name: "noncontiguous",
			batches: []replayBatch{
				{sequence: 4, batch: kvEventBatch{}},
				{sequence: 6, batch: kvEventBatch{}},
			},
		},
		{
			name: "malformed batch",
			batches: []replayBatch{
				{sequence: 4, batch: kvEventBatch{events: []kvEvent{{
					kind: eventStored, hashes: []string{"4"}, blockSize: 99,
				}}}},
				{sequence: 5, batch: kvEventBatch{}},
			},
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			projection := &fakeProjection{}
			sub := testSGLangSubscriber(projection, &fakeReplay{available: true, batches: test.batches})
			endpoint := "endpoint"
			sub.publishers[endpoint] = publisherEndpoint{eventURL: endpoint, replayURL: "replay"}
			sub.lastSequence[endpoint] = 3
			err := sub.processFrames(context.Background(), endpoint, liveFrames("topic", 6, packedBatch(t, nil)))
			if err == nil {
				t.Fatal("invalid replay range should require resynchronization")
			}
			if len(projection.applied) != 0 {
				t.Fatalf("invalid replay mutated projection: %+v", projection.applied)
			}
		})
	}
}

func TestIdleRedisFailureFailsSessionClosed(t *testing.T) {
	projection := &fakeProjection{pingErrAt: 2}
	sub := testSGLangSubscriber(projection, &fakeReplay{})
	sub.cfg.KVRedisProbeIntervalS = 0.01
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := sub.runSession(ctx, nil, nil)
	if err == nil {
		t.Fatal("idle Redis outage should terminate the session")
	}
	status := sub.Status()
	if status.Ready || status.Healthy || !status.FailClosed || status.Phase != "failed" {
		t.Fatalf("Redis outage did not fail readiness closed: %+v", status)
	}
	engine := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer engine.Close()
	sub.cfg.InferenceURL = engine.URL
	sub.cfg.InferenceHealthPath = "/health"
	sub.cfg.InferenceHealthTimeoutS = 1
	body, code := HealthResponse(context.Background(), sub.cfg, NewLocalQueue("pod"), nil, sub, NewEngineProber(sub.cfg), true)
	if code != http.StatusServiceUnavailable || body["status"] != "kv_unready" {
		t.Fatalf("readiness stayed open after idle Redis outage: code=%d body=%v", code, body)
	}
}

func TestRestartInvalidatesBeforeResuming(t *testing.T) {
	projection := &fakeProjection{}
	sub := testSGLangSubscriber(projection, &fakeReplay{})
	endpoint := "endpoint"
	if err := sub.processFrames(context.Background(), endpoint, liveFrames("topic", 8, packedBatch(t, nil))); err != nil {
		t.Fatal(err)
	}
	if sub.consumeFrames(context.Background(), endpoint, liveFrames("topic", 0,
		packedBatch(t, []any{storedEvent([]uint64{55}, 16)}))) {
		t.Fatal("restart should require a new session")
	}
	if projection.clearCount != 1 || len(sub.lastSequence) != 0 || len(projection.applied) != 1 {
		t.Fatalf("restart did not invalidate safely: clears=%d seq=%v applied=%d", projection.clearCount, sub.lastSequence, len(projection.applied))
	}
}

func TestDuplicateSequenceZeroPayloadIdentity(t *testing.T) {
	t.Run("exact duplicate is idempotent", func(t *testing.T) {
		projection := &fakeProjection{}
		sub := testSGLangSubscriber(projection, &fakeReplay{})
		endpoint := "endpoint"
		payload := packedBatch(t, []any{storedEvent([]uint64{55}, 16)})
		if err := sub.processFrames(context.Background(), endpoint, liveFrames("topic", 0, payload)); err != nil {
			t.Fatal(err)
		}
		if err := sub.processFrames(context.Background(), endpoint, liveFrames("topic", 0, payload)); err != nil {
			t.Fatal(err)
		}
		if projection.clearCount != 0 || len(projection.applied) != 1 || sub.lastSequence[endpoint] != 0 {
			t.Fatalf("duplicate mutated state: clears=%d applied=%d seq=%v",
				projection.clearCount, len(projection.applied), sub.lastSequence)
		}
	})

	t.Run("changed content forces rebootstrap", func(t *testing.T) {
		projection := &fakeProjection{}
		sub := testSGLangSubscriber(projection, &fakeReplay{})
		endpoint := "endpoint"
		if err := sub.processFrames(context.Background(), endpoint, liveFrames("topic", 0,
			packedBatch(t, []any{storedEvent([]uint64{55}, 16)}))); err != nil {
			t.Fatal(err)
		}
		if sub.consumeFrames(context.Background(), endpoint, liveFrames("topic", 0,
			packedBatch(t, []any{storedEvent([]uint64{99}, 16)}))) {
			t.Fatal("changed sequence-zero payload should require a new session")
		}
		if projection.clearCount != 1 || len(sub.lastSequence) != 0 || len(projection.applied) != 1 {
			t.Fatalf("epoch reset was not fail-closed: clears=%d seq=%v applied=%d",
				projection.clearCount, sub.lastSequence, len(projection.applied))
		}
	})
}

func TestBootstrapWatermarkDetectsReusedSequenceZero(t *testing.T) {
	projection := &fakeProjection{}
	replay := &fakeReplay{available: true, batches: []replayBatch{
		{sequence: 0, batch: kvEventBatch{events: []kvEvent{{kind: eventStored, hashes: []string{"55"}, blockSize: 16}}}},
		{sequence: 1, batch: kvEventBatch{}},
	}}
	sub := testSGLangSubscriber(projection, replay)
	endpoint := "endpoint"
	sub.publishers[endpoint] = publisherEndpoint{eventURL: endpoint, replayURL: "replay"}
	if _, err := sub.bootstrap(context.Background(), endpoint); err != nil {
		t.Fatal(err)
	}
	if sub.consumeFrames(context.Background(), endpoint, liveFrames("topic", 0,
		packedBatch(t, []any{storedEvent([]uint64{99}, 16)}))) {
		t.Fatal("changed sequence zero behind bootstrap watermark should resync")
	}
	if projection.clearCount != 1 || len(sub.lastSequence) != 0 {
		t.Fatalf("watermark epoch reset mismatch: clears=%d seq=%v", projection.clearCount, sub.lastSequence)
	}
}

func TestBootstrapCompleteAndTruncated(t *testing.T) {
	projection := &fakeProjection{}
	replay := &fakeReplay{available: true, batches: []replayBatch{
		{sequence: 0, batch: kvEventBatch{}}, {sequence: 1, batch: kvEventBatch{}},
	}}
	sub := testSGLangSubscriber(projection, replay)
	endpoint := "endpoint"
	sub.publishers[endpoint] = publisherEndpoint{eventURL: endpoint, replayURL: "replay"}
	result, err := sub.bootstrap(context.Background(), endpoint)
	if err != nil || !result.available || !result.complete || *result.lastSequence != 1 || len(projection.applied) != 2 {
		t.Fatalf("complete bootstrap = (%+v, %v), applied=%d", result, err, len(projection.applied))
	}

	projection.applied = nil
	replay.batches = []replayBatch{{sequence: 7, batch: kvEventBatch{}}, {sequence: 8, batch: kvEventBatch{}}}
	result, err = sub.bootstrap(context.Background(), endpoint)
	if err != nil || result.complete || *result.lastSequence != 8 || len(projection.applied) != 0 {
		t.Fatalf("truncated bootstrap = (%+v, %v), applied=%d", result, err, len(projection.applied))
	}
}

func TestDiscoveryValidatesExactSGLangDescriptor(t *testing.T) {
	var descriptor = `{"version":"0.5.15","kv_events":{"publisher":"zmq","endpoint_host":"*","block_size":16,"topic":"topic","endpoint_port_base":5557,"dp_size":2}}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, descriptor)
	}))
	defer server.Close()
	cfg := &Config{InferenceURL: server.URL, InferenceHost: "engine", KVEventPort: 5557,
		KVEventReplayPort: 5558, KVEventExpectedPageSize: 16, DPSize: 2}
	got, err := discoverSGLang(context.Background(), cfg, server.Client(), "topic")
	if err != nil || len(got) != 2 || got[1].eventURL != "tcp://engine:5558" || got[1].replayURL != "tcp://engine:5559" {
		t.Fatalf("discovery = (%+v, %v)", got, err)
	}
	descriptor = `{"version":"0.5.15","kv_events":{"publisher":"zmq","endpoint_host":"*","block_size":32,"topic":"topic","endpoint_port_base":5557,"dp_size":2}}`
	if _, err := discoverSGLang(context.Background(), cfg, server.Client(), "topic"); err == nil {
		t.Fatal("descriptor mismatch should be rejected")
	}
}

func TestDiscoveryEndpointHostAndRankPortPolicy(t *testing.T) {
	var descriptor string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, descriptor)
	}))
	defer server.Close()
	cfg := &Config{
		InferenceURL: server.URL, InferenceHost: "engine.example",
		KVEventPort: 5557, KVEventReplayPort: 65534, KVEventExpectedPageSize: 16, DPSize: 2,
	}
	makeDescriptor := func(host string, port int) string {
		return fmt.Sprintf(`{"version":"0.5.15","kv_events":{"publisher":"zmq","endpoint_host":%q,"block_size":16,"topic":"topic","endpoint_port_base":%d,"dp_size":2}}`,
			host, port)
	}

	descriptor = makeDescriptor("", 5557)
	if _, err := discoverSGLang(context.Background(), cfg, server.Client(), "topic"); err == nil {
		t.Fatal("missing endpoint_host should be rejected")
	}
	for _, wildcard := range []string{"*", "0.0.0.0", "::"} {
		descriptor = makeDescriptor(wildcard, 5557)
		got, err := discoverSGLang(context.Background(), cfg, server.Client(), "topic")
		if err != nil || got[0].eventURL != "tcp://engine.example:5557" {
			t.Fatalf("wildcard %q discovery = (%+v, %v)", wildcard, got, err)
		}
	}
	descriptor = makeDescriptor("engine.example", 5557)
	if _, err := discoverSGLang(context.Background(), cfg, server.Client(), "topic"); err != nil {
		t.Fatalf("matching concrete endpoint_host rejected: %v", err)
	}
	descriptor = makeDescriptor("other.example", 5557)
	if _, err := discoverSGLang(context.Background(), cfg, server.Client(), "topic"); err == nil {
		t.Fatal("unsupported concrete endpoint_host should be rejected")
	}
	descriptor = makeDescriptor("*", 65535)
	if _, err := discoverSGLang(context.Background(), cfg, server.Client(), "topic"); err == nil {
		t.Fatal("event rank port overflow should be rejected")
	}
	cfg.KVEventReplayPort = 65535
	descriptor = makeDescriptor("*", 5557)
	if _, err := discoverSGLang(context.Background(), cfg, server.Client(), "topic"); err == nil {
		t.Fatal("replay rank port overflow should be rejected")
	}
}

func TestVLLMDiscoveryAndTopicRegression(t *testing.T) {
	cfg := &Config{
		InferenceEngine: "vllm", InferenceHost: "engine", KVEventPort: 5557,
		KVEventReplayPort: 5558, DPSize: 1, DPSizeLocal: 1,
		KVEventExpectedPageSize: 16, KVEventTopic: "kv@",
	}
	publishers, pending, err := initialPublishers(context.Background(), cfg, nil, "kv@")
	if err != nil || len(pending) != 0 || len(publishers) != 1 ||
		publishers[0].eventURL != "tcp://engine:5557" {
		t.Fatalf("vLLM discovery regression: publishers=%+v pending=%+v err=%v", publishers, pending, err)
	}
	projection := &fakeProjection{}
	sub := newKVSubscriber(cfg, projection, &fakeReplay{})
	if err := sub.processFrames(context.Background(), publishers[0].eventURL,
		liveFrames("kv@worker", 1, packedBatch(t, nil))); err != nil {
		t.Fatalf("vLLM topic-prefix framing rejected: %v", err)
	}
}

func TestRealZMQPubSubMultipartFraming(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	publisher := zmq4.NewPub(ctx)
	defer publisher.Close()
	if err := publisher.Listen("tcp://127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	endpoint := "tcp://" + publisher.Addr().String()
	projection := &fakeProjection{}
	sub := testSGLangSubscriber(projection, &fakeReplay{})
	messages := make(chan liveMessage, 1)
	if err := sub.attach(ctx, publisherEndpoint{eventURL: endpoint}, messages); err != nil {
		t.Fatal(err)
	}

	frames := liveFrames("topic", 7, packedBatch(t, []any{removedEvent([]uint64{7}, "GPU")}))
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			t.Fatal("timed out receiving real PUB/SUB multipart message")
		case message := <-messages:
			if message.err != nil {
				t.Fatal(message.err)
			}
			if err := sub.processFrames(ctx, message.endpoint, message.frames); err != nil {
				t.Fatal(err)
			}
			if sub.lastSequence[endpoint] != 7 || len(projection.applied) != 1 {
				t.Fatalf("real PUB/SUB frames were not projected: seq=%v applied=%+v",
					sub.lastSequence, projection.applied)
			}
			return
		case <-ticker.C:
			if err := publisher.Send(zmq4.NewMsgFrom(frames...)); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestRealZMQDealerRouterReplayFraming(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	router := zmq4.NewRouter(ctx)
	defer router.Close()
	if err := router.Listen("tcp://127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	endpoint := "tcp://" + router.Addr().String()
	replayPayload := packedBatch(t, nil)
	serverErr := make(chan error, 1)
	go func() {
		request, err := router.Recv()
		if err != nil {
			serverErr <- err
			return
		}
		if len(request.Frames) != 3 {
			serverErr <- fmt.Errorf("router request frame count = %d, want 3", len(request.Frames))
			return
		}
		start, err := decodeSequence(request.Frames[2])
		if err != nil || start != 4 {
			serverErr <- fmt.Errorf("router replay start = %d, err=%v", start, err)
			return
		}
		identity := request.Frames[0]
		if err := router.Send(zmq4.NewMsgFrom(identity, []byte{}, encodeSequence(4), replayPayload)); err != nil {
			serverErr <- err
			return
		}
		serverErr <- router.Send(zmq4.NewMsgFrom(identity, []byte{}, replayEnd, []byte{}))
	}()

	collector := zmqReplayCollector{timeout: 2 * time.Second}
	available, batches, err := collector.Collect(ctx,
		publisherEndpoint{eventURL: endpoint, replayURL: endpoint}, 4, 2)
	if err != nil || !available || len(batches) != 1 || batches[0].sequence != 4 {
		t.Fatalf("real DEALER/ROUTER replay = available=%t batches=%+v err=%v", available, batches, err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestResolveEngineSpecificTopics(t *testing.T) {
	if resolveTopic("vllm", "kv@", "pod", "model") != "kv@" ||
		resolveTopic("sglang", "kv@", "pod", "model") != "kv@pod@model" ||
		resolveTopic("sglang", "configured", "pod", "model") != "configured" {
		t.Fatal("topic resolution mismatch")
	}
}
