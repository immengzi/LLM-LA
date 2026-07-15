package gateway

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
)

// affinityStore is the durable backing store consulted by AffinityMap. It is
// deliberately narrow (Get / Put / Warm / Close) so the pull hot path stays in
// memory and only admission (prefetch) and startup (warm) touch it. Mirrors the
// RedisAffinityStore surface used by router/affinity.py.
type affinityStore interface {
	// Get fetches a single mapping (used at admission on an in-memory miss).
	Get(key string) (string, bool)
	// Put queues a write-through upsert (off the hot path; never blocks).
	Put(key, endpoint string)
	// Warm returns the full key -> endpoint mapping for the namespace.
	Warm() map[string]string
	// Close flushes and releases the backend.
	Close()
}

// affinityRedis is the minimal Redis backend RedisAffinityStore needs.
// Injectable so tests can supply an in-memory fake (mirrors the fake redis
// client used by test_affinity_persist.py) without a real Redis.
type affinityRedis interface {
	// get returns (value, found). found=false on miss or error.
	get(key string) (string, bool)
	// setBatch upserts every key with an optional TTL (0 => no expiry).
	setBatch(kvs map[string]string, ttl time.Duration)
	// scanAll returns every key/value whose key starts with matchPrefix.
	scanAll(matchPrefix string) map[string]string
	close()
}

// RedisAffinityStore is a durable affinity backing store with an off-path,
// batched, last-write-wins async writer. Mirrors
// router/affinity_store.py:RedisAffinityStore.
type RedisAffinityStore struct {
	backend affinityRedis
	ns      string
	ttl     time.Duration
	batch   int

	ch      chan [2]string
	stop    chan struct{}
	done    chan struct{}
	dropped int64
}

// NewRedisAffinityStore builds and starts the store's background writer.
// namespace is fully resolved (e.g. "affinity:bz:served-model"); the
// per-conversation key is appended as "{namespace}:{key}".
func NewRedisAffinityStore(backend affinityRedis, namespace string, ttlSeconds, queueMax, batch int) *RedisAffinityStore {
	if queueMax < 1 {
		queueMax = 1
	}
	if batch < 1 {
		batch = 1
	}
	var ttl time.Duration
	if ttlSeconds > 0 {
		ttl = time.Duration(ttlSeconds) * time.Second
	}
	s := &RedisAffinityStore{
		backend: backend,
		ns:      strings.TrimRight(namespace, ":"),
		ttl:     ttl,
		batch:   batch,
		ch:      make(chan [2]string, queueMax),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	go s.writerLoop()
	return s
}

func (s *RedisAffinityStore) rkey(key string) string { return s.ns + ":" + key }

// Put queues a write-through upsert. Never blocks; drops (and counts) when the
// writer queue is full, exactly like the Python store (Redis slow/down).
func (s *RedisAffinityStore) Put(key, endpoint string) {
	if key == "" || endpoint == "" {
		return
	}
	select {
	case s.ch <- [2]string{key, endpoint}:
	default:
		atomic.AddInt64(&s.dropped, 1)
	}
}

func (s *RedisAffinityStore) writerLoop() {
	defer close(s.done)
	for {
		select {
		case <-s.stop:
			return
		case first := <-s.ch:
			// Coalesce a batch: last-write-wins per key within the batch.
			pending := map[string]string{first[0]: first[1]}
		coalesce:
			for i := 0; i < s.batch-1; i++ {
				select {
				case kv := <-s.ch:
					pending[kv[0]] = kv[1]
				default:
					break coalesce
				}
			}
			s.flush(pending)
		}
	}
}

func (s *RedisAffinityStore) flush(pending map[string]string) {
	if len(pending) == 0 {
		return
	}
	rekeyed := make(map[string]string, len(pending))
	for k, ep := range pending {
		rekeyed[s.rkey(k)] = ep
	}
	s.backend.setBatch(rekeyed, s.ttl)
}

// Get fetches a single mapping (used at admission on an in-memory miss).
func (s *RedisAffinityStore) Get(key string) (string, bool) {
	if key == "" {
		return "", false
	}
	return s.backend.get(s.rkey(key))
}

// Warm scans the namespace and returns the full key -> endpoint mapping (keys
// stripped of the namespace prefix). Used once at startup.
func (s *RedisAffinityStore) Warm() map[string]string {
	prefix := s.ns + ":"
	raw := s.backend.scanAll(prefix)
	out := make(map[string]string, len(raw))
	for rk, ep := range raw {
		if strings.HasPrefix(rk, prefix) {
			out[rk[len(prefix):]] = ep
		}
	}
	return out
}

// Close stops the writer, best-effort flushes anything still queued, and
// releases the backend.
func (s *RedisAffinityStore) Close() {
	close(s.stop)
	<-s.done
	pending := map[string]string{}
	for {
		select {
		case kv := <-s.ch:
			pending[kv[0]] = kv[1]
		default:
			if len(pending) > 0 {
				s.flush(pending)
			}
			s.backend.close()
			return
		}
	}
}

// ---------------------------------------------------------------------------
// go-redis backend
// ---------------------------------------------------------------------------

type goRedisAffinity struct {
	rdb *redis.Client
}

func newGoRedisAffinity(cfg *Config) *goRedisAffinity {
	rdb := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d", cfg.RedisHost, cfg.RedisPort),
	})
	return &goRedisAffinity{rdb: rdb}
}

func (g *goRedisAffinity) get(key string) (string, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	v, err := g.rdb.Get(ctx, key).Result()
	if err != nil {
		return "", false
	}
	return v, true
}

func (g *goRedisAffinity) setBatch(kvs map[string]string, ttl time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	pipe := g.rdb.Pipeline()
	for k, v := range kvs {
		pipe.Set(ctx, k, v, ttl) // ttl==0 => no expiry (go-redis semantics)
	}
	_, _ = pipe.Exec(ctx)
}

func (g *goRedisAffinity) scanAll(matchPrefix string) map[string]string {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out := map[string]string{}
	var cursor uint64
	for {
		keys, next, err := g.rdb.Scan(ctx, cursor, matchPrefix+"*", 500).Result()
		if err != nil {
			break
		}
		for _, k := range keys {
			if v, err := g.rdb.Get(ctx, k).Result(); err == nil {
				out[k] = v
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return out
}

func (g *goRedisAffinity) close() { _ = g.rdb.Close() }

// buildAffinityStore constructs a RedisAffinityStore from router config. The
// namespace is "{AFFINITY_REDIS_KEY_PREFIX}:{cluster}:{model}", where cluster
// falls back to the k8s NAMESPACE when CLUSTER is empty. Mirrors
// router/affinity_store.py:build_affinity_store.
func buildAffinityStore(cfg *Config) *RedisAffinityStore {
	cluster := strings.TrimSpace(cfg.Cluster)
	if cluster == "" {
		cluster = strings.TrimSpace(cfg.Namespace)
	}
	if cluster == "" {
		cluster = "default"
	}
	model := strings.TrimSpace(cfg.ModelName)
	if model == "" {
		model = "model"
	}
	prefix := strings.TrimSpace(cfg.AffinityRedisKeyPrefix)
	if prefix == "" {
		prefix = "affinity"
	}
	ns := fmt.Sprintf("%s:%s:%s", prefix, cluster, model)
	return NewRedisAffinityStore(newGoRedisAffinity(cfg), ns, cfg.AffinityRedisTTLSeconds, 100000, 256)
}
