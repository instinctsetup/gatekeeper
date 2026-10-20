package limiter

import (
	"context"
	"sync"
	"time"
)

const shards = 64

type shard struct {
	mu   sync.Mutex
	data map[string]map[string]int64
}

// MemoryStore keeps limiter state in process, sharded for low contention.
type MemoryStore struct {
	shards [shards]shard
	now    func() int64 // injectable for tests
}

func NewMemoryStore() *MemoryStore {
	m := &MemoryStore{now: func() int64 { return time.Now().UnixMilli() }}
	for i := range m.shards {
		m.shards[i].data = map[string]map[string]int64{}
	}
	return m
}

func (m *MemoryStore) Name() string { return "memory" }

func (m *MemoryStore) Eval(_ context.Context, key string, fn func(map[string]int64, int64) (map[string]int64, bool, int64)) (bool, int64, error) {
	s := &m.shards[fnv(key)%shards]
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.data[key]
	if !ok {
		state = map[string]int64{}
	}
	next, allowed, retry := fn(state, m.now())
	s.data[key] = next
	return allowed, retry, nil
}

func fnv(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

// ---- limiters built on a Store ----

type storeLimiter struct {
	name  string
	store Store
	fn    func(map[string]int64, int64) (map[string]int64, bool, int64)
	limit int
}

func (l *storeLimiter) Name() string { return l.name }

func (l *storeLimiter) Allow(ctx context.Context, key string, _ int) (Decision, error) {
	allowed, retry, err := l.store.Eval(ctx, key, l.fn)
	if err != nil {
		return Decision{}, err
	}
	return Decision{Allowed: allowed, Algorithm: l.name, Key: key,
		Limit: l.limit, RetryAfter: retry}, nil
}

// NewTokenBucket allows ratePerSec requests per second with bursts up to burst.
func NewTokenBucket(store Store, ratePerSec, burst int) Limiter {
	return &storeLimiter{"token-bucket", store, TokenBucket(ratePerSec, burst), ratePerSec}
}

// NewSlidingWindow allows `limit` requests per windowMs.
func NewSlidingWindow(store Store, limit int, windowMs int64) Limiter {
	return &storeLimiter{"sliding-window", store, SlidingWindow(limit, windowMs), limit}
}
