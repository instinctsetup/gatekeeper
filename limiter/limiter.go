// Package limiter implements the rate-limiting algorithms behind Gatekeeper.
//
// The algorithms are written as pure state transitions so they can be tested
// against a virtual clock; store adapters (in-memory, Redis) only persist
// state and run the same logic atomically.
package limiter

import "context"

// Decision is the outcome of one rate-limit check.
type Decision struct {
	Allowed    bool   `json:"allowed"`
	Algorithm  string `json:"algorithm"`
	Key        string `json:"key"`
	Limit      int    `json:"limit"`
	Remaining  int    `json:"remaining"`
	RetryAfter int64  `json:"retry_after_ms"`
}

// Limiter decides whether one request for a key may pass.
type Limiter interface {
	Allow(ctx context.Context, key string, cost int) (Decision, error)
	Name() string
}

// Store persists limiter state per key. Implementations must be atomic.
type Store interface {
	// Eval applies fn to the state stored under key, atomically.
	Eval(ctx context.Context, key string, fn func(state map[string]int64, now int64) (map[string]int64, bool, int64)) (bool, int64, error)
	Name() string
}
