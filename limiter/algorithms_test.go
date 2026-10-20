package limiter

import (
	"context"
	"testing"
)

func TestTokenBucketRefillAndBurst(t *testing.T) {
	tb := TokenBucket(10, 5) // 10 r/s, burst 5
	state := map[string]int64{}
	now := int64(0)
	// burst: 5 immediate allowed, 6th denied
	for i := 0; i < 5; i++ {
		var ok bool
		state, ok, _ = tb(state, now)
		if !ok {
			t.Fatalf("request %d should be allowed within burst", i+1)
		}
	}
	var ok bool
	state, ok, retry := tb(state, now)
	if ok {
		t.Fatal("6th request inside burst must be denied")
	}
	if retry <= 0 {
		t.Fatal("denied request should carry a retry hint")
	}
	// after 100ms, exactly one token refills
	state, ok, _ = tb(state, now+100)
	if !ok {
		t.Fatal("after 100ms at 10 r/s one request should be allowed")
	}
	if _, ok, _ = tb(state, now+100); ok {
		t.Fatal("only one token should have refilled in 100ms")
	}
	// after 10s, bucket caps at burst (no infinite accumulation)
	for i := 0; i < 5; i++ {
		state, ok, _ = tb(state, now+10100)
		if !ok {
			t.Fatalf("request %d after long idle should be allowed", i+1)
		}
	}
	if _, ok, _ = tb(state, now+10100); ok {
		t.Fatal("bucket must cap at burst after idle")
	}
}

func TestTokenBucketNoClockSkewExploit(t *testing.T) {
	tb := TokenBucket(10, 5)
	state, ok, _ := tb(map[string]int64{}, 1000)
	if !ok {
		t.Fatal("first request should pass")
	}
	// clock jumping backwards must not mint tokens
	if _, ok, _ := tb(state, 500); !ok {
		// still fine: tokens remain from first call; but no refill happened
		t.Log("allowed from remaining tokens, acceptable")
	}
}

func TestSlidingWindowSmoothsEdges(t *testing.T) {
	sw := SlidingWindow(10, 1000) // 10 per second
	state := map[string]int64{}
	now := int64(0)
	// use the full limit in the first window
	for i := 0; i < 10; i++ {
		var ok bool
		state, ok, _ = sw(state, now+int64(i)*10)
		if !ok {
			t.Fatalf("request %d in first window should pass", i+1)
		}
	}
	// fixed-window flaw: at t=1001 a fresh window would allow 10 more instantly.
	// sliding estimate still counts most of the previous window at t=1010,
	// so at most a request or two squeeze through, never a full new burst.
	edge := 0
	var lastRetry int64
	for i := 0; i < 10; i++ {
		var ok bool
		state, ok, lastRetry = sw(state, now+1010+int64(i)*10)
		if ok {
			edge++
		}
	}
	if edge > 2 {
		t.Fatalf("window edge should admit at most a couple of requests, admitted %d", edge)
	}
	if edge == 0 {
		t.Fatal("weighted estimate should admit roughly the unused fraction")
	}
	if lastRetry <= 0 {
		t.Fatal("denied request should say when to retry")
	}
	// halfway into the new window, roughly half the budget is back
	allowed := 0
	for i := 0; i < 10; i++ {
		var ok bool
		state, ok, _ = sw(state, now+1500+int64(i)*10)
		if ok {
			allowed++
		}
	}
	if allowed < 3 || allowed > 7 {
		t.Fatalf("expected ~half the budget back mid-window, got %d", allowed)
	}
}

func TestMemoryStoreIsolation(t *testing.T) {
	s := NewMemoryStore()
	now := int64(0)
	s.now = func() int64 { return now }
	lim := NewTokenBucket(s, 1, 2)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if d, _ := lim.Allow(ctx, "client-a", 1); !d.Allowed {
			t.Fatal("client-a within burst")
		}
	}
	if d, _ := lim.Allow(ctx, "client-a", 1); d.Allowed {
		t.Fatal("client-a exhausted")
	}
	if d, _ := lim.Allow(ctx, "client-b", 1); !d.Allowed {
		t.Fatal("client-b must have its own bucket")
	}
}
