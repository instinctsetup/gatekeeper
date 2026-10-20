package gateway

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/instinctsetup/gatekeeper/hash"
	"github.com/instinctsetup/gatekeeper/limiter"
)

func testUpstream(t *testing.T, hits *int64) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(hits, 1)
		fmt.Fprintln(w, "hello from upstream")
	}))
}

func newGateway(keys map[string]string, lim limiter.Limiter, ring *hash.Ring, stats *Stats) *Gateway {
	return New(Config{APIKeys: keys, Limiter: lim, Ring: ring, Stats: stats})
}

func TestAuthBlocksUnknownKeys(t *testing.T) {
	var hits int64
	up := testUpstream(t, &hits)
	defer up.Close()
	ring := hash.New()
	ring.Add(up.URL)
	gw := newGateway(map[string]string{"secret": "alice"},
		limiter.NewTokenBucket(limiter.NewMemoryStore(), 100, 100), ring, NewStats())

	req := httptest.NewRequest("GET", "/data", nil)
	rec := httptest.NewRecorder()
	gw.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("no key should be 401, got %d", rec.Code)
	}
	if atomic.LoadInt64(&hits) != 0 {
		t.Fatal("rejected request must never reach the upstream")
	}

	req = httptest.NewRequest("GET", "/data", nil)
	req.Header.Set("X-Api-Key", "secret")
	rec = httptest.NewRecorder()
	gw.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("valid key should pass, got %d", rec.Code)
	}
}

func TestRateLimitEnforcedWithHeaders(t *testing.T) {
	var hits int64
	up := testUpstream(t, &hits)
	defer up.Close()
	ring := hash.New()
	ring.Add(up.URL)
	gw := newGateway(nil, limiter.NewTokenBucket(limiter.NewMemoryStore(), 5, 3), ring, NewStats())

	codes := []int{}
	for i := 0; i < 6; i++ {
		req := httptest.NewRequest("GET", "/x", nil)
		req.Header.Set("X-Api-Key", "client-1")
		req.RemoteAddr = "1.2.3.4:999"
		rec := httptest.NewRecorder()
		gw.ServeHTTP(rec, req)
		codes = append(codes, rec.Code)
		if rec.Code == 429 {
			if rec.Header().Get("Retry-After") == "" {
				t.Fatal("429 must carry Retry-After")
			}
			if rec.Header().Get("X-RateLimit-Limit") != "5" {
				t.Fatal("429 must carry the limit header")
			}
		}
	}
	if codes[0] != 200 || codes[1] != 200 || codes[2] != 200 {
		t.Fatalf("burst of 3 should pass, got %v", codes)
	}
	for _, c := range codes[3:] {
		if c != http.StatusTooManyRequests {
			t.Fatalf("after burst every request should be 429, got %v", codes)
		}
	}
	if atomic.LoadInt64(&hits) != 3 {
		t.Fatalf("only 3 requests should reach the upstream, got %d", hits)
	}
}

func TestPerKeyIsolation(t *testing.T) {
	var hits int64
	up := testUpstream(t, &hits)
	defer up.Close()
	ring := hash.New()
	ring.Add(up.URL)
	gw := newGateway(nil, limiter.NewTokenBucket(limiter.NewMemoryStore(), 1, 1), ring, NewStats())

	fire := func(key string) int {
		req := httptest.NewRequest("GET", "/x", nil)
		req.Header.Set("X-Api-Key", key)
		rec := httptest.NewRecorder()
		gw.ServeHTTP(rec, req)
		return rec.Code
	}
	if fire("a") != 200 || fire("a") != 429 {
		t.Fatal("client a: first passes, second throttled")
	}
	if fire("b") != 200 {
		t.Fatal("client b has its own bucket")
	}
}

func TestStatsRecorded(t *testing.T) {
	var hits int64
	up := testUpstream(t, &hits)
	defer up.Close()
	ring := hash.New()
	ring.Add(up.URL)
	stats := NewStats()
	gw := newGateway(nil, limiter.NewTokenBucket(limiter.NewMemoryStore(), 1, 1), ring, stats)
	for i := 0; i < 4; i++ {
		req := httptest.NewRequest("GET", "/x", nil)
		req.RemoteAddr = "5.6.7.8:1"
		gw.ServeHTTP(httptest.NewRecorder(), req)
	}
	snap := stats.Snapshot()
	if snap.Total != 4 || snap.Allowed != 1 || snap.Denied != 3 {
		t.Fatalf("bad snapshot: %+v", snap)
	}
	admin := NewAdmin(stats, gw)
	rec := httptest.NewRecorder()
	admin.Mux().ServeHTTP(rec, httptest.NewRequest("GET", "/api/stats", nil))
	var decoded Snapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil || decoded.Total != 4 {
		t.Fatalf("admin stats endpoint broken: %v", err)
	}
}

func TestSlowDashboardDoesNotBlockGateway(t *testing.T) {
	stats := NewStats()
	stats.Subscribe() // never drained
	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			stats.Record(DecisionEvent{At: time.Now(), Allowed: true})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("recording blocked on a slow subscriber")
	}
}
