// Bench measures Gatekeeper with real numbers and writes docs/benchmarks.json.
//
//	go run ./cmd/bench
//
// Method note: limiter decisions are measured at full speed. The end-to-end
// proxy path is measured sequentially (one keep-alive connection, N requests)
// because the CI sandbox's loopback stack throttles concurrent connections;
// the exact parameters are recorded alongside the numbers.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"sort"
	"time"

	"github.com/instinctsetup/gatekeeper/gateway"
	"github.com/instinctsetup/gatekeeper/hash"
	"github.com/instinctsetup/gatekeeper/limiter"
)

func benchLimiterDecisions() (int, float64) {
	lim := limiter.NewTokenBucket(limiter.NewMemoryStore(), 1_000_000, 1_000_000)
	ctx := context.Background()
	const n = 200_000
	start := time.Now()
	for i := 0; i < n; i++ {
		_, _ = lim.Allow(ctx, fmt.Sprintf("key-%d", i%1000), 1)
	}
	dur := time.Since(start)
	return n, float64(dur.Nanoseconds()) / n / 1000.0
}

func benchProxySequential(n int) (int, float64, float64) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "ok")
	}))
	defer upstream.Close()
	ring := hash.New()
	ring.Add(upstream.URL)
	gw := gateway.New(gateway.Config{
		Limiter: limiter.NewTokenBucket(limiter.NewMemoryStore(), 10_000_000, 10_000_000),
		Ring:    ring, Stats: gateway.NewStats(),
	})
	srv := httptest.NewServer(gw)
	defer srv.Close()
	client := &http.Client{Timeout: 5 * time.Second}
	lat := make([]float64, 0, n)
	start := time.Now()
	ok := 0
	for i := 0; i < n; i++ {
		t0 := time.Now()
		resp, err := client.Get(srv.URL + "/work")
		if err != nil {
			continue
		}
		resp.Body.Close()
		ok++
		lat = append(lat, float64(time.Since(t0).Microseconds())/1000.0)
	}
	dur := time.Since(start)
	sort.Float64s(lat)
	p99 := lat[int(float64(len(lat))*0.99)-1]
	return int(float64(ok) / dur.Seconds()), p99, dur.Seconds()
}

func main() {
	n, us := benchLimiterDecisions()
	fmt.Printf("limiter decisions: %d in-process, %.2f µs per decision\n", n, us)

	const reqs = 2000
	rps, p99, secs := benchProxySequential(reqs)
	fmt.Printf("proxy path: %d req/s, p99 %.2f ms (%d sequential requests over one keep-alive connection)\n", rps, p99, reqs)

	out := map[string]any{
		"measured_at":       time.Now().UTC().Format("2006-01-02 15:04 UTC"),
		"go_version":        runtime.Version(),
		"cpu_threads":       runtime.NumCPU(),
		"limiter_decisions": n,
		"us_per_decision":   float64(int(us*100)) / 100,
		"proxy_req_per_s":   rps,
		"proxy_p99_ms":      float64(int(p99*100)) / 100,
		"proxy_requests":    reqs,
		"proxy_wall_s":      float64(int(secs*100)) / 100,
		"proxy_concurrency": 1,
		"method_note":       "proxy path measured sequentially (one keep-alive connection); the measurement sandbox throttles concurrent loopback connections",
	}
	_ = os.MkdirAll("docs", 0o755)
	b, _ := json.MarshalIndent(out, "", "  ")
	_ = os.WriteFile("docs/benchmarks.json", b, 0o644)
	fmt.Println("wrote docs/benchmarks.json")
}
