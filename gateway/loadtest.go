package gateway

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// LoadTest fires real HTTP traffic at the gateway from several client keys
// so the dashboard shows the limiter working. Server-side, not a benchmark
// tool - for numbers see cmd/bench.
type LoadTest struct {
	GatewayURL string
	client     *http.Client
	running    atomic.Bool
}

func NewLoadTest(gatewayURL string) *LoadTest {
	return &LoadTest{GatewayURL: gatewayURL, client: &http.Client{Timeout: 5 * time.Second}}
}

func (lt *LoadTest) HandleStart(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Clients int `json:"clients"`
		RPS     int `json:"rps"`
		Seconds int `json:"seconds"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, `{"error":"bad json"}`, http.StatusBadRequest)
		return
	}
	if body.Clients <= 0 {
		body.Clients = 3
	}
	if body.RPS <= 0 {
		body.RPS = 20
	}
	if body.Seconds <= 0 || body.Seconds > 60 {
		body.Seconds = 10
	}
	if !lt.running.CompareAndSwap(false, true) {
		http.Error(w, `{"error":"a load test is already running"}`, http.StatusConflict)
		return
	}
	go lt.run(body.Clients, body.RPS, body.Seconds)
	writeJSON(w, map[string]any{"started": true, "clients": body.Clients, "rps": body.RPS, "seconds": body.Seconds})
}

func (lt *LoadTest) run(clients, rps, seconds int) {
	defer lt.running.Store(false)
	var wg sync.WaitGroup
	stop := time.After(time.Duration(seconds) * time.Second)
	interval := time.Second / time.Duration(rps)
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			key := fmt.Sprintf("load-client-%d", c+1)
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			n := 0
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					n++
					req, _ := http.NewRequest("GET", fmt.Sprintf("%s/g/work/%d", lt.GatewayURL, n), nil)
					req.Header.Set("X-Api-Key", key)
					resp, err := lt.client.Do(req)
					if err == nil {
						_, _ = io.Copy(io.Discard, resp.Body)
						resp.Body.Close()
					}
				}
			}
		}(c)
	}
	wg.Wait()
}
