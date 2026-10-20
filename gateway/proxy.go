// Package gateway is the Gatekeeper reverse proxy: a middleware chain of
// request-ID, API-key auth and rate limiting in front of upstreams chosen
// by consistent hashing.
package gateway

import (
	"crypto/rand"
	"encoding/hex"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"time"

	"github.com/instinctsetup/gatekeeper/hash"
	"github.com/instinctsetup/gatekeeper/limiter"
)

type Config struct {
	APIKeys  map[string]string // key -> client name; empty = auth disabled
	Limiter  limiter.Limiter
	KeyFunc  func(*http.Request) string // which identity gets rate limited
	Ring     *hash.Ring
	Stats    *Stats
	FailOpen bool // limiter error: allow (true) or deny (false)
}

type Gateway struct {
	cfg     Config
	proxies sync.Map // upstream -> *httputil.ReverseProxy
}

func New(cfg Config) *Gateway {
	if cfg.KeyFunc == nil {
		cfg.KeyFunc = func(r *http.Request) string {
			if k := r.Header.Get("X-Api-Key"); k != "" {
				return k
			}
			return r.RemoteAddr
		}
	}
	if cfg.Ring == nil {
		cfg.Ring = hash.New()
	}
	if cfg.Stats == nil {
		cfg.Stats = NewStats()
	}
	return &Gateway{cfg: cfg}
}

func requestID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (g *Gateway) proxyFor(upstream string) (*httputil.ReverseProxy, error) {
	if p, ok := g.proxies.Load(upstream); ok {
		return p.(*httputil.ReverseProxy), nil
	}
	target, err := url.Parse(upstream)
	if err != nil {
		return nil, err
	}
	p := httputil.NewSingleHostReverseProxy(target)
	g.proxies.Store(upstream, p)
	return p, nil
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(code int) {
	w.code = code
	w.ResponseWriter.WriteHeader(code)
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	id := requestID()
	w.Header().Set("X-Request-Id", id)

	// middleware 1: API-key auth
	if len(g.cfg.APIKeys) > 0 {
		key := r.Header.Get("X-Api-Key")
		if _, ok := g.cfg.APIKeys[key]; !ok {
			http.Error(w, `{"error":"missing or invalid API key"}`, http.StatusUnauthorized)
			g.record(r, false, "", start, http.StatusUnauthorized)
			return
		}
	}

	// middleware 2: rate limit
	key := g.cfg.KeyFunc(r)
	decision, err := g.cfg.Limiter.Allow(r.Context(), key, 1)
	if err != nil {
		log.Printf("limiter error: %v (fail-open=%v)", err, g.cfg.FailOpen)
		if !g.cfg.FailOpen {
			http.Error(w, `{"error":"rate limiter unavailable"}`, http.StatusServiceUnavailable)
			g.record(r, false, "", start, http.StatusServiceUnavailable)
			return
		}
		decision = limiter.Decision{Allowed: true, Algorithm: g.cfg.Limiter.Name()}
	}
	if !decision.Allowed {
		w.Header().Set("Retry-After", msToSec(decision.RetryAfter))
		w.Header().Set("X-RateLimit-Limit", itoa(decision.Limit))
		http.Error(w, `{"error":"rate limit exceeded"}`, http.StatusTooManyRequests)
		g.record(r, false, "", start, http.StatusTooManyRequests)
		return
	}
	w.Header().Set("X-RateLimit-Limit", itoa(decision.Limit))
	w.Header().Set("X-RateLimit-Algorithm", decision.Algorithm)

	// route: consistent hashing over healthy upstreams
	upstream := g.cfg.Ring.Get(r.URL.Path + ":" + key)
	if upstream == "" {
		http.Error(w, `{"error":"no upstreams configured"}`, http.StatusBadGateway)
		g.record(r, true, "", start, http.StatusBadGateway)
		return
	}
	proxy, err := g.proxyFor(upstream)
	if err != nil {
		http.Error(w, `{"error":"bad upstream"}`, http.StatusBadGateway)
		return
	}
	sw := &statusWriter{ResponseWriter: w, code: 200}
	proxy.ServeHTTP(sw, r)
	g.record(r, true, upstream, start, sw.code)
}

func (g *Gateway) record(r *http.Request, allowed bool, upstream string, start time.Time, code int) {
	g.cfg.Stats.Record(DecisionEvent{
		At: time.Now(), Key: g.cfg.KeyFunc(r), Path: r.URL.Path,
		Allowed: allowed, Algorithm: g.cfg.Limiter.Name(), Upstream: upstream,
		LatencyMs:  float64(time.Since(start).Microseconds()) / 1000.0,
		StatusCode: code,
	})
}

func msToSec(ms int64) string {
	s := (ms + 999) / 1000
	if s < 1 {
		s = 1
	}
	return itoa64(s)
}

func itoa(i int) string { return itoa64(int64(i)) }
func itoa64(i int64) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [20]byte
	n := len(buf)
	for i > 0 {
		n--
		buf[n] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		n--
		buf[n] = '-'
	}
	return string(buf[n:])
}
