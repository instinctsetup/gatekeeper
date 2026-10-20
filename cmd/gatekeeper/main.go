// Gatekeeper: a rate-limiting API gateway with a live decision dashboard.
//
//	gatekeeper -addr :8080 -redis "" -rate 5 -burst 10
//
// With no -upstreams it starts three demo backends so the whole system runs
// self-contained.
package main

import (
	"embed"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/instinctsetup/gatekeeper/gateway"
	"github.com/instinctsetup/gatekeeper/hash"
	"github.com/instinctsetup/gatekeeper/limiter"
)

//go:embed web
var embeddedWeb embed.FS

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	redisAddr := flag.String("redis", "", "redis address (empty = in-memory store)")
	algorithm := flag.String("algorithm", "token-bucket", "token-bucket | sliding-window")
	rate := flag.Int("rate", 5, "requests per second allowed (token bucket rate / sliding window limit)")
	burst := flag.Int("burst", 10, "token bucket burst size")
	upstreams := flag.String("upstreams", "", "comma-separated upstream URLs (empty = start 3 demo backends)")
	demoBackends := flag.Int("demo-backends", 3, "demo backends to start when -upstreams is empty")
	flag.Parse()

	// store
	var store limiter.Store
	var redisClient *redis.Client
	if *redisAddr != "" {
		redisClient = redis.NewClient(&redis.Options{Addr: *redisAddr})
		log.Printf("using Redis at %s for limiter state (shared across replicas)", *redisAddr)
	} else {
		store = limiter.NewMemoryStore()
		log.Printf("using in-memory limiter state (single instance)")
	}

	// limiter
	var lim limiter.Limiter
	switch *algorithm {
	case "token-bucket":
		if redisClient != nil {
			lim = limiter.NewRedisTokenBucket(redisClient, *rate, *burst)
		} else {
			lim = limiter.NewTokenBucket(store, *rate, *burst)
		}
	case "sliding-window":
		if redisClient != nil {
			lim = limiter.NewRedisSlidingWindow(redisClient, *rate, int64(1000))
		} else {
			lim = limiter.NewSlidingWindow(store, *rate, 1000)
		}
	default:
		log.Fatalf("unknown algorithm %q", *algorithm)
	}

	// upstreams
	ring := hash.New()
	ups := []string{}
	if *upstreams != "" {
		ups = strings.Split(*upstreams, ",")
	} else {
		for i := 0; i < *demoBackends; i++ {
			port := 9100 + i
			name := fmt.Sprintf("backend-%d", i+1)
			go func(port int, name string) {
				_ = http.ListenAndServe(fmt.Sprintf(":%d", port), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					fmt.Fprintf(w, `{"served_by":%q,"path":%q}`+"\n", name, r.URL.Path)
				}))
			}(port, name)
			ups = append(ups, fmt.Sprintf("http://127.0.0.1:%d", port))
		}
		time.Sleep(200 * time.Millisecond)
	}
	for _, u := range ups {
		ring.Add(strings.TrimSpace(u))
	}
	log.Printf("upstreams: %v", ring.Members())

	stats := gateway.NewStats()
	gw := gateway.New(gateway.Config{Limiter: lim, Ring: ring, Stats: stats, FailOpen: true})
	admin := gateway.NewAdmin(stats, gw).
		WithLoadTest(gateway.NewLoadTest("http://127.0.0.1" + *addr))

	mux := http.NewServeMux()
	mux.Handle("/api/", admin.Mux())
	mux.Handle("/api/stream", admin.Mux())
	webFS, _ := fs.Sub(embeddedWeb, "web")
	mux.Handle("/", http.FileServer(http.FS(webFS)))
	mux.Handle("/g/", http.StripPrefix("/g/", gw)) // proxied traffic goes through /g/

	log.Printf("gatekeeper dashboard on http://localhost%s  (proxy at /g/*)", *addr)
	log.Fatal(http.ListenAndServe(*addr, mux))
}
