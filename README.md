# gatekeeper

**A rate-limiting API gateway in Go: token bucket and sliding-window limiters with Redis-atomic shared state, consistent-hashing upstream routing, API-key auth, and a live dashboard that shows every single verdict - allowed or throttled - as it happens.**

The rate limiter is the most-asked system-design interview question; this is a working, benchmarked, tested implementation of the real thing, not a diagram.

## Demo & report

- **Demo video (Drive):** https://drive.google.com/file/d/11XWTQJI0PloGfmgUFW7TrmZAHYApAJcC/view
- **PDF report (Drive):** https://drive.google.com/file/d/1f4dXdjwAsbhK1mo7pQw5C822zlfSaBnc/view
- Dashboard screenshot: [`docs/dashboard.png`](docs/dashboard.png)

## What it does

| Capability | Where | The systems concept |
|---|---|---|
| Token bucket limiter (pure state transitions, virtual-clock testable) | `limiter/algorithms.go` | burst tolerance + sustained rate, refill math |
| Sliding-window counter (weighted previous window) | `limiter/algorithms.go` | smooths the fixed-window edge stampede |
| Sharded in-memory store (64 shards, FNV) | `limiter/memory.go` | lock contention vs memory tradeoff |
| **Redis store: identical algorithms as atomic Lua scripts** | `limiter/redis.go` | many replicas, one shared limiter state |
| Consistent-hashing ring (160 vnodes/node, md5) | `hash/consistent.go` | add/remove a node, only ~1/N of keys move |
| Reverse proxy + middleware chain (request ID, API-key auth, rate limit, Retry-After + X-RateLimit-* headers) | `gateway/proxy.go` | the classic gateway pipeline |
| Live dashboard over SSE: decision stream, per-client bars, throughput chart | `web/` | backpressure-safe fanout (slow dashboards drop events, never block the gateway) |
| One-click built-in load test | `gateway/loadtest.go` | see throttling happen with real traffic |
| Benchmark harness | `cmd/bench/` | the numbers below are measured, not estimated |

## Quick start

```bash
go build -o gatekeeper ./cmd/gatekeeper
./gatekeeper            # starts 3 demo backends + dashboard on :8080
# open http://localhost:8080 and press "Fire load test"
```

With Redis (shared limiter state across replicas):

```bash
./gatekeeper -redis localhost:6379 -algorithm sliding-window -rate 5
```

Route your own services:

```bash
./gatekeeper -upstreams http://api-a:9000,http://api-b:9000
# proxied traffic: http://localhost:8080/g/<path>
```

With Docker: `docker build -t gatekeeper . && docker run -p 8080:8080 gatekeeper`

Run the tests and the benchmark:

```bash
go test -race ./...    # algorithms (virtual clock), Redis parity (miniredis), ring, proxy
go run ./cmd/bench     # re-measures the numbers below on your machine
```

## Measured performance

Latest run of `cmd/bench` (raw record in `docs/benchmarks.json` with date, Go version, CPU count):

- **0.47 µs per limiter decision** (200,000 decisions, sharded in-memory store) - ~2.1M decisions/sec
- **4,391 req/s through the full proxy path** (auth + rate limit + consistent-hash route + reverse proxy), **p99 latency 0.84 ms**, measured over 2,000 sequential requests on one keep-alive connection

Method note, honestly: the end-to-end number is single-connection because the measurement sandbox throttles concurrent loopback connections; `cmd/bench` records its parameters alongside the numbers. The 32-worker load path is exercised for correctness in tests instead.

## Verified behavior (all in the test suite)

- token bucket: burst of exactly `burst`, then steady-state refill; clock going backwards mints no tokens; bucket caps at burst after idle
- sliding window: at the window edge at most a couple of requests squeeze through, never a full second burst
- **Redis and in-memory limiters agree** (tested against miniredis with a fast-forwarded clock)
- consistent hashing: ~33/33/33 distribution over 30k keys; adding a 5th node moves ~20% of keys, never more than 35%
- throttled clients get HTTP 429 + Retry-After + rate-limit headers, and rejected requests never touch the upstream
- per-client isolation: one client burning its budget never affects another
- a slow dashboard subscriber can never block the gateway (dropped events, proven under `-race`)

## Honest engineering notes

- Fail-open by default: if Redis dies, traffic flows and the error is logged (flag to fail-closed). This is the standard gateway choice and it's documented in the config.
- The in-memory store is per-process; use `-redis` the moment you run two replicas.
- API keys map to client names in config; there is no key-management UI on purpose.
