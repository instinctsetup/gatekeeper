package limiter

// ---- token bucket ----
//
// State: tokens (scaled x1000 to stay integer), lastRefill (unix ms).
// A bucket refills at `rate` tokens per second up to `burst`.

const scale = 1000

// TokenBucketState transitions: returns new state, allowed, retryAfterMs.
func TokenBucket(ratePerSec, burst int) func(map[string]int64, int64) (map[string]int64, bool, int64) {
	return func(state map[string]int64, now int64) (map[string]int64, bool, int64) {
		tokens, ok := state["tokens"]
		last := state["last"]
		if !ok {
			tokens = int64(burst) * scale
			last = now
		}
		elapsed := now - last
		if elapsed < 0 {
			elapsed = 0
		}
		tokens += elapsed * int64(ratePerSec) * scale / 1000
		if max := int64(burst) * scale; tokens > max {
			tokens = max
		}
		allowed := tokens >= scale
		if allowed {
			tokens -= scale
		}
		retry := int64(0)
		if !allowed && ratePerSec > 0 {
			retry = (scale - tokens) * 1000 / (int64(ratePerSec) * scale)
			if retry < 1 {
				retry = 1
			}
		}
		return map[string]int64{"tokens": tokens, "last": now}, allowed, retry
	}
}

// ---- sliding window counter ----
//
// State: current window count, previous window count, windowStart (unix ms).
// Weighted estimate = prev*(1 - elapsed/window) + curr; standard Cloudflare-style
// approximation that smooths the burst-at-window-edge problem of fixed windows.

func SlidingWindow(limit int, windowMs int64) func(map[string]int64, int64) (map[string]int64, bool, int64) {
	return func(state map[string]int64, now int64) (map[string]int64, bool, int64) {
		start, ok := state["start"]
		if !ok || now-start >= 2*windowMs {
			state = map[string]int64{"curr": 0, "prev": 0, "start": now}
			start = now
		} else if now-start >= windowMs {
			state = map[string]int64{"curr": 0, "prev": state["curr"], "start": start + windowMs}
		}
		elapsed := now - state["start"]
		weighted := state["curr"] + state["prev"]*(windowMs-elapsed)/windowMs
		allowed := weighted < int64(limit)
		if allowed {
			state["curr"]++
		}
		retry := int64(0)
		if !allowed {
			retry = windowMs - elapsed
		}
		return state, allowed, retry
	}
}
