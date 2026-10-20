package limiter

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis token bucket: same math as TokenBucket(), executed atomically in Lua
// so many gateway replicas can share one limiter state.
var tokenBucketLua = redis.NewScript(`
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local now = tonumber(ARGV[#ARGV])
local scale = 1000

local data = redis.call('HMGET', key, 'tokens', 'last')
local tokens = tonumber(data[1])
local last = tonumber(data[2])
if tokens == nil then
  tokens = burst * scale
  last = now
end
local elapsed = now - last
if elapsed < 0 then elapsed = 0 end
tokens = tokens + elapsed * rate * scale / 1000
if tokens > burst * scale then tokens = burst * scale end
local allowed = 0
if tokens >= scale then
  tokens = tokens - scale
  allowed = 1
end
local retry = 0
if allowed == 0 and rate > 0 then
  retry = math.floor((scale - tokens) * 1000 / (rate * scale)) + 1
end
redis.call('HMSET', key, 'tokens', tokens, 'last', now)
redis.call('PEXPIRE', key, math.max(60000, math.floor(burst / math.max(rate,1) * 2000)))
return {allowed, retry}
`)

var slidingWindowLua = redis.NewScript(`
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local now = tonumber(ARGV[#ARGV])

local data = redis.call('HMGET', key, 'curr', 'prev', 'start')
local curr = tonumber(data[1])
local prev = tonumber(data[2])
local start = tonumber(data[3])
if start == nil or now - start >= 2 * window then
  curr, prev, start = 0, 0, now
elseif now - start >= window then
  curr, prev, start = 0, curr, start + window
end
local elapsed = now - start
local weighted = curr + math.floor(prev * (window - elapsed) / window)
local allowed = 0
if weighted < limit then
  curr = curr + 1
  allowed = 1
end
local retry = 0
if allowed == 0 then retry = window - elapsed end
redis.call('HMSET', key, 'curr', curr, 'prev', prev, 'start', start)
redis.call('PEXPIRE', key, 3 * window)
return {allowed, retry}
`)

// RedisLimiter shares limiter state across gateway replicas via Redis.
type RedisLimiter struct {
	name   string
	client *redis.Client
	script *redis.Script
	args   func() []any
	limit  int
	now    func() int64 // injectable clock (tests); defaults to wall time
}

func (r *RedisLimiter) Name() string { return r.name }

func (r *RedisLimiter) Allow(ctx context.Context, key string, _ int) (Decision, error) {
	args := append(r.args(), r.now())
	res, err := r.script.Run(ctx, r.client, []string{"gk:" + r.name + ":" + key}, args...).Int64Slice()
	if err != nil {
		return Decision{}, err
	}
	return Decision{Allowed: res[0] == 1, Algorithm: r.name, Key: key,
		Limit: r.limit, RetryAfter: res[1]}, nil
}

func NewRedisTokenBucket(client *redis.Client, ratePerSec, burst int) Limiter {
	return &RedisLimiter{"token-bucket", client, tokenBucketLua,
		func() []any { return []any{ratePerSec, burst} }, ratePerSec,
		func() int64 { return time.Now().UnixMilli() }}
}

func NewRedisSlidingWindow(client *redis.Client, limit int, windowMs int64) Limiter {
	return &RedisLimiter{"sliding-window", client, slidingWindowLua,
		func() []any { return []any{limit, windowMs} }, limit,
		func() int64 { return time.Now().UnixMilli() }}
}
