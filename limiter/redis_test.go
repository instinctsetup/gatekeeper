package limiter

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func setupRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	return mr, redis.NewClient(&redis.Options{Addr: mr.Addr()})
}

func TestRedisTokenBucketMatchesMemorySemantics(t *testing.T) {
	mr, client := setupRedis(t)
	ctx := context.Background()
	lim := NewRedisTokenBucket(client, 10, 5)
	clock := time.Now()
	lim.(*RedisLimiter).now = func() int64 { return clock.UnixMilli() }
	for i := 0; i < 5; i++ {
		if d, err := lim.Allow(ctx, "k", 1); err != nil || !d.Allowed {
			t.Fatalf("burst request %d: %v %v", i+1, d, err)
		}
	}
	if d, _ := lim.Allow(ctx, "k", 1); d.Allowed {
		t.Fatal("6th request must be denied")
	}
	clock = clock.Add(200 * time.Millisecond)
	mr.FastForward(200 * time.Millisecond)
	if d, _ := lim.Allow(ctx, "k", 1); !d.Allowed {
		t.Fatal("after 200ms at 10 r/s two tokens refilled, request should pass")
	}
}

func TestRedisSlidingWindow(t *testing.T) {
	mr, client := setupRedis(t)
	ctx := context.Background()
	lim := NewRedisSlidingWindow(client, 3, 1000)
	clock := time.Now()
	lim.(*RedisLimiter).now = func() int64 { return clock.UnixMilli() }
	for i := 0; i < 3; i++ {
		if d, _ := lim.Allow(ctx, "k", 1); !d.Allowed {
			t.Fatalf("request %d within limit", i+1)
		}
	}
	if d, _ := lim.Allow(ctx, "k", 1); d.Allowed {
		t.Fatal("4th request over limit must be denied")
	}
	clock = clock.Add(2 * time.Second)
	mr.FastForward(2 * time.Second)
	if d, _ := lim.Allow(ctx, "k", 1); !d.Allowed {
		t.Fatal("after two full windows the budget must be back")
	}
}
