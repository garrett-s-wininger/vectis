package ratelimit

import (
	"context"
	"testing"
	"time"
)

type testClock struct {
	t time.Time
}

func (c *testClock) Now() time.Time      { return c.t }
func (c *testClock) Add(d time.Duration) { c.t = c.t.Add(d) }

func TestMemoryRateLimiter_Refill(t *testing.T) {
	clock := &testClock{t: time.Now()}
	lim := newMemoryRateLimiterWithClock(clock)
	defer lim.Stop()

	ctx := context.Background()
	key := "test-key"
	rule := Rule{RefillRate: 12 * time.Second, BurstSize: 5}

	// Exhaust the bucket
	for i := 0; i < 5; i++ {
		allowed, _, err := lim.Allow(ctx, key, rule)
		if err != nil {
			t.Fatalf("Allow error: %v", err)
		}
		if !allowed {
			t.Fatalf("request %d should be allowed", i+1)
		}
	}

	// 6th request should be rate limited
	allowed, retryAfter, err := lim.Allow(ctx, key, rule)
	if err != nil {
		t.Fatalf("Allow error: %v", err)
	}
	if allowed {
		t.Fatal("6th request should be rate limited")
	}
	if retryAfter <= 0 {
		t.Fatal("expected positive retry-after")
	}

	// Advance time by refill rate to get 1 token back
	clock.Add(12 * time.Second)

	// Should be allowed now
	allowed, _, err = lim.Allow(ctx, key, rule)
	if err != nil {
		t.Fatalf("Allow error: %v", err)
	}
	if !allowed {
		t.Fatal("should be allowed after refill")
	}

	// Next request should be rate limited again
	allowed, _, err = lim.Allow(ctx, key, rule)
	if err != nil {
		t.Fatalf("Allow error: %v", err)
	}
	if allowed {
		t.Fatal("should be rate limited after consuming refill")
	}
}

func TestMemoryRateLimiter_DifferentKeys(t *testing.T) {
	clock := &testClock{t: time.Now()}
	lim := newMemoryRateLimiterWithClock(clock)
	defer lim.Stop()

	ctx := context.Background()
	rule := Rule{RefillRate: 12 * time.Second, BurstSize: 2}

	// Exhaust key1
	for i := 0; i < 2; i++ {
		allowed, _, _ := lim.Allow(ctx, "key1", rule)
		if !allowed {
			t.Fatalf("key1 request %d should be allowed", i+1)
		}
	}

	// key1 should be rate limited
	allowed, _, _ := lim.Allow(ctx, "key1", rule)
	if allowed {
		t.Fatal("key1 should be rate limited")
	}

	// key2 should still be allowed (independent bucket)
	allowed, _, _ = lim.Allow(ctx, "key2", rule)
	if !allowed {
		t.Fatal("key2 should be allowed")
	}
}

func TestMemoryRateLimiter_Stop(t *testing.T) {
	lim := NewMemoryRateLimiter()
	// Stop should not panic
	lim.Stop()
}
