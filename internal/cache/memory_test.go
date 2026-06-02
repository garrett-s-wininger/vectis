package cache

import (
	"context"
	"testing"
	"time"
)

func TestMemoryService_CleansStaleRateLimitBuckets(t *testing.T) {
	clock := &testClock{t: time.Now().UTC()}
	cache := newMemoryServiceWithClock(clock)
	ctx := context.Background()
	rule := RateLimitRule{RefillRate: time.Minute, BurstSize: 1}

	if _, err := cache.TakeRateLimitToken(ctx, "stale", rule); err != nil {
		t.Fatalf("TakeRateLimitToken stale: %v", err)
	}

	clock.Add(rateLimitBucketTTL + rateLimitBucketCleanupInterval + time.Second)

	if _, err := cache.TakeRateLimitToken(ctx, "fresh", rule); err != nil {
		t.Fatalf("TakeRateLimitToken fresh: %v", err)
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()
	if _, ok := cache.buckets["stale"]; ok {
		t.Fatal("expected stale rate-limit bucket to be cleaned")
	}

	if _, ok := cache.buckets["fresh"]; !ok {
		t.Fatal("expected fresh rate-limit bucket to remain")
	}
}

func TestMemoryService_CleansExpiredSessions(t *testing.T) {
	clock := &testClock{t: time.Now().UTC()}
	cache := newMemoryServiceWithClock(clock)
	ctx := context.Background()

	if err := cache.CreateSession(ctx, Session{
		TokenHash:     "expired",
		CSRFTokenHash: "csrf-expired",
		LocalUserID:   1,
		ExpiresAt:     clock.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("CreateSession expired: %v", err)
	}

	clock.Add(time.Hour + sessionCleanupInterval + time.Second)

	if err := cache.CreateSession(ctx, Session{
		TokenHash:     "fresh",
		CSRFTokenHash: "csrf-fresh",
		LocalUserID:   1,
		ExpiresAt:     clock.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("CreateSession fresh: %v", err)
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()
	if _, ok := cache.sessions["expired"]; ok {
		t.Fatal("expected expired session to be cleaned")
	}

	if _, ok := cache.sessions["fresh"]; !ok {
		t.Fatal("expected fresh session to remain")
	}
}

func TestMemoryService_ResolveDeletesExpiredSession(t *testing.T) {
	clock := &testClock{t: time.Now().UTC()}
	cache := newMemoryServiceWithClock(clock)
	ctx := context.Background()

	if err := cache.CreateSession(ctx, Session{
		TokenHash:     "expired",
		CSRFTokenHash: "csrf-expired",
		LocalUserID:   1,
		ExpiresAt:     clock.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	clock.Add(2 * time.Hour)

	if _, err := cache.ResolveSession(ctx, "expired", clock.Now(), time.Hour); !IsNotFound(err) {
		t.Fatalf("ResolveSession after expiry err = %v, want not found", err)
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()
	if _, ok := cache.sessions["expired"]; ok {
		t.Fatal("expected expired session lookup to delete session")
	}
}
