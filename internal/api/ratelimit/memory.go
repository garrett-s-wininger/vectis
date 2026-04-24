package ratelimit

import (
	"context"
	"sync"
	"time"
)

const (
	bucketCleanupInterval = 1 * time.Hour
	bucketTTL             = 24 * time.Hour
)

// clock provides time for rate limiting, allowing test overrides.
type clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// bucket represents a token bucket for rate limiting.
type bucket struct {
	tokens       float64
	lastRefill   time.Time
	lastAccessed time.Time
	mu           sync.Mutex
}

// MemoryRateLimiter is an in-memory token bucket rate limiter.
type MemoryRateLimiter struct {
	buckets map[string]*bucket
	mu      sync.RWMutex
	clock   clock
	done    chan struct{}
}

// NewMemoryRateLimiter creates a new in-memory rate limiter.
func NewMemoryRateLimiter() *MemoryRateLimiter {
	return newMemoryRateLimiterWithClock(realClock{})
}

func newMemoryRateLimiterWithClock(c clock) *MemoryRateLimiter {
	m := &MemoryRateLimiter{
		buckets: make(map[string]*bucket),
		clock:   c,
		done:    make(chan struct{}),
	}
	go m.cleanupLoop()
	return m
}

// Stop shuts down the background cleanup goroutine.
func (r *MemoryRateLimiter) Stop() {
	close(r.done)
}

// Allow checks if the request identified by key is within rate limits.
func (r *MemoryRateLimiter) Allow(ctx context.Context, key string, rule Rule) (bool, time.Duration, error) {
	r.mu.Lock()
	b, exists := r.buckets[key]
	if !exists {
		now := r.clock.Now()
		b = &bucket{
			tokens:       float64(rule.BurstSize),
			lastRefill:   now,
			lastAccessed: now,
		}
		r.buckets[key] = b
	}
	r.mu.Unlock()

	b.mu.Lock()
	defer b.mu.Unlock()

	now := r.clock.Now()
	b.lastAccessed = now
	elapsed := now.Sub(b.lastRefill)
	refillTokens := elapsed.Seconds() / rule.RefillRate.Seconds()
	b.tokens = min(float64(rule.BurstSize), b.tokens+refillTokens)
	b.lastRefill = now

	if b.tokens >= 1 {
		b.tokens--
		return true, 0, nil
	}

	// Calculate retry-after
	retryAfter := time.Duration((1 - b.tokens) * rule.RefillRate.Seconds() * float64(time.Second))
	return false, retryAfter, nil
}

func (r *MemoryRateLimiter) cleanupLoop() {
	ticker := time.NewTicker(bucketCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.cleanup()
		case <-r.done:
			return
		}
	}
}

func (r *MemoryRateLimiter) cleanup() {
	r.mu.Lock()
	defer r.mu.Unlock()

	cutoff := r.clock.Now().Add(-bucketTTL)
	for key, b := range r.buckets {
		b.mu.Lock()
		lastAccessed := b.lastAccessed
		b.mu.Unlock()

		if lastAccessed.Before(cutoff) {
			delete(r.buckets, key)
		}
	}
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
