// Package ratelimit provides request rate limiting for API endpoints.
package ratelimit

import (
	"context"
	"time"

	"vectis/internal/config"
)

// RateLimiter determines whether a request with the given key should be allowed.
type RateLimiter interface {
	// Allow checks if the request identified by key is within rate limits for the given rule.
	// Returns true if allowed, false if rate limited.
	// When false, retryAfter indicates how long to wait before the next request.
	Allow(ctx context.Context, key string, rule Rule) (allowed bool, retryAfter time.Duration, err error)
}

// Rule defines a token bucket rate limit.
type Rule struct {
	RefillRate time.Duration // Time between token refills (e.g., 12s = 5/min)
	BurstSize  int           // Maximum tokens in bucket
}

// Category defines rate limiting rules for different endpoint categories.
type Category struct {
	Auth    Rule // Setup, login, password change
	Token   Rule // Token management
	General Rule // All other endpoints
}

// DefaultCategory returns rate limits from configuration.
func DefaultCategory() Category {
	return Category{
		Auth:    Rule{RefillRate: config.RateLimitAuthRefillRate(), BurstSize: config.RateLimitAuthBurstSize()},
		Token:   Rule{RefillRate: config.RateLimitTokenRefillRate(), BurstSize: config.RateLimitTokenBurstSize()},
		General: Rule{RefillRate: config.RateLimitGeneralRefillRate(), BurstSize: config.RateLimitGeneralBurstSize()},
	}
}
