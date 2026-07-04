package ratelimit

import (
	"context"
	"fmt"
	"time"

	"vectis/internal/cache"
)

// CacheRateLimiter delegates rate-limit decisions to the shared API cache service.
type CacheRateLimiter struct {
	cache cache.Service
}

func NewCacheRateLimiter(cacheService cache.Service) *CacheRateLimiter {
	return &CacheRateLimiter{cache: cacheService}
}

func (r *CacheRateLimiter) Allow(ctx context.Context, key string, rule Rule) (bool, time.Duration, error) {
	if r == nil || r.cache == nil {
		return false, 0, fmt.Errorf("rate limiter requires a cache service")
	}

	decision, err := r.cache.TakeRateLimitToken(ctx, key, cache.RateLimitRule{
		RefillRate: rule.RefillRate,
		BurstSize:  rule.BurstSize,
	})

	if err != nil {
		return false, 0, err
	}

	return decision.Allowed, decision.RetryAfter, nil
}

var _ RateLimiter = (*CacheRateLimiter)(nil)
