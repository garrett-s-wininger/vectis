// Package cache provides shared API edge-state storage.
package cache

import (
	"context"
	"errors"
	"time"
)

var ErrNotFound = errors.New("cache entry not found")

func IsNotFound(err error) bool {
	return errors.Is(err, ErrNotFound)
}

type RateLimitRule struct {
	RefillRate time.Duration
	BurstSize  int
}

type RateLimitDecision struct {
	Allowed    bool
	RetryAfter time.Duration
}

type Session struct {
	TokenHash     string
	CSRFTokenHash string
	LocalUserID   int64
	Username      string
	ExpiresAt     time.Time
	LastUsedAt    time.Time
}

// Service stores API edge state that must be shared when the API is replicated.
type Service interface {
	TakeRateLimitToken(ctx context.Context, key string, rule RateLimitRule) (RateLimitDecision, error)
	CreateSession(ctx context.Context, session Session) error
	ResolveSession(ctx context.Context, tokenHash string, now time.Time, idleTTL time.Duration) (Session, error)
	TouchSession(ctx context.Context, tokenHash string, now time.Time) error
	DeleteSession(ctx context.Context, tokenHash string) error
	DeleteUserSessions(ctx context.Context, localUserID int64) error
}
