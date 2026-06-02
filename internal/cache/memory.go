package cache

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// MemoryService is a process-local cache backend for tests and single-replica deployments.
type MemoryService struct {
	buckets            map[string]*memoryBucket
	sessions           map[string]Session
	mu                 sync.Mutex
	clock              clock
	lastBucketCleanup  time.Time
	lastSessionCleanup time.Time
}

type memoryBucket struct {
	tokens     float64
	lastRefill time.Time
	lastAccess time.Time
}

func NewMemoryService() *MemoryService {
	return newMemoryServiceWithClock(realClock{})
}

func newMemoryServiceWithClock(c clock) *MemoryService {
	if c == nil {
		c = realClock{}
	}

	return &MemoryService{
		buckets:  make(map[string]*memoryBucket),
		sessions: make(map[string]Session),
		clock:    c,
	}
}

func (m *MemoryService) TakeRateLimitToken(_ context.Context, key string, rule RateLimitRule) (RateLimitDecision, error) {
	if key == "" {
		return RateLimitDecision{}, fmt.Errorf("rate limit key is required")
	}

	if rule.RefillRate <= 0 {
		return RateLimitDecision{}, fmt.Errorf("rate limit refill rate must be > 0")
	}

	if rule.BurstSize <= 0 {
		return RateLimitDecision{}, fmt.Errorf("rate limit burst size must be > 0")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.clock.Now().UTC()
	b, ok := m.buckets[key]
	if !ok {
		m.buckets[key] = &memoryBucket{
			tokens:     float64(rule.BurstSize - 1),
			lastRefill: now,
			lastAccess: now,
		}
		m.cleanupRateLimitBucketsLocked(now)

		return RateLimitDecision{Allowed: true}, nil
	}

	elapsed := max(now.Sub(b.lastRefill), 0)

	b.tokens = min(float64(rule.BurstSize), b.tokens+float64(elapsed)/float64(rule.RefillRate))
	b.lastRefill = now
	b.lastAccess = now
	if b.tokens >= 1 {
		b.tokens--
		m.cleanupRateLimitBucketsLocked(now)
		return RateLimitDecision{Allowed: true}, nil
	}

	m.cleanupRateLimitBucketsLocked(now)
	return RateLimitDecision{RetryAfter: time.Duration((1 - b.tokens) * float64(rule.RefillRate))}, nil
}

func (m *MemoryService) CreateSession(_ context.Context, session Session) error {
	if session.TokenHash == "" {
		return fmt.Errorf("session token hash is required")
	}

	if session.LocalUserID <= 0 {
		return fmt.Errorf("session local user id is required")
	}

	if session.CSRFTokenHash == "" {
		return fmt.Errorf("session csrf token hash is required")
	}

	if !session.ExpiresAt.After(m.clock.Now()) {
		return fmt.Errorf("session expires_at must be in the future")
	}

	if session.LastUsedAt.IsZero() {
		session.LastUsedAt = m.clock.Now().UTC()
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.sessions[session.TokenHash] = session
	m.cleanupSessionsLocked(m.clock.Now().UTC())
	return nil
}

func (m *MemoryService) ResolveSession(_ context.Context, tokenHash string, now time.Time, idleTTL time.Duration) (Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	session, ok := m.sessions[tokenHash]
	if !ok || !session.ExpiresAt.After(now) {
		if ok {
			delete(m.sessions, tokenHash)
		}

		return Session{}, ErrNotFound
	}

	if idleTTL > 0 && !session.LastUsedAt.After(now.Add(-idleTTL)) {
		delete(m.sessions, tokenHash)
		return Session{}, ErrNotFound
	}

	return session, nil
}

func (m *MemoryService) TouchSession(_ context.Context, tokenHash string, now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	session, ok := m.sessions[tokenHash]
	if !ok {
		return nil
	}

	session.LastUsedAt = now.UTC()
	m.sessions[tokenHash] = session

	return nil
}

func (m *MemoryService) DeleteSession(_ context.Context, tokenHash string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, tokenHash)
	return nil
}

func (m *MemoryService) DeleteUserSessions(_ context.Context, localUserID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for tokenHash, session := range m.sessions {
		if session.LocalUserID == localUserID {
			delete(m.sessions, tokenHash)
		}
	}

	return nil
}

func (m *MemoryService) cleanupRateLimitBucketsLocked(now time.Time) {
	if !m.lastBucketCleanup.IsZero() && now.Sub(m.lastBucketCleanup) < rateLimitBucketCleanupInterval {
		return
	}

	m.lastBucketCleanup = now
	cutoff := now.Add(-rateLimitBucketTTL)
	for key, bucket := range m.buckets {
		if bucket.lastAccess.Before(cutoff) {
			delete(m.buckets, key)
		}
	}
}

func (m *MemoryService) cleanupSessionsLocked(now time.Time) {
	if !m.lastSessionCleanup.IsZero() && now.Sub(m.lastSessionCleanup) < sessionCleanupInterval {
		return
	}

	m.lastSessionCleanup = now
	for tokenHash, session := range m.sessions {
		if !session.ExpiresAt.After(now) {
			delete(m.sessions, tokenHash)
		}
	}
}

var _ Service = (*MemoryService)(nil)
