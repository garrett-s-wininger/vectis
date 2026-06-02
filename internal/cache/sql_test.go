package cache

import (
	"context"
	"testing"
	"time"

	"vectis/internal/testutil/dbtest"
)

type testClock struct {
	t time.Time
}

func (c *testClock) Now() time.Time      { return c.t }
func (c *testClock) Add(d time.Duration) { c.t = c.t.Add(d) }

func TestSQLService_RateLimitSharedBuckets(t *testing.T) {
	db := dbtest.NewTestDB(t)
	clock := &testClock{t: time.Now()}
	cache1 := newSQLServiceWithClock(db, "sqlite3", clock)
	cache2 := newSQLServiceWithClock(db, "sqlite3", clock)

	ctx := context.Background()
	rule := RateLimitRule{RefillRate: time.Minute, BurstSize: 2}

	decision, err := cache1.TakeRateLimitToken(ctx, "shared", rule)
	if err != nil {
		t.Fatalf("cache1 TakeRateLimitToken error: %v", err)
	}

	if !decision.Allowed {
		t.Fatal("first request should be allowed")
	}

	decision, err = cache2.TakeRateLimitToken(ctx, "shared", rule)
	if err != nil {
		t.Fatalf("cache2 TakeRateLimitToken error: %v", err)
	}

	if !decision.Allowed {
		t.Fatal("second request should be allowed")
	}

	decision, err = cache1.TakeRateLimitToken(ctx, "shared", rule)
	if err != nil {
		t.Fatalf("cache1 TakeRateLimitToken error: %v", err)
	}

	if decision.Allowed {
		t.Fatal("third request should be rate limited across cache instances")
	}

	if decision.RetryAfter <= 0 {
		t.Fatal("expected positive retry-after")
	}
}

func TestSQLService_RateLimitRefill(t *testing.T) {
	db := dbtest.NewTestDB(t)
	clock := &testClock{t: time.Now()}
	cache := newSQLServiceWithClock(db, "sqlite3", clock)

	ctx := context.Background()
	rule := RateLimitRule{RefillRate: 12 * time.Second, BurstSize: 1}

	decision, err := cache.TakeRateLimitToken(ctx, "refill", rule)
	if err != nil {
		t.Fatalf("TakeRateLimitToken error: %v", err)
	}

	if !decision.Allowed {
		t.Fatal("first request should be allowed")
	}

	decision, err = cache.TakeRateLimitToken(ctx, "refill", rule)
	if err != nil {
		t.Fatalf("TakeRateLimitToken error: %v", err)
	}

	if decision.Allowed {
		t.Fatal("second request should be rate limited")
	}

	clock.Add(12 * time.Second)

	decision, err = cache.TakeRateLimitToken(ctx, "refill", rule)
	if err != nil {
		t.Fatalf("TakeRateLimitToken error: %v", err)
	}

	if !decision.Allowed {
		t.Fatal("request should be allowed after refill")
	}
}

func TestSQLService_Sessions(t *testing.T) {
	db := dbtest.NewTestDB(t)
	cache := NewSQLService(db, "sqlite3")
	ctx := context.Background()

	res, err := db.ExecContext(ctx, `INSERT INTO local_users (username, password_hash, enabled) VALUES (?, ?, ?)`, "admin", "hash", true)
	if err != nil {
		t.Fatalf("insert user: %v", err)
	}

	uid, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("last insert id: %v", err)
	}

	expiresAt := time.Now().UTC().Add(time.Hour)
	if err := cache.CreateSession(ctx, Session{
		TokenHash:     "hash",
		CSRFTokenHash: "csrf-hash",
		LocalUserID:   uid,
		ExpiresAt:     expiresAt,
	}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	session, err := cache.ResolveSession(ctx, "hash", time.Now().UTC(), time.Hour)
	if err != nil {
		t.Fatalf("ResolveSession: %v", err)
	}

	if session.LocalUserID != uid || session.Username != "admin" {
		t.Fatalf("bad session: %+v", session)
	}

	if session.LastUsedAt.IsZero() {
		t.Fatalf("expected last used time: %+v", session)
	}

	if err := cache.DeleteUserSessions(ctx, uid); err != nil {
		t.Fatalf("DeleteUserSessions: %v", err)
	}

	if _, err := cache.ResolveSession(ctx, "hash", time.Now().UTC(), time.Hour); !IsNotFound(err) {
		t.Fatalf("ResolveSession after delete err = %v, want not found", err)
	}
}

func TestSQLService_SessionExpiry(t *testing.T) {
	db := dbtest.NewTestDB(t)
	clock := &testClock{t: time.Now()}
	cache := newSQLServiceWithClock(db, "sqlite3", clock)
	ctx := context.Background()

	res, err := db.ExecContext(ctx, `INSERT INTO local_users (username, password_hash, enabled) VALUES (?, ?, ?)`, "admin", "hash", true)
	if err != nil {
		t.Fatalf("insert user: %v", err)
	}

	uid, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("last insert id: %v", err)
	}

	if err := cache.CreateSession(ctx, Session{
		TokenHash:     "hash",
		CSRFTokenHash: "csrf-hash",
		LocalUserID:   uid,
		ExpiresAt:     clock.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	if _, err := cache.ResolveSession(ctx, "hash", clock.Now().Add(2*time.Hour), time.Hour); !IsNotFound(err) {
		t.Fatalf("ResolveSession after expiry err = %v, want not found", err)
	}
}

func TestSQLService_SessionIdleExpiry(t *testing.T) {
	db := dbtest.NewTestDB(t)
	clock := &testClock{t: time.Now()}
	cache := newSQLServiceWithClock(db, "sqlite3", clock)
	ctx := context.Background()

	res, err := db.ExecContext(ctx, `INSERT INTO local_users (username, password_hash, enabled) VALUES (?, ?, ?)`, "admin", "hash", true)
	if err != nil {
		t.Fatalf("insert user: %v", err)
	}

	uid, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("last insert id: %v", err)
	}

	if err := cache.CreateSession(ctx, Session{
		TokenHash:     "hash",
		CSRFTokenHash: "csrf-hash",
		LocalUserID:   uid,
		ExpiresAt:     clock.Now().Add(24 * time.Hour),
	}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	if _, err := cache.ResolveSession(ctx, "hash", clock.Now().Add(2*time.Hour), time.Hour); !IsNotFound(err) {
		t.Fatalf("ResolveSession after idle expiry err = %v, want not found", err)
	}

	if err := cache.TouchSession(ctx, "hash", clock.Now().Add(90*time.Minute)); err != nil {
		t.Fatalf("TouchSession: %v", err)
	}

	if _, err := cache.ResolveSession(ctx, "hash", clock.Now().Add(2*time.Hour), time.Hour); err != nil {
		t.Fatalf("ResolveSession after touch: %v", err)
	}
}
