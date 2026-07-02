package cache

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const (
	rateLimitBucketCleanupInterval = time.Hour
	rateLimitBucketTTL             = 24 * time.Hour
	sessionCleanupInterval         = time.Hour
)

type clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// SQLService stores API cache state in the shared SQL database.
type SQLService struct {
	db                 *sql.DB
	driver             string
	clock              clock
	lastBucketCleanup  atomic.Int64
	lastSessionCleanup atomic.Int64
}

func NewSQLService(db *sql.DB, driver string) *SQLService {
	return newSQLServiceWithClock(db, driver, realClock{})
}

func newSQLServiceWithClock(db *sql.DB, driver string, c clock) *SQLService {
	driver = normalizeSQLDriver(driver)
	if c == nil {
		c = realClock{}
	}

	return &SQLService{
		db:     db,
		driver: driver,
		clock:  c,
	}
}

func (s *SQLService) TakeRateLimitToken(ctx context.Context, key string, rule RateLimitRule) (RateLimitDecision, error) {
	if s == nil || s.db == nil {
		return RateLimitDecision{}, fmt.Errorf("sql cache requires a database")
	}

	if key == "" {
		return RateLimitDecision{}, fmt.Errorf("rate limit key is required")
	}

	if rule.RefillRate <= 0 {
		return RateLimitDecision{}, fmt.Errorf("rate limit refill rate must be > 0")
	}

	if rule.BurstSize <= 0 {
		return RateLimitDecision{}, fmt.Errorf("rate limit burst size must be > 0")
	}

	now := s.clock.Now().UTC()
	decision, err := s.takeRateLimitTokenLocked(ctx, key, rule, now)
	if err != nil {
		return RateLimitDecision{}, err
	}

	s.maybeCleanupRateLimitBuckets(ctx, now)
	return decision, nil
}

func (s *SQLService) takeRateLimitTokenLocked(ctx context.Context, key string, rule RateLimitRule, now time.Time) (RateLimitDecision, error) {
	if s.driver == "sqlite3" {
		return s.takeRateLimitTokenSQLite(ctx, key, rule, now)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return RateLimitDecision{}, err
	}
	defer func() { _ = tx.Rollback() }()

	decision, err := s.takeRateLimitTokenInTransaction(ctx, tx, key, rule, now)
	if err != nil {
		return RateLimitDecision{}, err
	}

	if err := tx.Commit(); err != nil {
		return RateLimitDecision{}, err
	}

	return decision, nil
}

func (s *SQLService) takeRateLimitTokenSQLite(ctx context.Context, key string, rule RateLimitRule, now time.Time) (RateLimitDecision, error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return RateLimitDecision{}, err
	}
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(conn)

	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return RateLimitDecision{}, err
	}

	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")
		}
	}()

	decision, err := s.takeRateLimitTokenInTransaction(ctx, conn, key, rule, now)
	if err != nil {
		return RateLimitDecision{}, err
	}

	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return RateLimitDecision{}, err
	}

	committed = true
	return decision, nil
}

type sqlExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func (s *SQLService) takeRateLimitTokenInTransaction(ctx context.Context, exec sqlExecutor, key string, rule RateLimitRule, now time.Time) (RateLimitDecision, error) {
	nowUnix := now.UnixNano()
	inserted, err := s.insertInitialBucket(ctx, exec, key, rule, nowUnix)
	if err != nil {
		return RateLimitDecision{}, err
	}

	if inserted {
		return RateLimitDecision{Allowed: true}, nil
	}

	query := `SELECT tokens, last_refill_unix_nano FROM api_rate_limit_buckets WHERE bucket_key = ?`
	if s.driver == "pgx" {
		query += " FOR UPDATE"
	}

	var tokens float64
	var lastRefillUnix int64
	if err := exec.QueryRowContext(ctx, rebindQuery(query, s.driver), key).Scan(&tokens, &lastRefillUnix); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return RateLimitDecision{}, fmt.Errorf("rate limit bucket missing after insert conflict")
		}

		return RateLimitDecision{}, err
	}

	lastRefill := time.Unix(0, lastRefillUnix).UTC()
	elapsed := max(now.Sub(lastRefill), 0)

	tokens = min(float64(rule.BurstSize), tokens+float64(elapsed)/float64(rule.RefillRate))
	decision := RateLimitDecision{}
	if tokens >= 1 {
		tokens--
		decision.Allowed = true
	} else {
		decision.RetryAfter = time.Duration((1 - tokens) * float64(rule.RefillRate))
	}

	_, err = exec.ExecContext(ctx, rebindQuery(`
		UPDATE api_rate_limit_buckets
		SET tokens = ?, last_refill_unix_nano = ?, last_access_unix_nano = ?, updated_at = CURRENT_TIMESTAMP
		WHERE bucket_key = ?
	`, s.driver), tokens, nowUnix, nowUnix, key)

	if err != nil {
		return RateLimitDecision{}, err
	}

	return decision, nil
}

func (s *SQLService) insertInitialBucket(ctx context.Context, exec sqlExecutor, key string, rule RateLimitRule, nowUnix int64) (bool, error) {
	res, err := exec.ExecContext(ctx, rebindQuery(`
		INSERT INTO api_rate_limit_buckets (bucket_key, tokens, last_refill_unix_nano, last_access_unix_nano)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(bucket_key) DO NOTHING
	`, s.driver), key, float64(rule.BurstSize-1), nowUnix, nowUnix)

	if err != nil {
		return false, err
	}

	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}

	return n == 1, nil
}

func (s *SQLService) CreateSession(ctx context.Context, session Session) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("sql cache requires a database")
	}

	if strings.TrimSpace(session.TokenHash) == "" {
		return fmt.Errorf("session token hash is required")
	}

	if session.LocalUserID <= 0 {
		return fmt.Errorf("session local user id is required")
	}

	if strings.TrimSpace(session.CSRFTokenHash) == "" {
		return fmt.Errorf("session csrf token hash is required")
	}

	if !session.ExpiresAt.After(s.clock.Now()) {
		return fmt.Errorf("session expires_at must be in the future")
	}

	lastUsedAt := session.LastUsedAt.UTC()
	if lastUsedAt.IsZero() {
		lastUsedAt = s.clock.Now().UTC()
	}

	_, err := s.db.ExecContext(ctx, rebindQuery(`
		INSERT INTO api_sessions (session_hash, csrf_token_hash, local_user_id, expires_at_unix_nano, last_used_unix_nano)
		VALUES (?, ?, ?, ?, ?)
	`, s.driver), session.TokenHash, session.CSRFTokenHash, session.LocalUserID, session.ExpiresAt.UTC().UnixNano(), lastUsedAt.UnixNano())

	if err != nil {
		return err
	}

	s.maybeCleanupSessions(ctx, s.clock.Now().UTC())
	return nil
}

func (s *SQLService) ResolveSession(ctx context.Context, tokenHash string, now time.Time, idleTTL time.Duration) (Session, error) {
	if s == nil || s.db == nil {
		return Session{}, fmt.Errorf("sql cache requires a database")
	}

	var sess Session
	var expiresUnix int64
	var lastUsedUnix int64
	query := resolveSessionQuery(idleTTL)
	args := []any{tokenHash, now.UTC().UnixNano()}
	if idleTTL > 0 {
		args = append(args, now.UTC().Add(-idleTTL).UnixNano())
	}

	err := s.db.QueryRowContext(ctx, rebindQuery(query, s.driver), args...).Scan(&sess.TokenHash, &sess.CSRFTokenHash, &sess.LocalUserID, &sess.Username, &expiresUnix, &lastUsedUnix)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Session{}, ErrNotFound
		}

		return Session{}, err
	}

	sess.ExpiresAt = time.Unix(0, expiresUnix).UTC()
	sess.LastUsedAt = time.Unix(0, lastUsedUnix).UTC()
	return sess, nil
}

func resolveSessionQuery(idleTTL time.Duration) string {
	query := `
		SELECT s.session_hash, s.csrf_token_hash, s.local_user_id, u.username, s.expires_at_unix_nano, s.last_used_unix_nano
		FROM api_sessions s
		JOIN local_users u ON u.id = s.local_user_id
		WHERE s.session_hash = ?
		  AND u.enabled
		  AND s.expires_at_unix_nano > ?
	`

	if idleTTL > 0 {
		query += ` AND s.last_used_unix_nano > ?`
	}

	return query
}

func (s *SQLService) TouchSession(ctx context.Context, tokenHash string, now time.Time) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("sql cache requires a database")
	}

	_, err := s.db.ExecContext(ctx, rebindQuery(`
		UPDATE api_sessions SET last_used_at = CURRENT_TIMESTAMP, last_used_unix_nano = ? WHERE session_hash = ?
	`, s.driver), now.UTC().UnixNano(), tokenHash)

	return err
}

func (s *SQLService) DeleteSession(ctx context.Context, tokenHash string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("sql cache requires a database")
	}

	_, err := s.db.ExecContext(ctx, rebindQuery(`DELETE FROM api_sessions WHERE session_hash = ?`, s.driver), tokenHash)
	return err
}

func (s *SQLService) DeleteUserSessions(ctx context.Context, localUserID int64) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("sql cache requires a database")
	}

	_, err := s.db.ExecContext(ctx, rebindQuery(`DELETE FROM api_sessions WHERE local_user_id = ?`, s.driver), localUserID)
	return err
}

func (s *SQLService) maybeCleanupRateLimitBuckets(ctx context.Context, now time.Time) {
	last := s.lastBucketCleanup.Load()
	if last != 0 && now.UnixNano()-last < rateLimitBucketCleanupInterval.Nanoseconds() {
		return
	}

	if !s.lastBucketCleanup.CompareAndSwap(last, now.UnixNano()) {
		return
	}

	cutoff := now.Add(-rateLimitBucketTTL).UnixNano()
	_, _ = s.db.ExecContext(ctx, rebindQuery(
		`DELETE FROM api_rate_limit_buckets WHERE last_access_unix_nano < ?`,
		s.driver,
	), cutoff)
}

func (s *SQLService) maybeCleanupSessions(ctx context.Context, now time.Time) {
	last := s.lastSessionCleanup.Load()
	if last != 0 && now.UnixNano()-last < sessionCleanupInterval.Nanoseconds() {
		return
	}

	if !s.lastSessionCleanup.CompareAndSwap(last, now.UnixNano()) {
		return
	}

	_, _ = s.db.ExecContext(ctx, rebindQuery(
		`DELETE FROM api_sessions WHERE expires_at_unix_nano <= ?`,
		s.driver,
	), now.UnixNano())
}

func normalizeSQLDriver(driver string) string {
	switch strings.ToLower(strings.TrimSpace(driver)) {
	case "", "sqlite":
		return "sqlite3"
	case "postgres", "postgresql":
		return "pgx"
	default:
		return strings.ToLower(strings.TrimSpace(driver))
	}
}

func rebindQuery(query, driver string) string {
	if normalizeSQLDriver(driver) != "pgx" {
		return query
	}

	var b strings.Builder
	b.Grow(len(query) + 8)

	argNum := 1
	for i := 0; i < len(query); i++ {
		if query[i] == '?' {
			b.WriteByte('$')
			b.WriteString(strconv.Itoa(argNum))
			argNum++
			continue
		}

		b.WriteByte(query[i])
	}

	return b.String()
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

var _ Service = (*SQLService)(nil)
