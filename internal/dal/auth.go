package dal

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// APITokenRecord represents metadata for an API token (never includes the hash).
type APITokenRecord struct {
	ID          int64
	LocalUserID int64
	Label       string
	ExpiresAt   sql.NullTime
	CreatedAt   sql.NullTime
	LastUsedAt  sql.NullTime
}

// AuthRepository persists HTTP API authentication: bootstrap completion, local users, and API tokens.
type AuthRepository interface {
	IsSetupComplete(ctx context.Context) (bool, error)
	CompleteInitialSetup(ctx context.Context, username, passwordHash, tokenHash, tokenLabel string) (localUserID int64, err error)
	ResolveAPIToken(ctx context.Context, tokenHash string) (localUserID int64, username string, err error)
	TouchAPITokenUsed(ctx context.Context, tokenHash string) error
	UserExists(ctx context.Context, localUserID int64) (bool, error)
	UserEnabled(ctx context.Context, localUserID int64) (bool, error)
	ListAPITokens(ctx context.Context, localUserID int64) ([]*APITokenRecord, error)
	CreateAPIToken(ctx context.Context, localUserID int64, tokenHash, label string, expiresAt *time.Time) (int64, error)
	DeleteAPIToken(ctx context.Context, id int64) error
	GetAPITokenOwner(ctx context.Context, id int64) (localUserID int64, err error)
}

type SQLAuthRepository struct {
	db *sql.DB
}

func NewSQLAuthRepository(db *sql.DB) *SQLAuthRepository {
	return &SQLAuthRepository{db: db}
}

func (r *SQLAuthRepository) IsSetupComplete(ctx context.Context) (bool, error) {
	var t sql.NullTime
	err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx(`SELECT setup_completed_at FROM auth_instance_state WHERE id = 1`),
	).Scan(&t)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}

		return false, normalizeSQLError(err)
	}

	return t.Valid, nil
}

func (r *SQLAuthRepository) CompleteInitialSetup(ctx context.Context, username, passwordHash, tokenHash, tokenLabel string) (localUserID int64, err error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	result, err := tx.ExecContext(ctx,
		rebindQueryForPgx(`UPDATE auth_instance_state
SET setup_completed_at = CURRENT_TIMESTAMP
WHERE id = 1 AND setup_completed_at IS NULL`),
	)
	if err != nil {
		return 0, normalizeSQLError(err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, normalizeSQLError(err)
	}

	if rowsAffected == 0 {
		return 0, ErrSetupAlreadyComplete
	}

	var id int64
	if err := tx.QueryRowContext(ctx,
		rebindQueryForPgx(`INSERT INTO local_users (username, password_hash) VALUES (?, ?) RETURNING id`),
		username, passwordHash,
	).Scan(&id); err != nil {
		return 0, normalizeSQLError(err)
	}

	if _, err := tx.ExecContext(ctx,
		rebindQueryForPgx(`INSERT INTO api_tokens (local_user_id, token_hash, label) VALUES (?, ?, ?)`),
		id, tokenHash, tokenLabel,
	); err != nil {
		return 0, normalizeSQLError(err)
	}

	if _, err := tx.ExecContext(ctx,
		rebindQueryForPgx(`INSERT INTO role_bindings (local_user_id, namespace_id, role) VALUES (?, ?, ?)`),
		id, 1, "admin",
	); err != nil {
		return 0, normalizeSQLError(err)
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return id, nil
}

func (r *SQLAuthRepository) ResolveAPIToken(ctx context.Context, tokenHash string) (localUserID int64, username string, err error) {
	now := time.Now().UTC()
	row := r.db.QueryRowContext(ctx, rebindQueryForPgx(`
SELECT u.id, u.username
FROM api_tokens t
JOIN local_users u ON u.id = t.local_user_id
WHERE t.token_hash = ?
  AND u.enabled
  AND (t.expires_at IS NULL OR t.expires_at > ?)
`), tokenHash, now)

	var uid int64
	var uname string
	if err := row.Scan(&uid, &uname); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, "", ErrNotFound
		}

		return 0, "", normalizeSQLError(err)
	}

	return uid, uname, nil
}

func (r *SQLAuthRepository) TouchAPITokenUsed(ctx context.Context, tokenHash string) error {
	_, err := r.db.ExecContext(ctx,
		rebindQueryForPgx(`UPDATE api_tokens SET last_used_at = CURRENT_TIMESTAMP WHERE token_hash = ?`),
		tokenHash,
	)

	return normalizeSQLError(err)
}

func (r *SQLAuthRepository) UserExists(ctx context.Context, localUserID int64) (bool, error) {
	var exists bool
	err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx(`SELECT EXISTS(SELECT 1 FROM local_users WHERE id = ?)`),
		localUserID,
	).Scan(&exists)

	if err != nil {
		return false, normalizeSQLError(err)
	}

	return exists, nil
}

func (r *SQLAuthRepository) UserEnabled(ctx context.Context, localUserID int64) (bool, error) {
	var enabled bool
	err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx(`SELECT EXISTS(SELECT 1 FROM local_users WHERE id = ? AND enabled)`),
		localUserID,
	).Scan(&enabled)

	if err != nil {
		return false, normalizeSQLError(err)
	}

	return enabled, nil
}

func (r *SQLAuthRepository) ListAPITokens(ctx context.Context, localUserID int64) ([]*APITokenRecord, error) {
	rows, err := r.db.QueryContext(ctx,
		rebindQueryForPgx(`SELECT id, local_user_id, label, expires_at, created_at, last_used_at FROM api_tokens WHERE local_user_id = ? ORDER BY created_at DESC`),
		localUserID,
	)

	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer rows.Close()

	var tokens []*APITokenRecord
	for rows.Next() {
		var t APITokenRecord
		if err := rows.Scan(&t.ID, &t.LocalUserID, &t.Label, &t.ExpiresAt, &t.CreatedAt, &t.LastUsedAt); err != nil {
			return nil, normalizeSQLError(err)
		}

		tokens = append(tokens, &t)
	}

	if err := rows.Err(); err != nil {
		return nil, normalizeSQLError(err)
	}

	return tokens, nil
}

func (r *SQLAuthRepository) CreateAPIToken(ctx context.Context, localUserID int64, tokenHash, label string, expiresAt *time.Time) (int64, error) {
	var id int64
	var err error

	if expiresAt != nil {
		err = r.db.QueryRowContext(ctx,
			rebindQueryForPgx(`INSERT INTO api_tokens (local_user_id, token_hash, label, expires_at) VALUES (?, ?, ?, ?) RETURNING id`),
			localUserID, tokenHash, label, *expiresAt,
		).Scan(&id)
	} else {
		err = r.db.QueryRowContext(ctx,
			rebindQueryForPgx(`INSERT INTO api_tokens (local_user_id, token_hash, label) VALUES (?, ?, ?) RETURNING id`),
			localUserID, tokenHash, label,
		).Scan(&id)
	}

	if err != nil {
		return 0, normalizeSQLError(err)
	}

	return id, nil
}

func (r *SQLAuthRepository) DeleteAPIToken(ctx context.Context, id int64) error {
	res, err := r.db.ExecContext(ctx,
		rebindQueryForPgx(`DELETE FROM api_tokens WHERE id = ?`),
		id,
	)

	if err != nil {
		return normalizeSQLError(err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return normalizeSQLError(err)
	}

	if rowsAffected == 0 {
		return ErrNotFound
	}

	return nil
}

func (r *SQLAuthRepository) GetAPITokenOwner(ctx context.Context, id int64) (localUserID int64, err error) {
	var uid int64
	err = r.db.QueryRowContext(ctx,
		rebindQueryForPgx(`SELECT local_user_id FROM api_tokens WHERE id = ?`),
		id,
	).Scan(&uid)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, ErrNotFound
		}

		return 0, normalizeSQLError(err)
	}

	return uid, nil
}

var ErrSetupAlreadyComplete = errors.New("setup already complete")
