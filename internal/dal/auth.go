package dal

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// AuthRepository persists HTTP API authentication: bootstrap completion, local users, and API tokens.
type AuthRepository interface {
	IsSetupComplete(ctx context.Context) (bool, error)
	CompleteInitialSetup(ctx context.Context, username, passwordHash, tokenHash, tokenLabel string) (localUserID int64, err error)
	ResolveAPIToken(ctx context.Context, tokenHash string) (localUserID int64, username string, err error)
	TouchAPITokenUsed(ctx context.Context, tokenHash string) error
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

var ErrSetupAlreadyComplete = errors.New("setup already complete")
