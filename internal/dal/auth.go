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

// LocalUserRecord represents a local user account.
type LocalUserRecord struct {
	ID        int64
	Username  string
	Enabled   bool
	CreatedAt sql.NullTime
}

// TokenScopeRecord represents a scope restriction on an API token.
type TokenScopeRecord struct {
	Action      string
	NamespaceID sql.NullInt64
	Propagate   bool
}

// AuditEventRecord represents a single audit log entry.
type AuditEventRecord struct {
	Type          string
	ActorID       sql.NullInt64
	TargetID      sql.NullInt64
	Metadata      []byte
	IPAddress     string
	CorrelationID string
	CreatedAt     sql.NullTime
}

// AuthRepository persists HTTP API authentication: bootstrap completion, local users, and API tokens.
type AuthRepository interface {
	IsSetupComplete(ctx context.Context) (bool, error)
	CompleteInitialSetup(ctx context.Context, username, passwordHash, tokenHash, tokenLabel string) (localUserID int64, err error)
	ResolveAPIToken(ctx context.Context, tokenHash string) (localUserID int64, username string, tokenID int64, err error)
	TouchAPITokenUsed(ctx context.Context, tokenHash string) error
	UserExists(ctx context.Context, localUserID int64) (bool, error)
	UserEnabled(ctx context.Context, localUserID int64) (bool, error)
	GetUserPasswordHash(ctx context.Context, localUserID int64) (string, error)
	UpdateUserPassword(ctx context.Context, localUserID int64, passwordHash string) error
	DeleteAllAPITokensForUser(ctx context.Context, localUserID int64) error
	ChangePasswordAndRevokeTokens(ctx context.Context, localUserID int64, passwordHash string) error
	ListAPITokens(ctx context.Context, localUserID int64) ([]*APITokenRecord, error)
	CreateAPIToken(ctx context.Context, localUserID int64, tokenHash, label string, expiresAt *time.Time) (int64, error)
	CreateAPITokenWithScopes(ctx context.Context, localUserID int64, tokenHash, label string, expiresAt *time.Time, scopes []*TokenScopeRecord) (int64, error)
	DeleteAPIToken(ctx context.Context, id int64) error
	GetAPITokenOwner(ctx context.Context, id int64) (localUserID int64, err error)
	GetTokenScopes(ctx context.Context, tokenID int64) ([]*TokenScopeRecord, error)
	CreateLocalUser(ctx context.Context, username, passwordHash string) (int64, error)
	ListLocalUsers(ctx context.Context) ([]*LocalUserRecord, error)
	GetLocalUser(ctx context.Context, id int64) (*LocalUserRecord, error)
	UpdateLocalUserEnabled(ctx context.Context, id int64, enabled bool) error
	DeleteLocalUser(ctx context.Context, id int64) error
	InsertAuditEvents(ctx context.Context, events []*AuditEventRecord) error
	CountEnabledAdmins(ctx context.Context) (int, error)
	IsUserAdmin(ctx context.Context, localUserID int64) (bool, error)
	CountEnabledRootAdmins(ctx context.Context) (int, error)
	IsUserRootAdmin(ctx context.Context, localUserID int64) (bool, error)
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

func (r *SQLAuthRepository) ResolveAPIToken(ctx context.Context, tokenHash string) (localUserID int64, username string, tokenID int64, err error) {
	now := time.Now().UTC()
	row := r.db.QueryRowContext(ctx, rebindQueryForPgx(`
SELECT t.id, u.id, u.username
FROM api_tokens t
JOIN local_users u ON u.id = t.local_user_id
WHERE t.token_hash = ?
  AND u.enabled
  AND (t.expires_at IS NULL OR t.expires_at > ?)
`), tokenHash, now)

	var tid int64
	var uid int64
	var uname string
	if err := row.Scan(&tid, &uid, &uname); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, "", 0, ErrNotFound
		}

		return 0, "", 0, normalizeSQLError(err)
	}

	return uid, uname, tid, nil
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

func (r *SQLAuthRepository) GetUserPasswordHash(ctx context.Context, localUserID int64) (string, error) {
	var hash string
	err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx(`SELECT password_hash FROM local_users WHERE id = ?`),
		localUserID,
	).Scan(&hash)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", ErrNotFound
		}

		return "", normalizeSQLError(err)
	}

	return hash, nil
}

func (r *SQLAuthRepository) UpdateUserPassword(ctx context.Context, localUserID int64, passwordHash string) error {
	res, err := r.db.ExecContext(ctx,
		rebindQueryForPgx(`UPDATE local_users SET password_hash = ? WHERE id = ?`),
		passwordHash, localUserID,
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

func (r *SQLAuthRepository) DeleteAllAPITokensForUser(ctx context.Context, localUserID int64) error {
	_, err := r.db.ExecContext(ctx,
		rebindQueryForPgx(`DELETE FROM api_tokens WHERE local_user_id = ?`),
		localUserID,
	)

	return normalizeSQLError(err)
}

func (r *SQLAuthRepository) ChangePasswordAndRevokeTokens(ctx context.Context, localUserID int64, passwordHash string) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	res, err := tx.ExecContext(ctx,
		rebindQueryForPgx(`UPDATE local_users SET password_hash = ? WHERE id = ?`),
		passwordHash, localUserID,
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

	_, err = tx.ExecContext(ctx,
		rebindQueryForPgx(`DELETE FROM api_tokens WHERE local_user_id = ?`),
		localUserID,
	)

	if err != nil {
		return normalizeSQLError(err)
	}

	return tx.Commit()
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

func (r *SQLAuthRepository) CreateLocalUser(ctx context.Context, username, passwordHash string) (int64, error) {
	var id int64
	err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx(`INSERT INTO local_users (username, password_hash) VALUES (?, ?) RETURNING id`),
		username, passwordHash,
	).Scan(&id)

	if err != nil {
		return 0, normalizeSQLError(err)
	}

	return id, nil
}

func (r *SQLAuthRepository) ListLocalUsers(ctx context.Context) ([]*LocalUserRecord, error) {
	rows, err := r.db.QueryContext(ctx,
		rebindQueryForPgx(`SELECT id, username, enabled, created_at FROM local_users ORDER BY created_at DESC`),
	)

	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer rows.Close()

	var users []*LocalUserRecord
	for rows.Next() {
		var u LocalUserRecord
		if err := rows.Scan(&u.ID, &u.Username, &u.Enabled, &u.CreatedAt); err != nil {
			return nil, normalizeSQLError(err)
		}

		users = append(users, &u)
	}

	if err := rows.Err(); err != nil {
		return nil, normalizeSQLError(err)
	}

	return users, nil
}

func (r *SQLAuthRepository) GetLocalUser(ctx context.Context, id int64) (*LocalUserRecord, error) {
	var u LocalUserRecord
	err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx(`SELECT id, username, enabled, created_at FROM local_users WHERE id = ?`),
		id,
	).Scan(&u.ID, &u.Username, &u.Enabled, &u.CreatedAt)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}

		return nil, normalizeSQLError(err)
	}

	return &u, nil
}

func (r *SQLAuthRepository) UpdateLocalUserEnabled(ctx context.Context, id int64, enabled bool) error {
	res, err := r.db.ExecContext(ctx,
		rebindQueryForPgx(`UPDATE local_users SET enabled = ? WHERE id = ?`),
		enabled, id,
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

func (r *SQLAuthRepository) DeleteLocalUser(ctx context.Context, id int64) error {
	res, err := r.db.ExecContext(ctx,
		rebindQueryForPgx(`DELETE FROM local_users WHERE id = ?`),
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

func (r *SQLAuthRepository) CountEnabledAdmins(ctx context.Context) (int, error) {
	var count int
	err := r.db.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT COUNT(DISTINCT rb.local_user_id)
		FROM role_bindings rb
		JOIN local_users u ON u.id = rb.local_user_id
		WHERE rb.role = ? AND u.enabled
	`), "admin").Scan(&count)

	if err != nil {
		return 0, normalizeSQLError(err)
	}

	return count, nil
}

func (r *SQLAuthRepository) IsUserAdmin(ctx context.Context, localUserID int64) (bool, error) {
	var isAdmin bool
	err := r.db.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT EXISTS(
			SELECT 1 FROM role_bindings rb
			JOIN local_users u ON u.id = rb.local_user_id
			WHERE rb.local_user_id = ? AND rb.role = ? AND u.enabled
		)
	`), localUserID, "admin").Scan(&isAdmin)

	if err != nil {
		return false, normalizeSQLError(err)
	}

	return isAdmin, nil
}

func (r *SQLAuthRepository) CountEnabledRootAdmins(ctx context.Context) (int, error) {
	var count int
	err := r.db.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT COUNT(DISTINCT rb.local_user_id)
		FROM role_bindings rb
		JOIN local_users u ON u.id = rb.local_user_id
		JOIN namespaces n ON n.id = rb.namespace_id
		WHERE rb.role = ? AND u.enabled AND n.path = ?
	`), "admin", "/").Scan(&count)

	if err != nil {
		return 0, normalizeSQLError(err)
	}

	return count, nil
}

func (r *SQLAuthRepository) IsUserRootAdmin(ctx context.Context, localUserID int64) (bool, error) {
	var isAdmin bool
	err := r.db.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT EXISTS(
			SELECT 1 FROM role_bindings rb
			JOIN local_users u ON u.id = rb.local_user_id
			JOIN namespaces n ON n.id = rb.namespace_id
			WHERE rb.local_user_id = ? AND rb.role = ? AND u.enabled AND n.path = ?
		)
	`), localUserID, "admin", "/").Scan(&isAdmin)

	if err != nil {
		return false, normalizeSQLError(err)
	}

	return isAdmin, nil
}

func (r *SQLAuthRepository) CreateAPITokenWithScopes(ctx context.Context, localUserID int64, tokenHash, label string, expiresAt *time.Time, scopes []*TokenScopeRecord) (int64, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	var id int64
	if expiresAt != nil {
		err = tx.QueryRowContext(ctx,
			rebindQueryForPgx(`INSERT INTO api_tokens (local_user_id, token_hash, label, expires_at) VALUES (?, ?, ?, ?) RETURNING id`),
			localUserID, tokenHash, label, *expiresAt,
		).Scan(&id)
	} else {
		err = tx.QueryRowContext(ctx,
			rebindQueryForPgx(`INSERT INTO api_tokens (local_user_id, token_hash, label) VALUES (?, ?, ?) RETURNING id`),
			localUserID, tokenHash, label,
		).Scan(&id)
	}

	if err != nil {
		return 0, normalizeSQLError(err)
	}

	for _, scope := range scopes {
		if scope.NamespaceID.Valid {
			_, err = tx.ExecContext(ctx,
				rebindQueryForPgx(`INSERT INTO api_token_scopes (api_token_id, action, namespace_id, propagate) VALUES (?, ?, ?, ?)`),
				id, scope.Action, scope.NamespaceID.Int64, scope.Propagate,
			)
		} else {
			_, err = tx.ExecContext(ctx,
				rebindQueryForPgx(`INSERT INTO api_token_scopes (api_token_id, action, propagate) VALUES (?, ?, ?)`),
				id, scope.Action, scope.Propagate,
			)
		}
		if err != nil {
			return 0, normalizeSQLError(err)
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return id, nil
}

func (r *SQLAuthRepository) GetTokenScopes(ctx context.Context, tokenID int64) ([]*TokenScopeRecord, error) {
	rows, err := r.db.QueryContext(ctx,
		rebindQueryForPgx(`SELECT action, namespace_id, propagate FROM api_token_scopes WHERE api_token_id = ?`),
		tokenID,
	)

	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer rows.Close()

	var scopes []*TokenScopeRecord
	for rows.Next() {
		var s TokenScopeRecord
		if err := rows.Scan(&s.Action, &s.NamespaceID, &s.Propagate); err != nil {
			return nil, normalizeSQLError(err)
		}

		scopes = append(scopes, &s)
	}

	if err := rows.Err(); err != nil {
		return nil, normalizeSQLError(err)
	}

	return scopes, nil
}

func (r *SQLAuthRepository) InsertAuditEvents(ctx context.Context, events []*AuditEventRecord) error {
	if len(events) == 0 {
		return nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	for _, event := range events {
		_, err := tx.ExecContext(ctx,
			rebindQueryForPgx(`INSERT INTO audit_log (event_type, actor_id, target_id, metadata, ip_address, correlation_id) VALUES (?, ?, ?, ?, ?, ?)`),
			event.Type, event.ActorID, event.TargetID, event.Metadata, event.IPAddress, event.CorrelationID,
		)
		if err != nil {
			return normalizeSQLError(err)
		}
	}

	return tx.Commit()
}

var ErrSetupAlreadyComplete = errors.New("setup already complete")
