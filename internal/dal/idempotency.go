package dal

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

type SQLIdempotencyRepository struct {
	db *sql.DB
}

func (r *SQLIdempotencyRepository) Reserve(ctx context.Context, scope, key, requestHash string) (IdempotencyRecord, bool, error) {
	_, err := r.db.ExecContext(ctx,
		rebindQueryForPgx(`INSERT INTO idempotency_keys (scope, key, request_hash) VALUES (?, ?, ?)`),
		scope,
		key,
		requestHash,
	)

	if err == nil {
		return IdempotencyRecord{Scope: scope, Key: key, RequestHash: requestHash}, true, nil
	}

	if !IsConflict(normalizeSQLError(err)) {
		return IdempotencyRecord{}, false, normalizeSQLError(err)
	}

	var rec IdempotencyRecord
	var response sql.NullString
	if err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx(`SELECT scope, key, request_hash, response_json, resource_type, resource_id FROM idempotency_keys WHERE scope = ? AND key = ?`),
		scope,
		key,
	).Scan(&rec.Scope, &rec.Key, &rec.RequestHash, &response, &rec.ResourceType, &rec.ResourceID); err != nil {
		return IdempotencyRecord{}, false, normalizeSQLError(err)
	}

	if response.Valid {
		rec.ResponseJSON = &response.String
	}

	return rec, false, nil
}

func (r *SQLIdempotencyRepository) AttachResource(ctx context.Context, scope, key, resourceType, resourceID string) error {
	resourceType = strings.TrimSpace(resourceType)
	resourceID = strings.TrimSpace(resourceID)
	if resourceType == "" || resourceID == "" {
		return fmt.Errorf("%w: idempotency resource type and id are required", ErrConflict)
	}

	result, err := r.db.ExecContext(ctx,
		rebindQueryForPgx(`
			UPDATE idempotency_keys
			SET resource_type = ?,
				resource_id = ?,
				updated_at = CURRENT_TIMESTAMP
			WHERE scope = ?
				AND key = ?
				AND response_json IS NULL
		`),
		resourceType,
		resourceID,
		scope,
		key,
	)

	if err != nil {
		return normalizeSQLError(err)
	}

	return requireRowsAffected(result, "idempotency key", scope+":"+key)
}

func (r *SQLIdempotencyRepository) Complete(ctx context.Context, scope, key, responseJSON string) error {
	_, err := r.db.ExecContext(ctx,
		rebindQueryForPgx(`UPDATE idempotency_keys SET response_json = ?, updated_at = CURRENT_TIMESTAMP WHERE scope = ? AND key = ?`),
		responseJSON,
		scope,
		key,
	)

	return normalizeSQLError(err)
}

func (r *SQLIdempotencyRepository) Release(ctx context.Context, scope, key string) error {
	_, err := r.db.ExecContext(ctx,
		rebindQueryForPgx(`DELETE FROM idempotency_keys WHERE scope = ? AND key = ? AND response_json IS NULL`),
		scope,
		key,
	)

	return normalizeSQLError(err)
}
