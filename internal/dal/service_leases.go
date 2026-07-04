package dal

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type SQLServiceLeasesRepository struct {
	db *sql.DB
}

func (r *SQLServiceLeasesRepository) TryAcquire(ctx context.Context, name, owner string, now, leaseUntil time.Time) (bool, error) {
	if name == "" {
		return false, fmt.Errorf("service lease name is required")
	}

	if owner == "" {
		return false, fmt.Errorf("service lease owner is required")
	}

	if !leaseUntil.After(now) {
		return false, fmt.Errorf("service lease_until must be after now")
	}

	res, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
		INSERT INTO service_leases (name, owner, lease_until, updated_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET
			owner = excluded.owner,
			lease_until = excluded.lease_until,
			updated_at = excluded.updated_at
		WHERE service_leases.owner = ? OR service_leases.lease_until <= ?
	`), name, owner, leaseUntil.UTC().Unix(), now.UTC().Unix(), owner, now.UTC().Unix())

	if err != nil {
		return false, normalizeSQLError(err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return false, normalizeSQLError(err)
	}

	return n == 1, nil
}

func (r *SQLServiceLeasesRepository) Release(ctx context.Context, name, owner string) error {
	if name == "" {
		return fmt.Errorf("service lease name is required")
	}

	if owner == "" {
		return fmt.Errorf("service lease owner is required")
	}

	_, err := r.db.ExecContext(ctx,
		rebindQueryForPgx(`DELETE FROM service_leases WHERE name = ? AND owner = ?`),
		name,
		owner,
	)

	return normalizeSQLError(err)
}

var _ ServiceLeasesRepository = (*SQLServiceLeasesRepository)(nil)
