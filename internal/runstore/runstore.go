package runstore

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
)

const (
	DefaultLeaseTTL      = 15 * time.Minute
	DefaultRenewInterval = 5 * time.Minute
)

type RunStatusStore interface {
	MarkRunRunning(ctx context.Context, runID string) error
	MarkRunSucceeded(ctx context.Context, runID string) error
	MarkRunFailed(ctx context.Context, runID string, reason string) error
}

type Store struct {
	db *sql.DB
}

func NewStore(db *sql.DB) *Store {
	return &Store{db: db}
}

func (s *Store) MarkRunRunning(ctx context.Context, runID string) error {
	_, err := s.db.ExecContext(ctx,
		"UPDATE job_runs SET status = ?, started_at = COALESCE(started_at, CURRENT_TIMESTAMP) WHERE run_id = ?",
		"running", runID)
	return err
}

func (s *Store) MarkRunSucceeded(ctx context.Context, runID string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE job_runs SET status = ?, finished_at = CURRENT_TIMESTAMP,
			lease_owner = NULL, lease_until = NULL WHERE run_id = ?`,
		"succeeded", runID)
	return err
}

func (s *Store) MarkRunFailed(ctx context.Context, runID string, reason string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE job_runs SET status = ?, finished_at = CURRENT_TIMESTAMP, failure_reason = ?,
			lease_owner = NULL, lease_until = NULL WHERE run_id = ?`,
		"failed", reason, runID)
	return err
}

func (s *Store) TryClaim(ctx context.Context, runID, owner string, leaseUntil time.Time) (claimed bool, err error) {
	nowUnix := time.Now().Unix()
	res, err := s.db.ExecContext(ctx, `
		UPDATE job_runs SET
			lease_owner = ?,
			lease_until = ?,
			status = 'running',
			started_at = COALESCE(started_at, CURRENT_TIMESTAMP)
		WHERE run_id = ?
			AND status = 'queued'
			AND (lease_until IS NULL OR lease_until < ?)
	`, owner, leaseUntil.Unix(), runID, nowUnix)

	if err != nil {
		return false, err
	}

	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}

	return n == 1, nil
}

func (s *Store) RenewLease(ctx context.Context, runID, owner string, leaseUntil time.Time) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE job_runs SET lease_until = ?
		WHERE run_id = ? AND lease_owner = ? AND status = 'running'
	`, leaseUntil.Unix(), runID, owner)

	if err != nil {
		return err
	}

	n, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if n == 0 {
		return fmt.Errorf("renew lease: no matching running row for run_id=%q owner=%q", runID, owner)
	}

	return nil
}

func TouchDispatched(ctx context.Context, db *sql.DB, runID string) error {
	_, err := db.ExecContext(ctx,
		`UPDATE job_runs SET last_dispatched_at = ? WHERE run_id = ?`,
		time.Now().Unix(), runID)

	return err
}

func CreateRun(ctx context.Context, db *sql.DB, jobID string, runIndex *int) (runID string, runIndexOut int, err error) {
	runID = uuid.New().String()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return "", 0, err
	}
	defer func() { _ = tx.Rollback() }()

	var idx int
	if runIndex != nil {
		idx = *runIndex
	} else {
		err = tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(run_index), 0) + 1 FROM job_runs WHERE job_id = ?", jobID).Scan(&idx)
		if err != nil {
			return "", 0, err
		}
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO job_runs (run_id, job_id, run_index, status, started_at) VALUES (?, ?, ?, ?, NULL)`,
		runID, jobID, idx, "queued")

	if err != nil {
		return "", 0, err
	}

	if err = tx.Commit(); err != nil {
		return "", 0, err
	}

	return runID, idx, nil
}
