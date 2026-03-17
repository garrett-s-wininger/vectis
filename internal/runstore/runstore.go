package runstore

import (
	"context"
	"database/sql"

	"github.com/google/uuid"
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
		"UPDATE job_runs SET status = ?, finished_at = CURRENT_TIMESTAMP WHERE run_id = ?",
		"succeeded", runID)
	return err
}

func (s *Store) MarkRunFailed(ctx context.Context, runID string, reason string) error {
	_, err := s.db.ExecContext(ctx,
		"UPDATE job_runs SET status = ?, finished_at = CURRENT_TIMESTAMP, failure_reason = ? WHERE run_id = ?",
		"failed", reason, runID)
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
		"INSERT INTO job_runs (run_id, job_id, run_index, status, started_at) VALUES (?, ?, ?, ?, NULL)",
		runID, jobID, idx, "queued")

	if err != nil {
		return "", 0, err
	}

	if err = tx.Commit(); err != nil {
		return "", 0, err
	}

	return runID, idx, nil
}
