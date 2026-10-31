package dal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

type OrchestratorRetirement struct {
	RunID            string
	PersistenceToken string
}

type OrchestratorRetirementsRepository interface {
	ListDue(ctx context.Context, nowUnix int64, limit int) ([]OrchestratorRetirement, error)
	Delete(ctx context.Context, token string) error
	RetryAfter(ctx context.Context, token string, nextUnix int64) error
	RecordRejectedAdmission(ctx context.Context, runID, executionID, token string) (bool, error)
}

type SQLOrchestratorRetirementsRepository struct {
	db     *sql.DB
	cellID string
}

func (r *SQLRepositories) OrchestratorRetirements() OrchestratorRetirementsRepository {
	return &SQLOrchestratorRetirementsRepository{db: r.runs.db, cellID: r.runs.cellID}
}

func (r *SQLOrchestratorRetirementsRepository) RecordRejectedAdmission(ctx context.Context, runID, executionID, token string) (bool, error) {
	if runID == "" || executionID == "" || token == "" {
		return false, fmt.Errorf("%w: run, execution, and receipt token are required", ErrConflict)
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()

	var status, cell string
	err = tx.QueryRowContext(ctx, rebindQueryForPgx(`SELECT status, owning_cell FROM job_runs WHERE run_id = ?`+forUpdateClause()), runID).Scan(&status, &cell)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, normalizeSQLError(err)
	}

	rejected := errors.Is(err, sql.ErrNoRows)
	if !rejected {
		if cell != r.cellID {
			return false, fmt.Errorf("%w: admission belongs to another cell", ErrConflict)
		}

		rejected = isTerminalRunStatus(status)
		if !rejected {
			var current string
			if err := tx.QueryRowContext(ctx, rebindQueryForPgx(`SELECT execution_id FROM segment_executions WHERE run_id = ? AND task_id = ? ORDER BY attempt DESC, id DESC LIMIT 1`), runID, rootTaskID(runID)).Scan(&current); err != nil {
				return false, normalizeSQLError(err)
			}

			rejected = current != executionID
		}
	}

	if !rejected {
		return false, nil
	}

	if err := recordOrchestratorRetirementTx(ctx, tx, TerminalExecutionSnapshotUpdate{RunID: runID, PersistenceToken: token}, r.cellID); err != nil {
		return false, err
	}

	if err := tx.Commit(); err != nil {
		return false, err
	}

	return true, nil
}

func (r *SQLOrchestratorRetirementsRepository) ListDue(ctx context.Context, nowUnix int64, limit int) ([]OrchestratorRetirement, error) {
	if limit <= 0 || limit > 256 {
		limit = 256
	}

	rows, err := r.db.QueryContext(ctx, rebindQueryForPgx(`
		SELECT run_id, persistence_token FROM orchestrator_retirements
		WHERE cell_id = ? AND next_attempt_unix <= ?
		ORDER BY next_attempt_unix, persistence_token LIMIT ?`), r.cellID, nowUnix, limit)

	if err != nil {
		return nil, normalizeSQLError(err)
	}

	defer func() { _ = rows.Close() }()
	var result []OrchestratorRetirement
	for rows.Next() {
		var receipt OrchestratorRetirement
		if err := rows.Scan(&receipt.RunID, &receipt.PersistenceToken); err != nil {
			return nil, err
		}

		result = append(result, receipt)
	}

	return result, rows.Err()
}

func (r *SQLOrchestratorRetirementsRepository) Delete(ctx context.Context, token string) error {
	_, err := r.db.ExecContext(ctx, rebindQueryForPgx(`DELETE FROM orchestrator_retirements WHERE persistence_token = ? AND cell_id = ?`), token, r.cellID)
	return normalizeSQLError(err)
}

func (r *SQLOrchestratorRetirementsRepository) RetryAfter(ctx context.Context, token string, nextUnix int64) error {
	_, err := r.db.ExecContext(ctx, rebindQueryForPgx(`UPDATE orchestrator_retirements SET next_attempt_unix = ? WHERE persistence_token = ? AND cell_id = ?`), nextUnix, token, r.cellID)
	return normalizeSQLError(err)
}

func recordOrchestratorRetirementTx(ctx context.Context, tx *sql.Tx, update TerminalExecutionSnapshotUpdate, cellID string) error {
	token := strings.TrimSpace(update.PersistenceToken)
	if token == "" {
		return nil
	}

	_, err := tx.ExecContext(ctx, rebindQueryForPgx(`INSERT INTO orchestrator_retirements
		(persistence_token, run_id, cell_id) VALUES (?, ?, ?) ON CONFLICT (persistence_token) DO NOTHING`), token, update.RunID, cellID)
	if err != nil {
		return normalizeSQLError(err)
	}

	var existingRun, existingCell string
	if err := tx.QueryRowContext(ctx, rebindQueryForPgx(`SELECT run_id, cell_id FROM orchestrator_retirements WHERE persistence_token = ?`), token).Scan(&existingRun, &existingCell); err != nil {
		return err
	}

	if existingRun != update.RunID || existingCell != cellID {
		return fmt.Errorf("%w: persistence token belongs to another run or cell", ErrConflict)
	}

	return nil
}
