package dal

import (
	"context"
	"database/sql"
	"fmt"
)

func lockRunAttemptTx(ctx context.Context, tx *sql.Tx, runID, rootExecutionID string) error {
	if rootExecutionID == "" {
		return fmt.Errorf("%w: root execution identity is required", ErrConflict)
	}

	if err := lockRunForFinalizationTx(ctx, tx, runID); err != nil {
		return err
	}

	var current, status string
	if err := tx.QueryRowContext(ctx, rebindQueryForPgx(`SELECT status,
 (SELECT execution_id FROM segment_executions WHERE run_id = ? AND task_id = ? ORDER BY attempt DESC, id DESC LIMIT 1)
 FROM job_runs WHERE run_id = ?`), runID, rootTaskID(runID), runID).Scan(&status, &current); err != nil {
		return normalizeSQLError(err)
	}

	if current != rootExecutionID || !statusIn(status, []string{RunStatusQueued, RunStatusRunning, RunStatusOrphaned}) {
		return fmt.Errorf("%w: run attempt is superseded or terminal", ErrConflict)
	}

	return nil
}

func (r *SQLRunsRepository) ReportRunAttempt(ctx context.Context, runID, rootExecutionID, status, failureCode, reason string) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if err := lockRunAttemptTx(ctx, tx, runID, rootExecutionID); err != nil {
		return err
	}

	switch status {
	case RunStatusFailed:
		if err := markRunTerminalTx(ctx, tx, runID, status, failureCode, reason); err != nil {
			return err
		}
	case RunStatusOrphaned:
		if _, err := tx.ExecContext(ctx, rebindQueryForPgx(`UPDATE job_runs SET status = ?, failure_reason = ?, orphan_reason = ?, failure_code = '', lease_owner = NULL, lease_until = NULL WHERE run_id = ?`), status, reason, classifyOrphanReason(reason), runID); err != nil {
			return normalizeSQLError(err)
		}
	default:
		return fmt.Errorf("%w: unsupported worker run report", ErrConflict)
	}

	if err := clearRunHotStateOwnerTx(ctx, tx, runID); err != nil {
		return err
	}

	return tx.Commit()
}

func (r *SQLRunsRepository) PersistTaskContinuation(ctx context.Context, runID, rootExecutionID, sourceExecutionID string, children []ContinuationTask) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if err := lockRunAttemptTx(ctx, tx, runID, rootExecutionID); err != nil {
		return err
	}

	cancelled, _, err := getRunCancelIntentTx(ctx, tx, runID)
	if err != nil {
		return err
	}

	if cancelled {
		return ErrRunCancelRequested
	}

	var sourceRunID string
	if err := tx.QueryRowContext(ctx, rebindQueryForPgx("SELECT run_id FROM segment_executions WHERE execution_id = ?"), sourceExecutionID).Scan(&sourceRunID); err != nil {
		return normalizeSQLError(err)
	}

	if sourceRunID != runID {
		return fmt.Errorf("%w: source belongs to another run", ErrConflict)
	}

	if err := applyExecutionStatusUpdateTx(ctx, tx, ExecutionStatusUpdate{ExecutionID: sourceExecutionID, Status: ExecutionStatusSucceeded}); err != nil {
		return err
	}

	for _, child := range children {
		if child.Create.RunID != runID {
			return fmt.Errorf("%w: child belongs to another run", ErrConflict)
		}

		record, _, err := ensureTaskExecutionTx(ctx, tx, child.Create, TaskStatusPending, SegmentStatusPending, ExecutionStatusPending)
		if err != nil {
			return err
		}

		if child.ExecutionID == "" || record.ExecutionID != child.ExecutionID {
			return fmt.Errorf("%w: child execution identity differs", ErrConflict)
		}

		snapshot, err := taskExecutionStatusSnapshotByTaskIDTx(ctx, tx, record.TaskID)
		if err != nil {
			return err
		}

		if snapshot.hasStatuses(TaskStatusPlanned, SegmentStatusPlanned, ExecutionStatusPlanned) {
			if _, err := activatePlannedTaskExecutionSnapshotTx(ctx, tx, snapshot); err != nil {
				return err
			}
		}
	}

	if err := markRunQueuedForContinuationTx(ctx, tx, runID); err != nil {
		return err
	}

	return tx.Commit()
}
