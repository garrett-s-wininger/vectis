package dal

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type SQLTaskDispatchIntentsRepository struct {
	db     *sql.DB
	cellID string
}

func (r *SQLTaskDispatchIntentsRepository) Ensure(ctx context.Context, create TaskDispatchIntentCreate) (TaskDispatchIntent, bool, error) {
	create, err := normalizeTaskDispatchIntentCreate(create, r.cellID)
	if err != nil {
		return TaskDispatchIntent{}, false, err
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return TaskDispatchIntent{}, false, err
	}
	defer func() { _ = tx.Rollback() }()

	created, err := ensureTaskDispatchIntentTx(ctx, tx, create)
	if err != nil {
		return TaskDispatchIntent{}, false, err
	}

	intent, err := getTaskDispatchIntentByExecutionIDTx(ctx, tx, create.ExecutionID)
	if err != nil {
		return TaskDispatchIntent{}, false, err
	}

	if err := tx.Commit(); err != nil {
		return TaskDispatchIntent{}, false, err
	}

	return intent, created, nil
}

func (r *SQLTaskDispatchIntentsRepository) ListPending(ctx context.Context, cellID string, cutoffUnixNano int64, limit int) ([]TaskDispatchIntent, error) {
	return r.listPending(ctx, "", cellID, cutoffUnixNano, limit)
}

func (r *SQLTaskDispatchIntentsRepository) ListPendingForRun(ctx context.Context, runID, cellID string, cutoffUnixNano int64, limit int) ([]TaskDispatchIntent, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, fmt.Errorf("%w: run_id is required", ErrConflict)
	}

	return r.listPending(ctx, runID, cellID, cutoffUnixNano, limit)
}

func (r *SQLTaskDispatchIntentsRepository) GetRunSummary(ctx context.Context, runID string) (TaskDispatchSummary, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return TaskDispatchSummary{}, fmt.Errorf("%w: run_id is required", ErrConflict)
	}

	row := r.db.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT
			COUNT(*),
			COALESCE(SUM(CASE WHEN enqueued_at IS NULL AND last_enqueue_error IS NULL THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN enqueued_at IS NULL AND last_enqueue_error IS NOT NULL THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN enqueued_at IS NOT NULL THEN 1 ELSE 0 END), 0)
		FROM task_dispatch_intents
		WHERE run_id = ?
	`), runID)

	var summary TaskDispatchSummary
	summary.RunID = runID
	if err := row.Scan(&summary.Total, &summary.Pending, &summary.Failed, &summary.Enqueued); err != nil {
		return TaskDispatchSummary{}, normalizeSQLError(err)
	}

	summary.UnknownState = summary.Total - summary.Pending - summary.Failed - summary.Enqueued
	if summary.UnknownState < 0 {
		summary.UnknownState = 0
	}

	return summary, nil
}

func (r *SQLTaskDispatchIntentsRepository) ListByRun(ctx context.Context, runID string, limit int) ([]TaskDispatchIntent, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, fmt.Errorf("%w: run_id is required", ErrConflict)
	}

	if limit <= 0 {
		limit = 100
	}

	rows, err := r.db.QueryContext(ctx, rebindQueryForPgx(`
		SELECT
			id,
			execution_id,
			run_id,
			task_id,
			task_attempt_id,
			source_execution_id,
			cell_id,
			enqueued_at,
			last_enqueue_attempt_at,
			enqueue_attempts,
			last_enqueue_error,
			created_at,
			updated_at
		FROM task_dispatch_intents
		WHERE run_id = ?
		ORDER BY
			CASE WHEN enqueued_at IS NULL THEN 0 ELSE 1 END ASC,
			CASE WHEN enqueued_at IS NULL AND last_enqueue_error IS NOT NULL THEN 0 ELSE 1 END ASC,
			id ASC
		LIMIT ?
	`), runID, limit)

	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer rows.Close()

	var out []TaskDispatchIntent
	for rows.Next() {
		rec, err := scanTaskDispatchIntent(rows)
		if err != nil {
			return nil, err
		}

		out = append(out, rec)
	}

	if err := rows.Err(); err != nil {
		return nil, normalizeSQLError(err)
	}

	return out, nil
}

func (r *SQLTaskDispatchIntentsRepository) listPending(ctx context.Context, runID, cellID string, cutoffUnixNano int64, limit int) ([]TaskDispatchIntent, error) {
	cellID = normalizeTargetCellID(cellID, r.cellID)
	if cutoffUnixNano <= 0 {
		cutoffUnixNano = time.Now().UnixNano()
	}

	if limit <= 0 {
		limit = 100
	}

	runFilter := ""
	args := []any{cellID}
	if runID != "" {
		runFilter = " AND tdi.run_id = ?"
		args = append(args, runID)
	}

	args = append(args, SegmentStatusPending, ExecutionStatusPending, TaskStatusPending, TaskStatusPending, RunStatusQueued, cutoffUnixNano, limit)

	rows, err := r.db.QueryContext(ctx, rebindQueryForPgx(`
		SELECT
			tdi.id,
			tdi.execution_id,
			tdi.run_id,
			tdi.task_id,
			tdi.task_attempt_id,
			tdi.source_execution_id,
			tdi.cell_id,
			tdi.enqueued_at,
			tdi.last_enqueue_attempt_at,
			tdi.enqueue_attempts,
			tdi.last_enqueue_error,
			tdi.created_at,
			tdi.updated_at
		FROM task_dispatch_intents tdi
		JOIN job_runs jr ON jr.run_id = tdi.run_id
		JOIN segment_executions se ON se.execution_id = tdi.execution_id
		JOIN run_segments rs ON rs.segment_id = se.segment_id AND rs.run_id = tdi.run_id
		JOIN run_tasks rt ON rt.task_id = tdi.task_id AND rt.run_id = tdi.run_id
		JOIN task_attempts ta ON ta.attempt_id = tdi.task_attempt_id AND ta.task_id = rt.task_id AND ta.run_id = tdi.run_id
		WHERE tdi.cell_id = ?
`+runFilter+`
			AND tdi.enqueued_at IS NULL
			AND rs.status = ?
			AND se.status = ?
			AND rt.status = ?
			AND ta.status = ?
			AND jr.status = ?
			AND (tdi.last_enqueue_attempt_at IS NULL OR tdi.last_enqueue_attempt_at <= ?)
		ORDER BY tdi.id ASC
		LIMIT ?
	`), args...)

	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer rows.Close()

	var out []TaskDispatchIntent
	for rows.Next() {
		rec, err := scanTaskDispatchIntent(rows)
		if err != nil {
			return nil, err
		}

		out = append(out, rec)
	}

	if err := rows.Err(); err != nil {
		return nil, normalizeSQLError(err)
	}

	return out, nil
}

func (r *SQLTaskDispatchIntentsRepository) MarkEnqueued(ctx context.Context, executionID string, enqueuedAtUnixNano int64) error {
	executionID = strings.TrimSpace(executionID)
	if executionID == "" {
		return fmt.Errorf("%w: execution_id is required", ErrConflict)
	}

	if enqueuedAtUnixNano <= 0 {
		enqueuedAtUnixNano = time.Now().UnixNano()
	}

	res, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
		UPDATE task_dispatch_intents
		SET enqueued_at = ?,
			last_enqueue_attempt_at = ?,
			enqueue_attempts = enqueue_attempts + 1,
			last_enqueue_error = NULL,
			updated_at = ?
		WHERE execution_id = ?
	`), enqueuedAtUnixNano, enqueuedAtUnixNano, enqueuedAtUnixNano, executionID)

	if err != nil {
		return normalizeSQLError(err)
	}

	return requireRowsAffected(res, "task dispatch intent", executionID)
}

func (r *SQLTaskDispatchIntentsRepository) MarkEnqueueFailed(ctx context.Context, executionID string, attemptedAtUnixNano int64, message string) error {
	executionID = strings.TrimSpace(executionID)
	if executionID == "" {
		return fmt.Errorf("%w: execution_id is required", ErrConflict)
	}

	if attemptedAtUnixNano <= 0 {
		attemptedAtUnixNano = time.Now().UnixNano()
	}

	res, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
		UPDATE task_dispatch_intents
		SET last_enqueue_attempt_at = ?,
			enqueue_attempts = enqueue_attempts + 1,
			last_enqueue_error = ?,
			updated_at = ?
		WHERE execution_id = ?
	`), attemptedAtUnixNano, truncateCellEnqueueError(message), attemptedAtUnixNano, executionID)

	if err != nil {
		return normalizeSQLError(err)
	}

	return requireRowsAffected(res, "task dispatch intent", executionID)
}

func ensureTaskDispatchIntentTx(ctx context.Context, tx *sql.Tx, create TaskDispatchIntentCreate) (bool, error) {
	create, err := normalizeTaskDispatchIntentCreate(create, DefaultCellID)
	if err != nil {
		return false, err
	}

	now := time.Now().UnixNano()
	res, err := tx.ExecContext(ctx, rebindQueryForPgx(`
		INSERT INTO task_dispatch_intents
			(execution_id, run_id, task_id, task_attempt_id, source_execution_id, cell_id, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(execution_id) DO NOTHING
	`),
		create.ExecutionID,
		create.RunID,
		create.TaskID,
		create.TaskAttemptID,
		create.SourceExecutionID,
		create.CellID,
		now,
		now,
	)

	if err != nil {
		return false, normalizeSQLError(err)
	}

	created := false
	if rows, err := res.RowsAffected(); err == nil {
		created = rows == 1
	}

	return created, nil
}

func getTaskDispatchIntentByExecutionIDTx(ctx context.Context, tx *sql.Tx, executionID string) (TaskDispatchIntent, error) {
	row := tx.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT
			id,
			execution_id,
			run_id,
			task_id,
			task_attempt_id,
			source_execution_id,
			cell_id,
			enqueued_at,
			last_enqueue_attempt_at,
			enqueue_attempts,
			last_enqueue_error,
			created_at,
			updated_at
		FROM task_dispatch_intents
		WHERE execution_id = ?
	`), executionID)

	return scanTaskDispatchIntent(row)
}

type taskDispatchIntentScanner interface {
	Scan(dest ...any) error
}

func scanTaskDispatchIntent(scanner taskDispatchIntentScanner) (TaskDispatchIntent, error) {
	var rec TaskDispatchIntent
	var enqueuedAt sql.NullInt64
	var lastAttemptAt sql.NullInt64
	var lastError sql.NullString
	if err := scanner.Scan(
		&rec.ID,
		&rec.ExecutionID,
		&rec.RunID,
		&rec.TaskID,
		&rec.TaskAttemptID,
		&rec.SourceExecutionID,
		&rec.CellID,
		&enqueuedAt,
		&lastAttemptAt,
		&rec.EnqueueAttempts,
		&lastError,
		&rec.CreatedAt,
		&rec.UpdatedAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return TaskDispatchIntent{}, fmt.Errorf("%w: task dispatch intent", ErrNotFound)
		}

		return TaskDispatchIntent{}, normalizeSQLError(err)
	}

	rec.EnqueuedAt = nullInt64Ptr(enqueuedAt)
	rec.LastEnqueueAttemptAt = nullInt64Ptr(lastAttemptAt)
	rec.LastEnqueueError = nullStringPtr(lastError)
	return rec, nil
}

func normalizeTaskDispatchIntentCreate(create TaskDispatchIntentCreate, fallbackCellID string) (TaskDispatchIntentCreate, error) {
	create.ExecutionID = strings.TrimSpace(create.ExecutionID)
	create.RunID = strings.TrimSpace(create.RunID)
	create.TaskID = strings.TrimSpace(create.TaskID)
	create.TaskAttemptID = strings.TrimSpace(create.TaskAttemptID)
	create.SourceExecutionID = strings.TrimSpace(create.SourceExecutionID)
	create.CellID = normalizeTargetCellID(create.CellID, fallbackCellID)

	if create.ExecutionID == "" {
		return TaskDispatchIntentCreate{}, fmt.Errorf("%w: execution_id is required", ErrConflict)
	}

	if create.RunID == "" {
		return TaskDispatchIntentCreate{}, fmt.Errorf("%w: run_id is required", ErrConflict)
	}

	if create.TaskID == "" {
		return TaskDispatchIntentCreate{}, fmt.Errorf("%w: task_id is required", ErrConflict)
	}

	if create.TaskAttemptID == "" {
		return TaskDispatchIntentCreate{}, fmt.Errorf("%w: task_attempt_id is required", ErrConflict)
	}

	return create, nil
}

var _ TaskDispatchIntentsRepository = (*SQLTaskDispatchIntentsRepository)(nil)
