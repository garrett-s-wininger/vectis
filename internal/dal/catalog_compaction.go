package dal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

func compactedExecutionIdentityTx(ctx context.Context, tx *sql.Tx, runID, taskID, attemptID, executionID string) (bool, error) {
	if executionID == "" {
		return false, nil
	}

	var live bool
	if err := tx.QueryRowContext(ctx, rebindQueryForPgx(
		"SELECT EXISTS (SELECT 1 FROM segment_executions WHERE execution_id = ?)",
	), executionID).Scan(&live); err != nil {
		return false, normalizeSQLError(err)
	}

	if live {
		return false, nil
	}

	var finalRunID, finalTaskID, finalAttemptID string
	err := tx.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT run_id, task_id, task_attempt_id FROM run_task_attempt_facts
		WHERE execution_id = ?
	`), executionID).Scan(&finalRunID, &finalTaskID, &finalAttemptID)

	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}

	if err != nil {
		return false, normalizeSQLError(err)
	}

	if runID != finalRunID || (taskID != "" && taskID != finalTaskID) ||
		(attemptID != "" && attemptID != finalAttemptID) {
		return false, fmt.Errorf("%w: event does not match final execution identity", ErrConflict)
	}

	return true, nil
}
