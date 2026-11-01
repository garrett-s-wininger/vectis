package dal

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"
)

func archiveTerminalSnapshotAttemptsTx(ctx context.Context, tx *sql.Tx, rows []terminalSnapshotRow) error {
	if err := verifyArchivedAttemptIdentitiesTx(ctx, tx, rows); err != nil {
		return err
	}

	values := make([][]any, 0, len(rows))
	for _, row := range rows {
		accepted, started, finished, observed := terminalSnapshotFactTimes(row, time.Now().UnixNano())
		sequence := row.eventSequence
		if sequence < 1 {
			sequence = 1
		}

		values = append(values, []any{row.runID, row.taskID, row.taskAttemptID, row.executionID, row.cellID,
			row.status, row.attempt, nullableInt64(accepted), nullableInt64(started), nullableInt64(finished), nullableInt64(observed), sequence})
	}

	return execBatchedValuesTx(ctx, tx, `INSERT INTO run_task_attempt_facts
 (run_id, task_id, task_attempt_id, execution_id, cell_id, status, attempt,
 accepted_at_unix_nano, started_at_unix_nano, finished_at_unix_nano, last_observed_at, event_sequence)
 VALUES `, values, ` ON CONFLICT(task_attempt_id) DO NOTHING`)
}

func (r *SQLRunsRepository) ListRunTaskAttemptHistory(ctx context.Context, runID string, taskIDs []string) ([]TaskAttemptRecord, error) {
	out := []TaskAttemptRecord{}
	for start := 0; start < len(taskIDs); start += 200 {
		end := min(start+200, len(taskIDs))
		args := []any{runID}
		for _, id := range taskIDs[start:end] {
			args = append(args, id)
		}

		placeholders := strings.TrimSuffix(strings.Repeat("?,", end-start), ",")
		if err := func() error {
			rows, err := r.db.QueryContext(ctx, rebindQueryForPgx(`
   SELECT task_attempt_id, task_id, run_id, execution_id, cell_id, status, attempt,
   accepted_at_unix_nano, started_at_unix_nano, finished_at_unix_nano, last_observed_at, event_sequence
   FROM run_task_attempt_facts WHERE run_id = ? AND task_id IN (`+placeholders+`)`), args...)

			if err != nil {
				return normalizeSQLError(err)
			}
			defer func() { _ = rows.Close() }()

			for rows.Next() {
				var a TaskAttemptRecord
				var accepted, started, finished sql.NullInt64
				if err := rows.Scan(&a.AttemptID, &a.TaskID, &a.RunID, &a.ExecutionID, &a.CellID, &a.Status, &a.Attempt,
					&accepted, &started, &finished, &a.LastObservedAt, &a.EventSequence); err != nil {
					return normalizeSQLError(err)
				}

				a.ExecutionStatus = a.Status
				a.AcceptedAt, a.StartedAt, a.FinishedAt = unixNanoStringPtr(accepted), unixNanoStringPtr(started), unixNanoStringPtr(finished)
				out = append(out, a)
			}

			return normalizeSQLError(rows.Err())
		}(); err != nil {
			return nil, err
		}

		if err := func() error {
			rows, err := r.db.QueryContext(ctx, rebindQueryForPgx(`
   SELECT ta.attempt_id, ta.task_id, ta.run_id, se.execution_id, ta.cell_id, ta.status, se.status, ta.attempt,
   CAST(ta.accepted_at AS TEXT), CAST(ta.started_at AS TEXT), CAST(ta.finished_at AS TEXT), ta.last_observed_at, ta.event_sequence
   FROM task_attempts ta JOIN segment_executions se ON se.task_attempt_id = ta.attempt_id
   WHERE ta.run_id = ? AND ta.task_id IN (`+placeholders+`)`), args...)

			if err != nil {
				return normalizeSQLError(err)
			}
			defer func() { _ = rows.Close() }()

			for rows.Next() {
				var a TaskAttemptRecord
				var accepted, started, finished sql.NullString
				if err := rows.Scan(&a.AttemptID, &a.TaskID, &a.RunID, &a.ExecutionID, &a.CellID, &a.Status, &a.ExecutionStatus, &a.Attempt,
					&accepted, &started, &finished, &a.LastObservedAt, &a.EventSequence); err != nil {
					return normalizeSQLError(err)
				}

				a.AcceptedAt, a.StartedAt, a.FinishedAt = nullStringPtr(accepted), nullStringPtr(started), nullStringPtr(finished)
				out = append(out, a)
			}

			return normalizeSQLError(rows.Err())
		}(); err != nil {
			return nil, err
		}

	}

	return mergeTaskAttemptHistory(nil, out), nil
}

func mergeTaskAttemptHistory(history, current []TaskAttemptRecord) []TaskAttemptRecord {
	byID := make(map[string]TaskAttemptRecord, len(history)+len(current))
	for _, a := range history {
		byID[a.AttemptID] = a
	}

	for _, a := range current {
		byID[a.AttemptID] = a
	}

	out := make([]TaskAttemptRecord, 0, len(byID))
	for _, a := range byID {
		out = append(out, a)
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].TaskID != out[j].TaskID {
			return out[i].TaskID < out[j].TaskID
		}

		if out[i].Attempt != out[j].Attempt {
			return out[i].Attempt < out[j].Attempt
		}

		return out[i].AttemptID < out[j].AttemptID
	})

	return out
}

func (r *SQLRunsRepository) attachRunTaskAttemptHistory(ctx context.Context, runID string, tasks []TaskRecord) error {
	ids := make([]string, 0, len(tasks))
	for _, task := range tasks {
		ids = append(ids, task.TaskID)
	}

	history, err := r.ListRunTaskAttemptHistory(ctx, runID, ids)
	if err != nil {
		return err
	}

	byTask := map[string][]TaskAttemptRecord{}
	for _, a := range history {
		byTask[a.TaskID] = append(byTask[a.TaskID], a)
	}

	for i := range tasks {
		tasks[i].Attempts = mergeTaskAttemptHistory(byTask[tasks[i].TaskID], tasks[i].Attempts)
	}

	return nil
}

func verifyArchivedAttemptIdentitiesTx(ctx context.Context, tx *sql.Tx, incoming []terminalSnapshotRow) error {
	byAttempt := map[string]terminalSnapshotRow{}
	byExecution := map[string]terminalSnapshotRow{}
	matches := func(a, b terminalSnapshotRow) bool {
		return a.runID == b.runID && a.taskID == b.taskID && a.taskAttemptID == b.taskAttemptID && a.executionID == b.executionID && a.cellID == b.cellID && a.attempt == b.attempt && a.status == b.status
	}

	for _, row := range incoming {
		if old, ok := byAttempt[row.taskAttemptID]; ok && !matches(old, row) {
			return fmt.Errorf("%w: duplicate attempt identity", ErrConflict)
		}

		if old, ok := byExecution[row.executionID]; ok && !matches(old, row) {
			return fmt.Errorf("%w: duplicate execution identity", ErrConflict)
		}

		byAttempt[row.taskAttemptID], byExecution[row.executionID] = row, row
	}

	for start := 0; start < len(incoming); start += 200 {
		end := min(start+200, len(incoming))
		args := make([]any, 0, 2*(end-start))
		for _, row := range incoming[start:end] {
			args = append(args, row.taskAttemptID)
		}

		for _, row := range incoming[start:end] {
			args = append(args, row.executionID)
		}

		placeholders := strings.TrimSuffix(strings.Repeat("?,", end-start), ",")
		if err := func() error {
			rows, err := tx.QueryContext(ctx, rebindQueryForPgx(`SELECT run_id, task_id, task_attempt_id, execution_id, cell_id, attempt, status, TRUE
   FROM run_task_attempt_facts WHERE task_attempt_id IN (`+placeholders+`) OR execution_id IN (`+placeholders+`)
   UNION ALL
   SELECT run_id, task_id, task_attempt_id, execution_id, cell_id, attempt, status, FALSE
   FROM segment_executions WHERE task_attempt_id IN (`+placeholders+`) OR execution_id IN (`+placeholders+`)`), append(args, args...)...)

			if err != nil {
				return normalizeSQLError(err)
			}
			defer func() { _ = rows.Close() }()

			for rows.Next() {
				var old terminalSnapshotRow
				var archived bool
				if err := rows.Scan(&old.runID, &old.taskID, &old.taskAttemptID, &old.executionID, &old.cellID, &old.attempt, &old.status, &archived); err != nil {
					return normalizeSQLError(err)
				}

				a, okA := byAttempt[old.taskAttemptID]
				b, okB := byExecution[old.executionID]
				oldA, oldB := old, old
				if !archived {
					oldA.status, oldB.status = a.status, b.status
				}

				if (okA && !matches(oldA, a)) || (okB && !matches(oldB, b)) {
					return fmt.Errorf("%w: archived attempt identity or outcome changed", ErrConflict)
				}
			}

			return normalizeSQLError(rows.Err())
		}(); err != nil {
			return err
		}
	}

	return nil
}
