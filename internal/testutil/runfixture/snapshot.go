package runfixture

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"vectis/internal/dal"
)

func AssertTerminalSnapshotRetryFencing(t testing.TB, db *sql.DB) {
	t.Helper()
	ctx := t.Context()
	runs := dal.NewSQLRepositories(db).Runs()
	runID, _, err := runs.CreateRun(ctx, "snapshot-retry-fencing", nil, 1)
	if err != nil {
		t.Fatal(err)
	}

	snapshot := func(token string) dal.TerminalExecutionSnapshotUpdate {
		t.Helper()
		pending, err := runs.GetPendingExecution(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}

		return dal.TerminalExecutionSnapshotUpdate{
			RunID: runID, Outcome: dal.ExecutionFinalizationOutcomeRunFailed, PersistenceToken: token,
			Executions: []dal.TaskExecutionSnapshot{{
				Record: dal.TaskExecutionRecord{
					RunID: runID, TaskID: pending.TaskID, TaskKey: pending.TaskKey,
					TaskAttemptID: pending.TaskAttemptID, ExecutionID: pending.ExecutionID,
					SegmentID: pending.SegmentID, CellID: pending.CellID, Attempt: pending.Attempt,
				}, Status: dal.ExecutionStatusFailed,
			}},
		}
	}

	original := snapshot("original-receipt")
	for range 2 {
		if err := runs.ApplyTerminalExecutionSnapshot(ctx, original); err != nil {
			t.Fatalf("same-generation persistence must be idempotent: %v", err)
		}
	}

	if err := runs.RequeueRunForRetry(ctx, runID); err != nil {
		t.Fatal(err)
	}

	if err := runs.ApplyTerminalExecutionSnapshot(ctx, original); !errors.Is(err, dal.ErrConflict) {
		t.Fatalf("stale snapshot accepted before retry claim: %v", err)
	}

	retry := snapshot("retry-receipt")
	root := retry.Executions[0].Record
	claim, err := runs.TryClaimExecution(ctx, root.ExecutionID, "replacement", time.Now().Add(time.Hour))
	if err != nil || !claim.Claimed {
		t.Fatalf("retry claim = %+v, %v", claim, err)
	}

	if err := runs.ApplyTerminalExecutionSnapshot(ctx, original); !errors.Is(err, dal.ErrConflict) {
		t.Fatalf("stale snapshot error = %v, want conflict", err)
	}

	state, found, err := runs.GetRunStatus(ctx, runID)
	if err != nil || !found || state != dal.RunStatusRunning {
		t.Fatalf("stale snapshot changed retry status: %s, %v, %v", state, found, err)
	}

	var token string
	if err := db.QueryRowContext(ctx, `SELECT claim_token FROM segment_executions WHERE execution_id = $1`, root.ExecutionID).Scan(&token); err != nil || token != claim.ClaimToken {
		t.Fatalf("stale snapshot cleared retry claim: token=%q, err=%v", token, err)
	}

	if err := runs.ApplyTerminalExecutionSnapshot(ctx, retry); err != nil {
		t.Fatalf("current retry snapshot rejected: %v", err)
	}

	if err := runs.ApplyTerminalExecutionSnapshot(ctx, original); !errors.Is(err, dal.ErrConflict) {
		t.Fatalf("stale snapshot accepted after same-outcome retry: %v", err)
	}

	var executionID string
	if err := db.QueryRowContext(ctx, `SELECT execution_id FROM run_task_final_facts`).Scan(&executionID); err != nil || executionID != root.ExecutionID {
		t.Fatalf("stale snapshot changed current final facts: %q, %v", executionID, err)
	}
}
