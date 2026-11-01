package dal_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"vectis/internal/dal"
	"vectis/internal/testutil/dbtest"
)

func TestTaskAttemptHistorySurvivesCompactionAndRequeue(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositoriesWithCellID(db, "local")
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, "history", `{"root":{"uses":"builtins/script"}}`); err != nil {
		t.Fatal(err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, "history", nil, 1)
	if err != nil {
		t.Fatal(err)
	}

	childID := runID + ":child"
	var first dal.TerminalExecutionSnapshotUpdate
	for generation := 1; generation <= 2; generation++ {
		if generation == 2 {
			if err := repos.Runs().RequeueRunForRetry(ctx, runID); err != nil {
				t.Fatal(err)
			}
		}

		if generation == 2 {
			if err := repos.Runs().ApplyRunStatusUpdate(ctx, dal.RunStatusUpdate{RunID: runID, Status: dal.RunStatusFailed, RootExecutionID: first.Executions[0].Record.ExecutionID}); err != nil {
				t.Fatal(err)
			}

			current, err := repos.Runs().GetRun(ctx, runID)
			if err != nil || current.Status != dal.RunStatusQueued {
				t.Fatalf("old catalog status overwrote retry: %+v, %v", current, err)
			}
		}

		root, err := repos.Runs().GetPendingExecution(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}

		childAttempt := fmt.Sprintf("%s:attempt:%d", childID, generation)
		status, outcome := dal.ExecutionStatusFailed, dal.ExecutionFinalizationOutcomeRunFailed
		if generation == 2 {
			status, outcome = dal.ExecutionStatusSucceeded, dal.ExecutionFinalizationOutcomeRunSucceeded
		}

		now := time.Date(2026, 1, generation, 0, 0, 0, 0, time.UTC).UnixNano()
		snapshot := dal.TerminalExecutionSnapshotUpdate{RunID: runID, Outcome: outcome, Executions: []dal.TaskExecutionSnapshot{
			{Record: dal.TaskExecutionRecord{RunID: runID, TaskID: root.TaskID, TaskKey: dal.RootTaskKey, Name: "root",
				TaskAttemptID: root.TaskAttemptID, ExecutionID: root.ExecutionID, SegmentID: root.SegmentID, CellID: "local", Attempt: generation},
				Status: status, AcceptedAtUnixNano: now, StartedAtUnixNano: now, FinishedAtUnixNano: now + 100},
			{Record: dal.TaskExecutionRecord{RunID: runID, TaskID: childID, ParentTaskID: root.TaskID, TaskKey: "child", Name: "child",
				TaskAttemptID: childAttempt, ExecutionID: childAttempt + ":execution", CellID: "local", Attempt: generation},
				Status: status, AcceptedAtUnixNano: now, StartedAtUnixNano: now, FinishedAtUnixNano: now + 100},
		}}

		if generation == 1 {
			first = snapshot
		}

		if err := repos.Runs().ApplyTerminalExecutionSnapshot(ctx, snapshot); err != nil {
			t.Fatal(err)
		}

		tampered := snapshot
		tampered.Executions = append([]dal.TaskExecutionSnapshot(nil), snapshot.Executions...)
		tampered.Executions[1].Record.ExecutionID += ":forged"
		if err := repos.Runs().ApplyTerminalExecutionSnapshot(ctx, tampered); !dal.IsConflict(err) {
			t.Fatalf("archive identity changed: %v", err)
		}

		if generation == 1 {
			otherRun, _, err := repos.Runs().CreateRun(ctx, "history", nil, 2)
			if err != nil {
				t.Fatal(err)
			}

			otherRoot, err := repos.Runs().GetPendingExecution(ctx, otherRun)
			if err != nil {
				t.Fatal(err)
			}

			tampered.Executions[1].Record.ExecutionID = otherRoot.ExecutionID
			if err := repos.Runs().ApplyTerminalExecutionSnapshot(ctx, tampered); !dal.IsConflict(err) {
				t.Fatalf("snapshot stole another live execution: %v", err)
			}
		}

		if err := repos.Runs().ApplyTerminalExecutionSnapshot(ctx, snapshot); err != nil {
			t.Fatal(err)
		}
	}

	repos = dal.NewSQLRepositoriesWithCellID(db, "local")
	var cursor int64
	total := 0
	for {
		tasks, next, err := repos.Runs().ListRunTasks(ctx, runID, cursor, 1)
		if err != nil {
			t.Fatal(err)
		}

		for _, task := range tasks {
			total++
			if len(task.Attempts) != 2 {
				t.Fatalf("task %s lost history: %+v", task.TaskKey, task.Attempts)
			}

			old, latest := task.Attempts[0], task.Attempts[1]
			if old.Attempt != 1 || old.ExecutionStatus != dal.ExecutionStatusFailed || latest.Attempt != 2 || latest.ExecutionStatus != dal.ExecutionStatusSucceeded {
				t.Fatalf("wrong history: %+v", task.Attempts)
			}

			if old.ExecutionID == latest.ExecutionID || old.StartedAt == nil || latest.StartedAt == nil || *old.StartedAt == *latest.StartedAt {
				t.Fatalf("execution identity or timestamps were overwritten: %+v", task.Attempts)
			}
		}

		if next == 0 {
			break
		}

		cursor = next
	}

	if total != 2 {
		t.Fatalf("pagination duplicated tasks: %d", total)
	}

	completion, err := repos.Runs().GetRunTaskCompletion(ctx, runID)
	if err != nil || completion.Total != 2 || completion.Succeeded != 2 || completion.TerminalFailed != 0 {
		t.Fatalf("history changed latest completion: %+v, %v", completion, err)
	}

	if err := repos.Runs().ApplyTerminalExecutionSnapshot(ctx, first); !dal.IsConflict(err) {
		t.Fatalf("stale generation accepted: %v", err)
	}

	oldChild := first.Executions[1].Record.ExecutionID
	if err := repos.Runs().ApplyExecutionStatusUpdate(ctx, dal.ExecutionStatusUpdate{ExecutionID: oldChild, Status: dal.ExecutionStatusRunning}); err != nil {
		t.Fatal(err)
	}

	if err := repos.Runs().(interface {
		VerifyCatalogExecutionSourceCell(context.Context, string, string) error
	}).VerifyCatalogExecutionSourceCell(ctx, oldChild, "other"); !dal.IsConflict(err) {
		t.Fatalf("history ownership bypass: %v", err)
	}

	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM segment_executions WHERE run_id = ? AND task_id = ?", runID, childID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("live child rows resurrected: %d, %v", count, err)
	}

	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM run_task_attempt_facts WHERE run_id = ?", runID).Scan(&count); err != nil || count != 4 {
		t.Fatalf("archive rows: %d, %v", count, err)
	}

	if _, err := db.ExecContext(ctx, "DELETE FROM job_runs WHERE run_id = ?", runID); err != nil {
		t.Fatal(err)
	}

	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM run_task_attempt_facts WHERE run_id = ?", runID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("retention did not cascade: %d, %v", count, err)
	}
}
