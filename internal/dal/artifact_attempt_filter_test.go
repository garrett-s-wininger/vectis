package dal_test

import (
	"context"
	"testing"
	"time"

	"vectis/internal/dal"
	"vectis/internal/testutil/dbtest"
)

func TestArtifactAttemptFilterUsesLiveAndArchivedIdentityBeforePagination(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()
	runID := createArtifactTestRun(t, ctx, repos, "artifact-attempts")
	first, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}

	publish := func(name, attemptID, executionID string) {
		t.Helper()
		if _, err := repos.Artifacts().Record(ctx, dal.ArtifactCreate{RunID: runID, Name: name,
			TaskID: first.TaskID, TaskAttemptID: attemptID, ExecutionID: executionID,
			BlobKey: "sha256:aaaa", BlobAlgorithm: "sha256", BlobDigest: "aaaa", SizeBytes: 4, ArtifactShardID: "local"}); err != nil {
			t.Fatal(err)
		}
	}

	publish("first-a", first.TaskAttemptID, first.ExecutionID)
	publish("first-b", first.TaskAttemptID, first.ExecutionID)
	now := time.Now().UnixNano()

	if err := repos.Runs().ApplyTerminalExecutionSnapshot(ctx, dal.TerminalExecutionSnapshotUpdate{RunID: runID,
		Outcome: dal.ExecutionFinalizationOutcomeRunFailed, Executions: []dal.TaskExecutionSnapshot{{
			Record: dal.TaskExecutionRecord{RunID: runID, TaskID: first.TaskID, TaskKey: dal.RootTaskKey, Name: "root",
				TaskAttemptID: first.TaskAttemptID, ExecutionID: first.ExecutionID, SegmentID: first.SegmentID, CellID: first.CellID, Attempt: 1},
			Status: dal.ExecutionStatusFailed, AcceptedAtUnixNano: now, StartedAtUnixNano: now, FinishedAtUnixNano: now + 1,
		}}}); err != nil {
		t.Fatal(err)
	}

	if err := repos.Runs().RequeueRunForRetry(ctx, runID); err != nil {
		t.Fatal(err)
	}

	second, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}

	if second.Attempt != 2 {
		t.Fatalf("expected second run attempt, got %d", second.Attempt)
	}

	publish("second", second.TaskAttemptID, second.ExecutionID)
	publish("second-execution-only", "", second.ExecutionID)
	publish("unattributed", "", "")

	rows, cursor, err := repos.Artifacts().ListByRunFiltered(ctx, runID, 0, 1, dal.ArtifactListFilter{Attempt: 1})
	if err != nil || len(rows) != 1 || rows[0].Name != "first-a" || cursor == 0 {
		t.Fatalf("first page: %+v cursor=%d err=%v", rows, cursor, err)
	}

	rows, cursor, err = repos.Artifacts().ListByRunFiltered(ctx, runID, cursor, 1, dal.ArtifactListFilter{Attempt: 1})
	if err != nil || len(rows) != 1 || rows[0].Name != "first-b" || cursor != 0 {
		t.Fatalf("second page: %+v cursor=%d err=%v", rows, cursor, err)
	}

	for _, tc := range []struct{ attempt, count int }{{1, 2}, {2, 2}, {3, 0}, {0, 5}} {
		rows, _, err := repos.Artifacts().ListByRunFiltered(ctx, runID, 0, 100, dal.ArtifactListFilter{Attempt: tc.attempt, TaskID: first.TaskID})
		if err != nil || len(rows) != tc.count {
			t.Fatalf("attempt %d: count=%d expected=%d err=%v", tc.attempt, len(rows), tc.count, err)
		}
	}

	rows, _, err = repos.Artifacts().ListByRunFiltered(ctx, runID, 0, 100, dal.ArtifactListFilter{Attempt: 1, TaskAttemptID: second.TaskAttemptID})
	if err != nil || len(rows) != 0 {
		t.Fatalf("conflicting attempt filters returned artifacts: %+v, %v", rows, err)
	}
}
