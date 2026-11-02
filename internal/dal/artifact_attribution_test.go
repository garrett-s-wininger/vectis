package dal_test

import (
	"context"
	"testing"
	"time"

	"vectis/internal/dal"
	"vectis/internal/testutil/dbtest"
)

func TestArtifactAttributionSurvivesCompactionAndLateCatalogDelivery(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()
	runID := createArtifactTestRun(t, ctx, repos, "artifact-history")
	root, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}

	child, _, err := repos.Runs().EnsurePendingTaskExecution(ctx, dal.TaskExecutionCreate{RunID: runID, TaskKey: "report", Name: "Report"})
	if err != nil {
		t.Fatal(err)
	}

	create := dal.ArtifactCreate{RunID: runID, TaskID: child.TaskID, TaskAttemptID: child.TaskAttemptID, ExecutionID: child.ExecutionID,
		Name: "report", Path: "report.txt", BlobKey: "sha256:aaaa", BlobAlgorithm: "sha256", BlobDigest: "aaaa", SizeBytes: 4, ArtifactShardID: "artifact-1"}
	if _, err := repos.Artifacts().Record(ctx, create); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UnixNano()
	snapshot := dal.TerminalExecutionSnapshotUpdate{RunID: runID, Outcome: dal.ExecutionFinalizationOutcomeRunSucceeded, Executions: []dal.TaskExecutionSnapshot{
		{Record: dal.TaskExecutionRecord{RunID: runID, TaskID: root.TaskID, TaskKey: dal.RootTaskKey, Name: "root", TaskAttemptID: root.TaskAttemptID, ExecutionID: root.ExecutionID, SegmentID: root.SegmentID, CellID: root.CellID, Attempt: root.Attempt},
			Status: dal.ExecutionStatusSucceeded, AcceptedAtUnixNano: now, StartedAtUnixNano: now, FinishedAtUnixNano: now + 1},
		{Record: child, Status: dal.ExecutionStatusSucceeded, AcceptedAtUnixNano: now, StartedAtUnixNano: now, FinishedAtUnixNano: now + 1},
	}}

	if err := repos.Runs().ApplyTerminalExecutionSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}

	var live int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM segment_executions WHERE execution_id = ?", child.ExecutionID).Scan(&live); err != nil || live != 0 {
		t.Fatalf("child execution not compacted: %d, %v", live, err)
	}

	assertProducer := func(name string) {
		t.Helper()
		rec, err := repos.Artifacts().GetByRunAndName(ctx, runID, name)
		if err != nil {
			t.Fatal(err)
		}

		if rec.TaskID == nil || *rec.TaskID != child.TaskID || rec.TaskAttemptID == nil || *rec.TaskAttemptID != child.TaskAttemptID || rec.ExecutionID == nil || *rec.ExecutionID != child.ExecutionID {
			t.Fatalf("lost producer: %+v", rec)
		}
	}

	assertProducer("report")
	create.Name = "late-report"
	if _, err := repos.Artifacts().Record(ctx, create); err != nil {
		t.Fatal(err)
	}

	assertProducer("late-report")
	for _, filter := range []dal.ArtifactListFilter{{TaskID: child.TaskID}, {TaskAttemptID: child.TaskAttemptID}, {ExecutionID: child.ExecutionID}, {Attempt: 1}, {Attempt: 1, TaskID: child.TaskID}} {
		rows, _, err := repos.Artifacts().ListByRunFiltered(ctx, runID, 0, 100, filter)
		if err != nil || len(rows) != 2 {
			t.Fatalf("filtered artifact history: %d, %v", len(rows), err)
		}
	}

	otherRun := createArtifactTestRun(t, ctx, repos, "other-artifact-history")
	for _, tc := range []struct{ name, runID, taskID, attemptID, executionID string }{
		{"wrong run", otherRun, child.TaskID, child.TaskAttemptID, child.ExecutionID},
		{"wrong task", runID, root.TaskID, child.TaskAttemptID, child.ExecutionID},
		{"wrong attempt", runID, child.TaskID, root.TaskAttemptID, child.ExecutionID},
		{"unknown execution", runID, child.TaskID, child.TaskAttemptID, "missing"},
		{"foreign task only", otherRun, child.TaskID, "", ""},
		{"foreign attempt only", otherRun, "", child.TaskAttemptID, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := create
			candidate.RunID, candidate.TaskID, candidate.TaskAttemptID, candidate.ExecutionID = tc.runID, tc.taskID, tc.attemptID, tc.executionID
			if _, err := repos.Artifacts().Record(ctx, candidate); !dal.IsConflict(err) {
				t.Fatalf("invalid producer accepted: %v", err)
			}
		})
	}
}
