package dal_test

import (
	"context"
	"testing"
	"time"

	"vectis/internal/dal"
	"vectis/internal/testutil/dbtest"
)

func TestQueuedDispatchCandidatesExcludeAcceptedCellWork(t *testing.T) {
	ctx := context.Background()
	runs := dal.NewSQLRepositories(dbtest.NewTestDB(t)).Runs()
	var pendingRun string
	for _, status := range []string{dal.ExecutionStatusPending, dal.ExecutionStatusAccepted, dal.ExecutionStatusRunning, dal.ExecutionStatusSucceeded} {
		runID, _, err := runs.CreateRun(ctx, "job-"+status, nil, 1)
		if err != nil {
			t.Fatal(err)
		}
		execution, err := runs.GetPendingExecution(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		if status == dal.ExecutionStatusPending {
			pendingRun = runID
		} else if err := runs.ApplyExecutionStatusUpdate(ctx, dal.ExecutionStatusUpdate{ExecutionID: execution.ExecutionID, Status: status}); err != nil {
			t.Fatal(err)
		}
	}
	candidates, err := runs.ListQueuedBeforeDispatchCutoffLimit(ctx, time.Now().Add(time.Minute).Unix(), 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 1 || candidates[0].RunID != pendingRun {
		t.Fatalf("only pending execution should be redispatched, got %+v", candidates)
	}
}
