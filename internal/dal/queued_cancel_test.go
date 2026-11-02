package dal_test

import (
	"context"
	"testing"
	"time"

	"vectis/internal/dal"
	"vectis/internal/testutil/dbtest"
)

func TestQueuedCancellationSurvivesExecutionClaim(t *testing.T) {
	ctx := context.Background()
	runs := dal.NewSQLRepositories(dbtest.NewTestDB(t)).Runs()
	runID, _, err := runs.CreateRun(ctx, "cancel-before-start", nil, 1)
	if err != nil {
		t.Fatal(err)
	}

	execution, err := runs.GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := runs.RequestRunCancel(ctx, runID, dal.CancelReasonAPI); err != nil {
		t.Fatal(err)
	}

	claim, err := runs.TryClaimExecution(ctx, execution.ExecutionID, "worker", time.Now().Add(time.Minute))
	if err != nil || !claim.Claimed {
		t.Fatalf("claim=%+v error=%v", claim, err)
	}

	requested, err := runs.RunCancelRequested(ctx, runID)
	if err != nil || !requested {
		t.Fatalf("queued intent lost on claim: %v, %v", requested, err)
	}
}
