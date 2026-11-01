package reconciler

import (
	"context"
	"testing"
	"time"

	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/dbtest"
)

func TestProcessDoesNotFinalizeRunningCatalogProjection(t *testing.T) {
	ctx := context.Background()
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	runID, _, err := repos.Runs().CreateRun(ctx, "distributed", nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	root, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	// Distributed containers finish before their children. The global database
	// sees only the completed root until the cell publishes the final snapshot.
	if err := repos.Runs().ApplyExecutionStatusUpdate(ctx, dal.ExecutionStatusUpdate{ExecutionID: root.ExecutionID, Status: dal.ExecutionStatusSucceeded}); err != nil {
		t.Fatal(err)
	}
	if err := repos.Runs().ApplyRunStatusUpdate(ctx, dal.RunStatusUpdate{RunID: runID, Status: dal.RunStatusRunning, RootExecutionID: root.ExecutionID}); err != nil {
		t.Fatal(err)
	}
	queue := mocks.NewMockQueueService()
	svc := NewService(interfaces.NewLogger("test"), db, queue, interfaces.SystemClock{})
	for range 2 {
		if err := svc.Process(ctx); err != nil {
			t.Fatal(err)
		}
		status, _, err := repos.Runs().GetRunStatus(ctx, runID)
		if err != nil || status != dal.RunStatusRunning {
			t.Fatalf("projection status=%s error=%v; must await authoritative cell result", status, err)
		}
	}
	if err := repos.Runs().ApplyRunStatusUpdate(ctx, dal.RunStatusUpdate{RunID: runID, Status: dal.RunStatusFailed, RootExecutionID: root.ExecutionID}); err != nil {
		t.Fatalf("cell failure must remain applicable: %v", err)
	}
	if len(queue.GetJobs()) != 0 {
		t.Fatal("global projection was redispatched")
	}
}

func TestProcessStillRecoversExpiredHotStateOwnership(t *testing.T) {
	ctx := context.Background()
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	runID, _, err := repos.Runs().CreateRun(ctx, "expired", nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := repos.Runs().UpsertRunHotStateOwner(ctx, dal.RunHotStateOwnerUpdate{
		RunID: runID, CellID: "local", OwnerID: "orchestrator", OwnerEpoch: "epoch", LeaseUntil: time.Now().Add(-time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	if err := repos.Runs().ApplyRunStatusUpdate(ctx, dal.RunStatusUpdate{RunID: runID, Status: dal.RunStatusRunning}); err != nil {
		t.Fatal(err)
	}
	svc := NewService(interfaces.NewLogger("test"), db, mocks.NewMockQueueService(), interfaces.SystemClock{})
	if err := svc.Process(ctx); err != nil {
		t.Fatal(err)
	}
	status, _, err := repos.Runs().GetRunStatus(ctx, runID)
	if err != nil || status != dal.RunStatusOrphaned {
		t.Fatalf("expired local owner status=%s error=%v", status, err)
	}
}
