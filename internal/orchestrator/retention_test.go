package orchestrator_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/dal"
	"vectis/internal/orchestrator"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func completeRetainedRun(t *testing.T, svc *orchestrator.Service, runID string) (dal.ExecutionFinalizationResult, string) {
	t.Helper()
	ctx := t.Context()

	loaded, err := svc.LoadRun(ctx, orchestrator.RunSpec{RunID: runID})
	if err != nil {
		t.Fatal(err)
	}

	claim, err := svc.ClaimExecution(ctx, runID, loaded.Root.ExecutionID, "worker", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}

	result, err := svc.CompleteExecutionByClaim(ctx, runID, loaded.Root.ExecutionID, "worker", claim.ClaimToken, dal.ExecutionStatusSucceeded, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if result.PersistenceToken == "" {
		t.Fatal("terminal completion has no persistence token")
	}

	return result, claim.ClaimToken
}

func TestServiceReclaimsOnlyAcknowledgedGenerations(t *testing.T) {
	svc := orchestrator.New(2)
	t.Cleanup(svc.Close)

	ctx := t.Context()
	first, claimToken := completeRetainedRun(t, svc, "retained")
	replayed, err := svc.CompleteExecutionByClaim(ctx, first.RunID, first.ExecutionID, "worker", claimToken, dal.ExecutionStatusSucceeded, "", "")
	if err != nil || !reflect.DeepEqual(first, replayed) {
		t.Fatalf("completion replay = %+v, %v", replayed, err)
	}

	if err := svc.AcknowledgeRunPersistence(ctx, first.RunID, "wrong-token"); err != nil {
		t.Fatal(err)
	}

	if runs, tasks, awaiting, released := svc.RetentionStats(); runs != 1 || tasks != 1 || awaiting != 1 || released != 0 {
		t.Fatalf("unacknowledged state changed: %d %d %d %d", runs, tasks, awaiting, released)
	}

	changed := first.Executions[0]
	changed.Record.ExecutionID = "new-attempt"
	if _, err := svc.LoadRun(ctx, orchestrator.RunSpec{RunID: first.RunID, Executions: []orchestrator.TaskExecutionSnapshot{changed}}); !errors.Is(err, dal.ErrConflict) {
		t.Fatalf("replaced an unpersisted terminal generation: %v", err)
	}

	if err := svc.AcknowledgeRunPersistence(ctx, first.RunID, first.PersistenceToken); err != nil {
		t.Fatal(err)
	}

	if _, err := svc.GetRunTaskCompletion(ctx, first.RunID); !errors.Is(err, dal.ErrNotFound) {
		t.Fatalf("graph still present: %v", err)
	}

	next, err := svc.LoadRun(ctx, orchestrator.RunSpec{RunID: first.RunID})
	if err != nil {
		t.Fatal(err)
	}

	if err := svc.AcknowledgeRunPersistence(ctx, first.RunID, first.PersistenceToken); err != nil {
		t.Fatal(err)
	}

	claim, err := svc.ClaimExecution(ctx, next.RunID, next.Root.ExecutionID, "worker", time.Now().Add(time.Hour))
	if err != nil || !claim.Claimed {
		t.Fatalf("old receipt removed active generation: %+v %v", claim, err)
	}

	second, err := svc.CompleteExecutionByClaim(ctx, next.RunID, next.Root.ExecutionID, "worker", claim.ClaimToken, dal.ExecutionStatusSucceeded, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if second.PersistenceToken == first.PersistenceToken {
		t.Fatal("persistence token reused")
	}

	if err := svc.AcknowledgeRunPersistence(ctx, first.RunID, first.PersistenceToken); err != nil {
		t.Fatal(err)
	}

	if _, err := svc.GetRunTaskCompletion(ctx, second.RunID); err != nil {
		t.Fatalf("old receipt removed new terminal generation: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := svc.AcknowledgeRunPersistence(ctx, second.RunID, second.PersistenceToken); err != nil {
				t.Error(err)
			}
		}()
	}

	wg.Wait()
	if runs, tasks, awaiting, released := svc.RetentionStats(); runs != 0 || tasks != 0 || awaiting != 0 || released != 2 {
		t.Fatalf("retention counts: %d %d %d %d", runs, tasks, awaiting, released)
	}
}

func TestServiceCompletedHistoryDoesNotAccumulate(t *testing.T) {
	svc := orchestrator.New(2)
	t.Cleanup(svc.Close)
	for i := 0; i < 1000; i++ {
		result, _ := completeRetainedRun(t, svc, fmt.Sprintf("history-%d", i))
		if err := svc.AcknowledgeRunPersistence(context.Background(), result.RunID, result.PersistenceToken); err != nil {
			t.Fatal(err)
		}
	}
	if runs, tasks, awaiting, released := svc.RetentionStats(); runs != 0 || tasks != 0 || awaiting != 0 || released != 1000 {
		t.Fatalf("history retained: %d %d %d %d", runs, tasks, awaiting, released)
	}
}

func TestGRPCAcknowledgeRunPersistence(t *testing.T) {
	svc := orchestrator.New(1)
	t.Cleanup(svc.Close)
	client, cleanup := newOrchestratorTestClient(t, svc)
	t.Cleanup(cleanup)
	result, _ := completeRetainedRun(t, svc, "grpc-retirement")
	ctx := t.Context()
	if _, err := client.AcknowledgeRunPersistence(ctx, &api.AcknowledgeRunPersistenceRequest{RunId: testString(result.RunID)}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty token accepted: %v", err)
	}
	for range 2 {
		if _, err := client.AcknowledgeRunPersistence(ctx, &api.AcknowledgeRunPersistenceRequest{RunId: testString(result.RunID), PersistenceToken: testString(result.PersistenceToken)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := svc.GetRunTaskCompletion(ctx, result.RunID); !errors.Is(err, dal.ErrNotFound) {
		t.Fatalf("acknowledged graph retained: %v", err)
	}
}

func TestFailedAndCancelledGraphsRequirePersistenceAcknowledgment(t *testing.T) {
	for _, completionStatus := range []string{dal.ExecutionStatusFailed, dal.ExecutionStatusCancelled} {
		t.Run(completionStatus, func(t *testing.T) {
			svc := orchestrator.New(1)
			t.Cleanup(svc.Close)
			ctx := t.Context()
			loaded, err := svc.LoadRun(ctx, orchestrator.RunSpec{RunID: "terminal", Tasks: []orchestrator.TaskSpec{{TaskKey: "child", ParentTaskKey: dal.RootTaskKey}}})
			if err != nil {
				t.Fatal(err)
			}
			claim, err := svc.ClaimExecution(ctx, loaded.RunID, loaded.Root.ExecutionID, "worker", time.Now().Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			result, err := svc.CompleteExecutionByClaim(ctx, loaded.RunID, loaded.Root.ExecutionID, "worker", claim.ClaimToken, completionStatus, "", "")
			if err != nil {
				t.Fatal(err)
			}
			if result.PersistenceToken == "" || len(result.Executions) != 2 {
				t.Fatalf("incomplete terminal snapshot: %+v", result)
			}
			if err := svc.AcknowledgeRunPersistence(ctx, loaded.RunID, result.PersistenceToken); err != nil {
				t.Fatal(err)
			}
			if runs, tasks, pending, reclaimed := svc.RetentionStats(); runs != 0 || tasks != 0 || pending != 0 || reclaimed != 1 {
				t.Fatalf("terminal graph retained: %d %d %d %d", runs, tasks, pending, reclaimed)
			}
		})
	}
}
