package orchestrator_test

import (
	"errors"
	"testing"
	"time"

	"vectis/internal/dal"
	"vectis/internal/orchestrator"
)

func TestServiceSnapshotPreservesActiveClaimIdentity(t *testing.T) {
	for _, replace := range []bool{false, true} {
		name := "control"
		if replace {
			name = "changed_attempt"
		}

		t.Run(name, func(t *testing.T) {
			svc := orchestrator.New(1)
			t.Cleanup(svc.Close)

			ctx := t.Context()
			loaded, err := svc.LoadRun(ctx, orchestrator.RunSpec{RunID: "audit-active"})
			if err != nil {
				t.Fatal(err)
			}

			until := time.Now().Add(time.Hour)
			claim, err := svc.ClaimExecution(ctx, loaded.RunID, loaded.Root.ExecutionID, "worker", until)
			if err != nil || !claim.Claimed {
				t.Fatalf("claim: %+v %v", claim, err)
			}

			replacement := loaded.Root
			if replace {
				replacement.ExecutionID = "replacement-execution"
				replacement.Attempt = 2
				replacement.TaskAttemptID = replacement.TaskID + ":attempt:2"
			}

			_, loadErr := svc.LoadRun(ctx, orchestrator.RunSpec{RunID: loaded.RunID, Executions: []orchestrator.TaskExecutionSnapshot{{Record: replacement, Status: dal.ExecutionStatusPending}}})
			if replace && !errors.Is(loadErr, dal.ErrConflict) {
				t.Fatalf("active identity replacement was not rejected: %v", loadErr)
			}

			if !replace && loadErr != nil {
				t.Fatal(loadErr)
			}

			renewErr := svc.RenewExecutionLease(ctx, loaded.RunID, loaded.Root.ExecutionID, "worker", claim.ClaimToken, until)
			t.Logf("replacement load error=%v original claim renewal error=%v", loadErr, renewErr)
			if renewErr != nil {
				t.Fatal("LoadRun replaced the identity of an actively claimed execution")
			}
		})
	}
}

func TestServiceSnapshotRejectionIsAtomic(t *testing.T) {
	svc := orchestrator.New(1)
	t.Cleanup(svc.Close)

	ctx := t.Context()
	loaded, err := svc.LoadRun(ctx, orchestrator.RunSpec{RunID: "audit-batch", Tasks: []orchestrator.TaskSpec{{TaskKey: "child", ParentTaskKey: dal.RootTaskKey}}})
	if err != nil {
		t.Fatal(err)
	}

	_, err = svc.LoadRun(ctx, orchestrator.RunSpec{RunID: loaded.RunID, Executions: []orchestrator.TaskExecutionSnapshot{
		{Record: loaded.Root, Status: dal.ExecutionStatusSucceeded},
		{Record: dal.TaskExecutionRecord{TaskKey: "child", RunID: "wrong-run"}, Status: dal.ExecutionStatusPending},
	}})

	if !errors.Is(err, dal.ErrConflict) {
		t.Fatalf("batch rejection=%v", err)
	}

	after, err := svc.GetRunTaskSnapshot(ctx, loaded.RunID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}

	for _, execution := range after.Executions {
		if execution.Record.TaskKey == dal.RootTaskKey {
			t.Logf("rejected batch left root status=%s, summary succeeded=%d", execution.Status, after.Summary.Succeeded)
			if execution.Status != dal.ExecutionStatusPending {
				t.Fatal("rejected snapshot batch mutated the live graph")
			}
		}
	}
}
