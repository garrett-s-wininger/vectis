package main

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/orchestrator"
	"vectis/internal/testutil/dbtest"
)

type retirementChoreographer struct {
	*localOrchestratorChoreographer
	ackErr   error
	ackCalls int
}

func (c *retirementChoreographer) AcknowledgeRunPersistence(ctx context.Context, runID, token string) error {
	c.ackCalls++
	if c.ackErr != nil {
		return c.ackErr
	}

	return c.service.AcknowledgeRunPersistence(ctx, runID, token)
}

func retirementWorker(t *testing.T) (*worker, *retirementChoreographer, *api.Job, *cell.ExecutionEnvelope, *executionClaimState, *sql.DB) {
	t.Helper()
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := t.Context()
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, "retirement-job", `{"id":"retirement-job","root":{"uses":"builtins/result"}}`); err != nil {
		t.Fatal(err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, "retirement-job", nil, 1)
	if err != nil {
		t.Fatal(err)
	}

	dispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}

	record := dal.TaskExecutionRecord{RunID: runID, TaskID: dispatch.TaskID, TaskKey: dispatch.TaskKey, Name: dispatch.TaskName, TaskAttemptID: dispatch.TaskAttemptID, SegmentID: dispatch.SegmentID, SegmentName: dispatch.SegmentName, ExecutionID: dispatch.ExecutionID, CellID: dispatch.CellID, Attempt: dispatch.Attempt}
	env := executionEnvelopeFromRecord(record)
	j := &api.Job{Id: stringPtr("retirement-job"), RunId: stringPtr(runID), Root: &api.Node{Id: stringPtr(dal.RootTaskKey), Uses: stringPtr("builtins/result")}}
	svc := orchestrator.New(1)
	t.Cleanup(svc.Close)
	c := &retirementChoreographer{localOrchestratorChoreographer: &localOrchestratorChoreographer{service: svc}}

	if err := c.LoadRun(ctx, j, env, nil); err != nil {
		t.Fatal(err)
	}

	claim, err := c.ClaimAndStartExecution(ctx, env, "worker", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}

	w := &worker{workerID: "worker", store: repos.Runs(), retirements: repos.OrchestratorRetirements(), choreographer: c, logger: interfaces.NewLogger("retirement-test"), clock: interfaces.SystemClock{}}
	return w, c, j, env, newExecutionClaimState(claim.ClaimToken), db
}

func TestTerminalPersistenceFailureDoesNotReleaseState(t *testing.T) {
	w, c, j, env, claim, db := retirementWorker(t)
	ctx := t.Context()
	if _, err := db.Exec(`CREATE TRIGGER fail_receipt BEFORE INSERT ON orchestrator_retirements BEGIN SELECT RAISE(ABORT, 'receipt failure'); END`); err != nil {
		t.Fatal(err)
	}

	_, ok := w.completeExecutionAndFinalizeRunByClaim(ctx, j, env, claim, dal.ExecutionStatusSucceeded, "", "")
	if ok || c.ackCalls != 0 {
		t.Fatalf("failed transaction acknowledged: ok=%v calls=%d", ok, c.ackCalls)
	}

	status, _, err := w.store.GetRunStatus(ctx, env.RunID)
	if err != nil || status != dal.RunStatusQueued {
		t.Fatalf("snapshot partially committed: %s %v", status, err)
	}

	if runs, _, pending, _ := c.service.RetentionStats(); runs != 1 || pending != 1 {
		t.Fatal("unpersisted graph was discarded")
	}

	if _, err := db.Exec(`DROP TRIGGER fail_receipt`); err != nil {
		t.Fatal(err)
	}

	_, ok = w.completeExecutionAndFinalizeRunByClaim(ctx, j, env, claim, dal.ExecutionStatusSucceeded, "", "")
	if !ok || c.ackCalls != 1 {
		t.Fatalf("recovered persistence failed: ok=%v calls=%d", ok, c.ackCalls)
	}

	if runs, _, _, _ := c.service.RetentionStats(); runs != 0 {
		t.Fatal("persisted graph was not reclaimed")
	}
}

func TestAnotherWorkerRecoversLostPersistenceAcknowledgment(t *testing.T) {
	w, c, j, env, claim, _ := retirementWorker(t)
	ctx := t.Context()
	c.ackErr = errors.New("orchestrator unavailable")
	_, ok := w.completeExecutionAndFinalizeRunByClaim(ctx, j, env, claim, dal.ExecutionStatusSucceeded, "", "")
	if !ok {
		t.Fatal("acknowledgment failure changed persisted run outcome")
	}

	receipts, err := w.retirements.ListDue(ctx, time.Now().Unix(), 256)
	if err != nil || len(receipts) != 1 {
		t.Fatalf("lost receipt: %+v %v", receipts, err)
	}

	if runs, _, _, _ := c.service.RetentionStats(); runs != 1 {
		t.Fatal("state discarded before acknowledgment")
	}

	replacement := &worker{store: w.store, retirements: w.retirements, choreographer: c, logger: w.logger}
	if err := replacement.drainRetirements(ctx); err != nil {
		t.Fatal(err)
	}

	if due, err := w.retirements.ListDue(ctx, time.Now().Unix(), 256); err != nil || len(due) != 0 {
		t.Fatalf("failed receipt was not deferred: %+v %v", due, err)
	}

	if runs, _, _, _ := c.service.RetentionStats(); runs != 1 {
		t.Fatal("failed retry released graph")
	}

	c.ackErr = nil
	if err := w.retirements.RetryAfter(ctx, receipts[0].PersistenceToken, 0); err != nil {
		t.Fatal(err)
	}

	if err := replacement.drainRetirements(ctx); err != nil {
		t.Fatal(err)
	}

	if runs, _, _, _ := c.service.RetentionStats(); runs != 0 {
		t.Fatal("replacement worker did not reclaim graph")
	}

	receipts, err = w.retirements.ListDue(ctx, time.Now().Unix(), 256)
	if err != nil || len(receipts) != 0 {
		t.Fatalf("delivered receipt retained: %+v %v", receipts, err)
	}
}

func TestReclaimedRunCannotReloadFromDelayedDelivery(t *testing.T) {
	w, c, j, env, claim, _ := retirementWorker(t)
	ctx := t.Context()
	if _, ok := w.completeExecutionAndFinalizeRunByClaim(ctx, j, env, claim, dal.ExecutionStatusSucceeded, "", ""); !ok {
		t.Fatal("completion failed")
	}

	if err := w.prepareRunForExecution(ctx, j, env, time.Now().Add(time.Hour)); !errors.Is(err, dal.ErrConflict) {
		t.Fatalf("delayed root load accepted: %v", err)
	}

	if recovered, err := w.recoverOrchestratorExecutionClaim(ctx, j, env, claim, time.Now().Add(time.Hour), "claim"); recovered || !errors.Is(err, dal.ErrConflict) {
		t.Fatalf("completed run recovered: %v %v", recovered, err)
	}

	if runs, _, _, _ := c.service.RetentionStats(); runs != 0 {
		t.Fatal("delayed delivery recreated terminal graph")
	}

	if err := c.LoadRun(ctx, j, env, nil); err != nil {
		t.Fatal(err)
	}

	if _, claimed, _, err := w.tryClaimExecution(ctx, j, env, time.Now().Add(time.Hour)); claimed || err != nil {
		t.Fatalf("root crossed terminal persistence fence: claimed=%v err=%v", claimed, err)
	}
}
