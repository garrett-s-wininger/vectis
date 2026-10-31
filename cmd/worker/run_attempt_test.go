package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/observability"
	"vectis/internal/orchestrator"
	"vectis/internal/testutil/dbtest"
)

func TestWorkerReportsCannotMutateRetry(t *testing.T) {
	for _, report := range []string{"claim_completion_control", "orphan", "failure", "failure_after_retry_success"} {
		t.Run(report, func(t *testing.T) {
			w, _, _, env, _, db := retirementWorker(t)
			ctx := t.Context()
			w.runCtx = ctx
			w.clock = mocks.NewMockClock()
			old, err := w.store.TryClaimExecution(ctx, env.ExecutionID, "old-worker", time.Now().Add(time.Hour))
			if err != nil || !old.Claimed {
				t.Fatalf("old claim: %+v %v", old, err)
			}

			if err := w.store.MarkRunOrphaned(ctx, env.RunID, dal.OrphanReasonLeaseExpired); err != nil {
				t.Fatal(err)
			}

			if err := w.store.RequeueRunForRetry(ctx, env.RunID); err != nil {
				t.Fatal(err)
			}

			next, err := w.store.GetPendingExecution(ctx, env.RunID)
			if err != nil {
				t.Fatal(err)
			}

			current, err := w.store.TryClaimExecution(ctx, next.ExecutionID, "new-worker", time.Now().Add(time.Hour))
			if err != nil || !current.Claimed {
				t.Fatalf("current claim: %+v %v", current, err)
			}

			if err := w.store.UpsertRunHotStateOwner(ctx, dal.RunHotStateOwnerUpdate{RunID: env.RunID, CellID: dal.DefaultCellID, OwnerID: "new-orchestrator", OwnerEpoch: "new-worker", LeaseUntil: time.Now().Add(time.Hour)}); err != nil {
				t.Fatal(err)
			}

			wantState, wantOwner := dal.RunStatusRunning, true
			if report == "failure_after_retry_success" {
				if _, err := w.store.CompleteExecutionAndFinalizeRunByClaim(ctx, next.ExecutionID, "new-worker", current.ClaimToken, dal.ExecutionStatusSucceeded, "", ""); err != nil {
					t.Fatal(err)
				}

				wantState, wantOwner = dal.RunStatusSucceeded, false
			}

			switch report {
			case "claim_completion_control":
				_, err = w.store.CompleteExecutionAndFinalizeRunByClaim(ctx, env.ExecutionID, "old-worker", old.ClaimToken, dal.ExecutionStatusFailed, dal.FailureCodeExecution, "late failure")
				if !errors.Is(err, dal.ErrConflict) {
					t.Fatalf("stale claim completion error: %v", err)
				}
			case "orphan":
				err = w.markRunOrphanedWithRetry(env, dal.OrphanReasonAckUncertain)
			default:
				err = w.markRunFailedWithRetry(env, dal.FailureCodeInvalidEnvelope, "old delivery rejected")
			}

			if !errors.Is(err, dal.ErrConflict) {
				t.Fatalf("stale report must conflict: %v", err)
			}

			state, _, err := w.store.GetRunStatus(ctx, env.RunID)
			if err != nil {
				t.Fatal(err)
			}

			_, ownerFound, err := w.store.GetRunHotStateOwner(ctx, env.RunID)
			if err != nil {
				t.Fatal(err)
			}

			var token string
			if err := db.QueryRowContext(ctx, `SELECT COALESCE(claim_token, '') FROM segment_executions WHERE execution_id = ?`, next.ExecutionID).Scan(&token); err != nil {
				t.Fatal(err)
			}

			t.Logf("old report=%s new attempt=%d status=%s owner retained=%v current token retained=%v", report, next.Attempt, state, ownerFound, token == current.ClaimToken)
			if state != wantState || ownerFound != wantOwner {
				t.Fatal("old worker report changed newer attempt state")
			}
		})
	}
}

func continuationFixture(t *testing.T) (*worker, *api.Job, *cell.ExecutionEnvelope, dal.ExecutionFinalizationResult, *sql.DB, *executionClaimState) {
	t.Helper()
	ctx := t.Context()
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	definition := `{"id":"attempt-audit","root":{"id":"root","uses":"builtins/parallel","steps":[{"id":"child","uses":"builtins/result","with":{"success":"true"}}]}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, "attempt-audit", definition); err != nil {
		t.Fatal(err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, "attempt-audit", nil, 1)
	if err != nil {
		t.Fatal(err)
	}

	var j api.Job
	if err := json.Unmarshal([]byte(definition), &j); err != nil {
		t.Fatal(err)
	}

	j.RunId = stringPtr(runID)
	env, err := cell.AttachPendingExecutionEnvelope(ctx, repos.Runs(), &api.JobRequest{Job: &j}, runID, time.Now().UnixNano())
	if err != nil {
		t.Fatal(err)
	}

	svc := orchestrator.New(1)
	t.Cleanup(svc.Close)
	ch := &localOrchestratorChoreographer{service: svc}
	if err := ch.LoadRun(ctx, &j, env, nil); err != nil {
		t.Fatal(err)
	}

	claim, err := ch.ClaimAndStartExecution(ctx, env, "old-worker", time.Now().Add(time.Hour))
	if err != nil || !claim.Claimed {
		t.Fatalf("claim: %+v %v", claim, err)
	}

	result, err := ch.CompleteExecution(ctx, env, "old-worker", claim.ClaimToken, dal.ExecutionStatusSucceeded, "", "")
	if err != nil || len(result.Children) != 1 {
		t.Fatalf("fanout completion: %+v %v", result, err)
	}

	w := &worker{workerID: "old-worker", retirements: repos.OrchestratorRetirements(), runCtx: ctx, store: repos.Runs(), choreographer: ch, clock: mocks.NewMockClock(), logger: interfaces.NewLogger("attempt-audit")}
	if err := w.store.ApplyExecutionStatusUpdate(ctx, dal.ExecutionStatusUpdate{ExecutionID: env.ExecutionID, Status: dal.ExecutionStatusSucceeded}); err != nil {
		t.Fatal(err)
	}

	return w, &j, env, result, db, newExecutionClaimState(claim.ClaimToken)
}

func TestContinuationPreservesAttemptAndCancellation(t *testing.T) {
	for _, scenario := range []string{"no_cancel_control", "current_cancel", "stale_parent_after_retry"} {
		t.Run(scenario, func(t *testing.T) {
			w, j, env, result, db, _ := continuationFixture(t)
			ctx := t.Context()
			if scenario == "stale_parent_after_retry" {
				if err := w.store.MarkRunOrphaned(ctx, env.RunID, dal.OrphanReasonLeaseExpired); err != nil {
					t.Fatal(err)
				}

				if err := w.store.RequeueRunForRetry(ctx, env.RunID); err != nil {
					t.Fatal(err)
				}

				next, err := w.store.GetPendingExecution(ctx, env.RunID)
				if err != nil {
					t.Fatal(err)
				}

				claim, err := w.store.TryClaimExecution(ctx, next.ExecutionID, "new-worker", time.Now().Add(time.Hour))
				if err != nil || !claim.Claimed {
					t.Fatalf("retry claim: %+v %v", claim, err)
				}
			} else if err := w.store.MarkRunRunning(ctx, env.RunID); err != nil {
				t.Fatal(err)
			}

			if scenario != "no_cancel_control" {
				if _, err := w.store.RequestRunCancel(ctx, env.RunID, "cancel current attempt"); err != nil {
					t.Fatal(err)
				}
			}

			persisted, persistErr := w.persistTaskContinuation(ctx, j, env, result)
			var state string
			var cancelAt int64
			if err := db.QueryRowContext(ctx, `SELECT status, COALESCE(cancel_requested_at, 0) FROM job_runs WHERE run_id = ?`, env.RunID).Scan(&state, &cancelAt); err != nil {
				t.Fatal(err)
			}

			var children int
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_tasks WHERE run_id = ? AND task_key <> 'root'`, env.RunID).Scan(&children); err != nil {
				t.Fatal(err)
			}

			t.Logf("scenario=%s persisted=%v error=%v run status=%s cancel intent retained=%v child rows=%d", scenario, persisted, persistErr, state, cancelAt != 0, children)
			if scenario == "no_cancel_control" {
				if !persisted || persistErr != nil || state != dal.RunStatusQueued || children != 1 {
					t.Fatal("control continuation failed")
				}
			} else if persisted || persistErr == nil || state == dal.RunStatusQueued || (cancelAt == 0 && state != dal.RunStatusCancelled) || children != 0 {
				t.Fatal("continuation overwrote current attempt or lost cancellation")
			}
		})
	}
}

func TestCancelledContinuationPersistsAndReleasesGraph(t *testing.T) {
	for _, transport := range []string{"unary", "stream"} {
		t.Run(transport, func(t *testing.T) {
			w, j, env, _, db, claim := continuationFixture(t)
			ctx := t.Context()
			local := w.choreographer.(*localOrchestratorChoreographer)
			faults := &recoveryRPCFaults{}
			remote, closeServer := recoveryFaultClient(t, local.service, faults, transport == "unary")
			defer closeServer()

			w.choreographer = remote
			queue := mocks.NewMockQueueClient()
			w.queue = queue
			if err := w.store.MarkRunRunning(ctx, env.RunID); err != nil {
				t.Fatal(err)
			}

			if _, err := w.store.RequestRunCancel(ctx, env.RunID, "cancel before fanout"); err != nil {
				t.Fatal(err)
			}

			lost := &loseCancellationResponse{executionChoreographer: remote, canceller: remote}
			w.choreographer = lost
			outcome := w.finalizeSucceededTaskRunByExecutionClaim(ctx, j, j.GetId(), env.RunID, claim, env)
			if outcome != observability.WorkerOutcomeAborted {
				t.Fatalf("outcome = %s", outcome)
			}

			if len(queue.GetJobs()) != 0 {
				t.Fatal("cancelled children were enqueued")
			}

			state, _, err := w.store.GetRunStatus(ctx, env.RunID)
			if err != nil || state != dal.RunStatusCancelled {
				t.Fatalf("status=%s err=%v", state, err)
			}

			var facts int
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_task_final_facts WHERE run_id = ?`, env.RunID).Scan(&facts); err != nil || facts != 2 {
				t.Fatalf("facts=%d err=%v", facts, err)
			}

			if runs, _, awaiting, _ := local.service.RetentionStats(); runs != 1 || awaiting != 1 {
				t.Fatalf("unacknowledged graph missing: runs=%d awaiting=%d", runs, awaiting)
			}

			w.choreographer = remote
			if err := w.drainRetirements(ctx); err != nil {
				t.Fatal(err)
			}

			if runs, _, _, _ := local.service.RetentionStats(); runs != 0 {
				t.Fatal("cancelled graph leaked")
			}

			if lost.calls != 2 {
				t.Fatalf("cancellation calls=%d", lost.calls)
			}
		})
	}
}

type loseCancellationResponse struct {
	executionChoreographer
	canceller continuationCanceller
	calls     int
}

func (c *loseCancellationResponse) CancelRunContinuation(ctx context.Context, env *cell.ExecutionEnvelope, owner, token string) (dal.ExecutionFinalizationResult, error) {
	c.calls++
	result, err := c.canceller.CancelRunContinuation(ctx, env, owner, token)

	if err == nil && c.calls == 1 {
		return dal.ExecutionFinalizationResult{}, errors.New("lost cancellation response")
	}

	return result, err
}

func TestContinuationTransactionRollsBackAllWrites(t *testing.T) {
	w, j, env, result, db, _ := continuationFixture(t)
	ctx := t.Context()
	if _, err := db.ExecContext(ctx, `UPDATE segment_executions SET status = 'running' WHERE execution_id = ?`, env.ExecutionID); err != nil {
		t.Fatal(err)
	}

	if err := w.store.MarkRunRunning(ctx, env.RunID); err != nil {
		t.Fatal(err)
	}

	if _, err := db.ExecContext(ctx, `CREATE TRIGGER fail_continuation BEFORE UPDATE OF status ON job_runs WHEN NEW.status = 'queued' BEGIN SELECT RAISE(ABORT, 'injected continuation failure'); END`); err != nil {
		t.Fatal(err)
	}

	queue := mocks.NewMockQueueClient()
	w.queue = queue
	if persisted, err := w.dispatchOrchestratorContinuation(ctx, j, env, result, true); err == nil || persisted || !strings.Contains(err.Error(), "injected continuation failure") {
		t.Fatalf("failed continuation accepted: %v %v", persisted, err)
	}

	var sourceStatus string
	var children int
	if err := db.QueryRowContext(ctx, `SELECT status FROM segment_executions WHERE execution_id = ?`, env.ExecutionID).Scan(&sourceStatus); err != nil {
		t.Fatal(err)
	}

	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_tasks WHERE run_id = ? AND task_key <> 'root'`, env.RunID).Scan(&children); err != nil {
		t.Fatal(err)
	}

	if sourceStatus != dal.ExecutionStatusRunning || children != 0 || len(queue.GetJobs()) != 0 {
		t.Fatalf("partial commit: source=%s children=%d queued=%d", sourceStatus, children, len(queue.GetJobs()))
	}

	if _, err := db.ExecContext(ctx, `DROP TRIGGER fail_continuation`); err != nil {
		t.Fatal(err)
	}

	if persisted, err := w.dispatchOrchestratorContinuation(ctx, j, env, result, true); err != nil || !persisted {
		t.Fatalf("recovery failed: %v %v", persisted, err)
	}
}

func TestContinuationDeliveryPreservesRootGeneration(t *testing.T) {
	w, j, source, result, _, _ := continuationFixture(t)
	ctx := t.Context()
	if _, err := w.persistTaskContinuation(ctx, j, source, result); err != nil {
		t.Fatal(err)
	}

	child := result.Children[0]
	dispatch, err := w.prepareContinuationDispatch(ctx, j, source, child)
	if err != nil {
		t.Fatal(err)
	}

	req := &api.JobRequest{Job: j}
	if _, err := cell.AttachExecutionEnvelope(req, dispatch, time.Now().UnixNano()); err != nil {
		t.Fatal(err)
	}

	if err := refreshHydratedExecutionEnvelope(req); err != nil {
		t.Fatal(err)
	}

	env, _, err := cell.ExecutionEnvelopeFromRequest(req)
	if err != nil || env == nil {
		t.Fatalf("hydrated envelope: %v", err)
	}

	if env.RunAttemptExecutionID() != source.ExecutionID {
		t.Fatal("hydration lost root generation")
	}

	stored, err := w.store.GetExecutionDispatch(ctx, child.ExecutionID)
	if err != nil || stored.RootExecutionID != source.ExecutionID {
		t.Fatalf("durable dispatch generation=%s err=%v", stored.RootExecutionID, err)
	}
}

func TestContinuationCancellationFencesAndReplays(t *testing.T) {
	w, j, env, _, _, claim := continuationFixture(t)
	ctx := t.Context()
	ch := w.choreographer.(*localOrchestratorChoreographer)
	for _, identity := range []struct{ root, owner, token string }{
		{"another-generation", w.workerID, claim.get()},
		{env.ExecutionID, "another-worker", claim.get()},
		{env.ExecutionID, w.workerID, "another-claim"},
	} {
		if _, err := ch.service.CancelRunContinuation(ctx, env.RunID, identity.root, env.ExecutionID, identity.owner, identity.token); !errors.Is(err, dal.ErrConflict) {
			t.Fatalf("unauthorized cancellation: %v", err)
		}
	}

	result, err := ch.CancelRunContinuation(ctx, env, w.workerID, claim.get())
	if err != nil {
		t.Fatal(err)
	}

	replay, err := ch.CompleteExecution(ctx, env, w.workerID, claim.get(), dal.ExecutionStatusSucceeded, "", "")
	if err != nil || replay.PersistenceToken != result.PersistenceToken || replay.Outcome != dal.ExecutionFinalizationOutcomeRunCancelled || len(replay.Children) != 0 {
		t.Fatalf("stale continuation replayed: %+v %v", replay, err)
	}

	if outcome := w.finalizeSucceededTaskRunByExecutionClaim(ctx, j, j.GetId(), env.RunID, claim, env); outcome != observability.WorkerOutcomeAborted {
		t.Fatalf("cancelled completion replay outcome: %s", outcome)
	}

	if runs, _, _, _ := ch.service.RetentionStats(); runs != 0 {
		t.Fatal("replayed cancelled graph leaked")
	}
}

func TestCurrentWorkerReportsAndOperatorForceFail(t *testing.T) {
	for _, state := range []string{dal.RunStatusOrphaned, dal.RunStatusFailed} {
		t.Run(state, func(t *testing.T) {
			w, _, _, env, _, _ := retirementWorker(t)
			if err := w.store.ReportRunAttempt(t.Context(), env.RunID, env.ExecutionID, state, dal.FailureCodeExecution, "current worker report"); err != nil {
				t.Fatal(err)
			}

			actual, _, err := w.store.GetRunStatus(t.Context(), env.RunID)
			if err != nil || actual != state {
				t.Fatalf("status=%s err=%v", actual, err)
			}

			if err := w.store.MarkRunFailed(t.Context(), env.RunID, dal.FailureCodeExecution, "operator force fail"); err != nil {
				t.Fatal(err)
			}
		})
	}
}
