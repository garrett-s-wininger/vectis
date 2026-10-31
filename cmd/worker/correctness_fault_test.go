package main

import (
	"context"
	"errors"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/job"
	"vectis/internal/observability"
	"vectis/internal/orchestrator"
)

func TestWorkerFault_LostFanoutCompletionResponse(t *testing.T) {
	for _, loseResponse := range []bool{false, true} {
		for _, unary := range []bool{false, true} {
			name := "stream"
			if unary {
				name = "unary"
			}

			if loseResponse {
				name += "_response_lost"
			} else {
				name += "_control"
			}

			t.Run(name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()

				seed, _, j, env, _, _ := retirementWorker(t)
				svc := orchestrator.New(1)
				t.Cleanup(svc.Close)

				faults := &recoveryRPCFaults{}
				faults.loseCompletion.Store(loseResponse)
				ch, _ := recoveryFaultClient(t, svc, faults, unary)
				clock := mocks.NewMockClock()
				clock.SetNow(time.Now())
				queue := mocks.NewMockQueueClient()
				core := &recoveryCountingCore{Core: testWorkerCore(job.NewExecutor())}

				w := &worker{ctx: ctx, runCtx: ctx, logger: seed.logger, workerID: "fanout-worker", cellID: dal.DefaultCellID, store: seed.store, retirements: seed.retirements, choreographer: ch, queue: queue, logClient: mocks.NewMockLogClient(), core: core, clock: clock, renewInterval: time.Hour}
				j.Root.Uses = stringPtr("builtins/parallel")
				j.Root.With = nil
				j.Root.Steps = []*api.Node{{Id: stringPtr("child"), Uses: stringPtr("builtins/result"), With: map[string]string{"success": "true"}}}

				got := w.runTaskExecution(ctx, j, j.GetId(), env.RunID, "fanout-delivery", env)
				pending, err := svc.ListPending(ctx, env.RunID, 100)
				if err != nil {
					t.Fatal(err)
				}

				t.Logf("worker outcome=%s, activated pending children=%d, enqueued children=%d, core executions=%d", got, len(pending), len(queue.GetJobs()), core.calls.Load())
				if got != observability.WorkerOutcomeSuccess || len(queue.GetJobs()) != 1 || len(pending) != 1 || core.calls.Load() != 1 {
					t.Fatal("lost continuation response stranded activated child")
				}

				dispatched, ok, err := cell.ExecutionEnvelopeFromRequest(queue.GetJobRequests()[0])
				if err != nil || !ok || dispatched.ExecutionID != pending[0].ExecutionID {
					t.Fatalf("queued child does not match activation: %+v, %v, %v", dispatched, ok, err)
				}

				state, found, err := w.store.GetRunStatus(ctx, env.RunID)
				if err != nil || !found || state != dal.RunStatusQueued {
					t.Fatalf("continuation was not durably queued: %s, %v, %v", state, found, err)
				}
			})
		}
	}
}

func TestWorkerFault_OldSnapshotCannotOverwriteRetry(t *testing.T) {
	w, c, _, env, claim, db := retirementWorker(t)
	ctx := t.Context()
	result, err := c.CompleteExecution(ctx, env, "worker", claim.get(), dal.ExecutionStatusFailed, "execution_error", "original failure")
	if err != nil {
		t.Fatal(err)
	}

	if err := w.persistTerminalExecutionSnapshotOnce(ctx, result, "execution_error", "original failure"); err != nil {
		t.Fatal(err)
	}

	if err := w.store.RequeueRunForRetry(ctx, env.RunID); err != nil {
		t.Fatal(err)
	}

	retry, err := w.store.GetPendingExecution(ctx, env.RunID)
	if err != nil {
		t.Fatal(err)
	}

	active, err := w.store.TryClaimExecution(ctx, retry.ExecutionID, "replacement", time.Now().Add(time.Hour))
	if err != nil || !active.Claimed {
		t.Fatalf("retry claim: %+v %v", active, err)
	}

	before, _, err := w.store.GetRunStatus(ctx, env.RunID)
	if err != nil {
		t.Fatal(err)
	}

	replayErr := w.persistTerminalExecutionSnapshotOnce(ctx, result, "execution_error", "original failure")
	if !errors.Is(replayErr, dal.ErrConflict) {
		t.Fatalf("stale snapshot error = %v, want conflict", replayErr)
	}

	after, _, err := w.store.GetRunStatus(ctx, env.RunID)
	if err != nil {
		t.Fatal(err)
	}

	var token string
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(claim_token,'') FROM segment_executions WHERE execution_id = ?`, retry.ExecutionID).Scan(&token); err != nil {
		t.Fatal(err)
	}

	t.Logf("old snapshot replay error=%v, retry run status before=%s after=%s, active retry token preserved=%v", replayErr, before, after, token == active.ClaimToken)
	if after != before || token != active.ClaimToken {
		t.Fatal("stale terminal snapshot overwrote newer retry")
	}
}

func TestWorkerFault_TerminalRunCannotExecuteDelayedDelivery(t *testing.T) {
	for _, terminalStatus := range []string{dal.RunStatusSucceeded, dal.RunStatusFailed, dal.RunStatusCancelled, dal.RunStatusAborted, dal.RunStatusAbandoned} {
		t.Run(terminalStatus, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			w, _, j, env, _, db := retirementWorker(t)
			if terminalStatus == dal.RunStatusAbandoned {
				if err := w.store.MarkRunOrphaned(ctx, env.RunID, "lease_expired"); err != nil {
					t.Fatal(err)
				}

				if err := w.store.RepairMarkRunAbandoned(ctx, env.RunID, "operator abandoned"); err != nil {
					t.Fatal(err)
				}
			} else if _, err := db.ExecContext(ctx, `UPDATE job_runs SET status = ? WHERE run_id = ?`, terminalStatus, env.RunID); err != nil {
				t.Fatal(err)
			}

			w.ctx, w.runCtx = ctx, ctx
			w.clock = mocks.NewMockClock()
			w.renewInterval = time.Hour
			w.queue = mocks.NewMockQueueClient()
			w.logClient = mocks.NewMockLogClient()
			core := &recoveryCountingCore{Core: testWorkerCore(job.NewExecutor())}
			w.core = core
			svc := orchestrator.New(1)
			t.Cleanup(svc.Close)

			w.choreographer = &localOrchestratorChoreographer{service: svc}
			j.Root.With = map[string]string{"success": "true"}
			outcome := w.runTaskExecution(ctx, j, j.GetId(), env.RunID, "late-delivery", env)
			if core.calls.Load() != 0 || outcome != observability.WorkerOutcomeSkippedUnclaimed {
				t.Fatalf("terminal run executed: calls=%d outcome=%s", core.calls.Load(), outcome)
			}

			state, found, err := w.store.GetRunStatus(ctx, env.RunID)
			if err != nil || !found || state != terminalStatus {
				t.Fatalf("terminal status changed: %s %v %v", state, found, err)
			}

			if runs, _, _, _ := svc.RetentionStats(); runs != 0 {
				t.Fatalf("terminal graph loaded: %d", runs)
			}
		})
	}
}
