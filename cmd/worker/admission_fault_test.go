package main

import (
	"context"
	"errors"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/dal"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type unavailableAdmissionDiscarder struct {
	*retirementChoreographer
	discardErr error
}

func (c *unavailableAdmissionDiscarder) DiscardRunAdmission(ctx context.Context, runID, token string) error {
	if c.discardErr != nil {
		return c.discardErr
	}

	return c.service.DiscardRunAdmission(ctx, runID, token)
}

func TestWorkerFault_RejectedAdmissionReceiptSurvivesWorkerReplacement(t *testing.T) {
	w, c, j, env, claim, _ := retirementWorker(t)
	ctx := t.Context()
	if _, ok := w.completeExecutionAndFinalizeRunByClaim(ctx, j, env, claim, dal.ExecutionStatusSucceeded, "", ""); !ok {
		t.Fatal("completion failed")
	}

	if err := c.LoadRun(ctx, j, env, nil); err != nil {
		t.Fatal(err)
	}

	ch := &unavailableAdmissionDiscarder{retirementChoreographer: c, discardErr: errors.New("discard unavailable")}
	w.choreographer = ch
	if _, claimed, _, err := w.tryClaimExecution(ctx, j, env, time.Now().Add(time.Hour)); claimed || err != nil {
		t.Fatalf("claim = %v %v", claimed, err)
	}

	if runs, _, _, _ := c.service.RetentionStats(); runs != 1 {
		t.Fatal("graph discarded before successful delivery")
	}

	if receipts, err := w.retirements.ListDue(ctx, time.Now().Unix(), 256); err != nil || len(receipts) != 1 {
		t.Fatalf("discard receipt missing: %+v %v", receipts, err)
	}

	ch.discardErr = nil
	replacement := &worker{store: w.store, retirements: w.retirements, choreographer: ch, logger: w.logger}
	if err := replacement.drainRetirements(ctx); err != nil {
		t.Fatal(err)
	}

	if runs, _, _, _ := c.service.RetentionStats(); runs != 0 {
		t.Fatal("replacement failed to discard rejected admission")
	}

	if receipts, err := w.retirements.ListDue(ctx, time.Now().Unix(), 256); err != nil || len(receipts) != 0 {
		t.Fatalf("delivered receipt retained: %+v %v", receipts, err)
	}
}

func TestWorkerFault_LateOrphanPreservesTerminalResult(t *testing.T) {
	w, _, j, env, claim, _ := retirementWorker(t)
	ctx := t.Context()
	if _, ok := w.completeExecutionAndFinalizeRunByClaim(ctx, j, env, claim, dal.ExecutionStatusSucceeded, "", ""); !ok {
		t.Fatal("initial completion failed")
	}

	if err := w.store.MarkRunOrphaned(ctx, env.RunID, "ack_uncertain"); err != nil {
		t.Fatal(err)
	}

	state, _, err := w.store.GetRunStatus(ctx, env.RunID)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("durable succeeded run after late orphan write: %s", state)
	if state != dal.RunStatusSucceeded {
		t.Fatal("late orphan report overwrote a terminal result")
	}
}

type auditClaimDeadlineClient struct {
	api.OrchestratorServiceClient
	bounded bool
}

func (c *auditClaimDeadlineClient) ClaimExecution(ctx context.Context, _ *api.ClaimExecutionRequest, _ ...grpc.CallOption) (*api.ClaimExecutionResponse, error) {
	_, c.bounded = ctx.Deadline()
	return nil, status.Error(codes.Unavailable, "injected failure")
}

func TestWorkerFault_ClaimRPCIsBounded(t *testing.T) {
	w, _, j, env, _, _ := retirementWorker(t)
	client := &auditClaimDeadlineClient{}
	w.choreographer = &grpcExecutionChoreographer{client: client, streamDisabled: true}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	_, _, _, _ = w.tryClaimExecution(ctx, j, env, time.Now().Add(time.Hour))
	t.Logf("worker claim RPC has a deadline: %v", client.bounded)
	if !client.bounded {
		t.Fatal("worker dropped the caller deadline without adding a replacement bound")
	}
}

func TestWorkerFault_RejectedAdmissionIsReclaimed(t *testing.T) {
	w, c, j, env, claim, _ := retirementWorker(t)
	ctx := t.Context()
	if _, ok := w.completeExecutionAndFinalizeRunByClaim(ctx, j, env, claim, dal.ExecutionStatusSucceeded, "", ""); !ok {
		t.Fatal("completion failed")
	}

	if err := c.LoadRun(ctx, j, env, nil); err != nil {
		t.Fatal(err)
	}

	if _, claimed, _, err := w.tryClaimExecution(ctx, j, env, time.Now().Add(time.Hour)); claimed || err != nil {
		t.Fatalf("claim = %v %v", claimed, err)
	}

	runs, tasks, pending, _ := c.service.RetentionStats()
	t.Logf("durably completed run after skipped late claim: retained runs=%d tasks=%d awaiting persistence=%d", runs, tasks, pending)
	if runs != 0 {
		t.Fatal("late delivery resurrected a graph without a reclamation receipt")
	}
}
