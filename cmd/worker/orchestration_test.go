package main

import (
	"context"
	"net"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/orchestrator"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type sqlExecutionChoreographer struct {
	runs dal.RunsRepository
}

func (c sqlExecutionChoreographer) completesExecutionDurably() bool {
	return true
}

func (c sqlExecutionChoreographer) LoadRun(context.Context, *api.Job, *cell.ExecutionEnvelope, []orchestrator.TaskExecutionSnapshot) error {
	return nil
}

func (c sqlExecutionChoreographer) ClaimAndStartExecution(ctx context.Context, env *cell.ExecutionEnvelope, owner string, leaseUntil time.Time) (dal.ExecutionClaimResult, error) {
	claim, err := c.runs.TryClaimExecution(ctx, env.ExecutionID, owner, leaseUntil)
	if err != nil || !claim.Claimed {
		return claim, err
	}

	if claim.TransitionedToAccepted {
		if err := c.runs.MarkExecutionStarted(ctx, env.ExecutionID); err != nil {
			return dal.ExecutionClaimResult{}, err
		}
		claim.ExecutionStarted = true
	}

	return claim, nil
}

func (c sqlExecutionChoreographer) RenewExecutionLease(ctx context.Context, env *cell.ExecutionEnvelope, owner, claimToken string, leaseUntil time.Time) error {
	return c.runs.RenewExecutionLease(ctx, env.ExecutionID, owner, claimToken, leaseUntil)
}

func (c sqlExecutionChoreographer) CompleteExecution(ctx context.Context, env *cell.ExecutionEnvelope, owner, claimToken, status, failureCode, reason string) (dal.ExecutionFinalizationResult, error) {
	return c.runs.CompleteExecutionAndFinalizeRunByClaim(ctx, env.ExecutionID, owner, claimToken, status, failureCode, reason)
}

func (c sqlExecutionChoreographer) RequiresDurableTaskRows() bool {
	return true
}

type localOrchestratorChoreographer struct {
	service *orchestrator.Service
}

func newLocalOrchestratorChoreographer(t cleanupTestingT) *localOrchestratorChoreographer {
	t.Helper()

	service := orchestrator.New(1)
	t.Cleanup(service.Close)
	return &localOrchestratorChoreographer{service: service}
}

type cleanupTestingT interface {
	Helper()
	Cleanup(func())
}

func (c *localOrchestratorChoreographer) LoadRun(ctx context.Context, j *api.Job, env *cell.ExecutionEnvelope, snapshots []orchestrator.TaskExecutionSnapshot) error {
	spec, err := orchestrator.RunSpecFromJobAndEnvelope(j, env)
	if err != nil {
		return err
	}

	spec.Executions = append([]orchestrator.TaskExecutionSnapshot(nil), snapshots...)
	_, err = c.service.LoadRun(ctx, spec)
	return err
}

func (c *localOrchestratorChoreographer) ClaimAndStartExecution(ctx context.Context, env *cell.ExecutionEnvelope, owner string, leaseUntil time.Time) (dal.ExecutionClaimResult, error) {
	return c.service.ClaimExecution(ctx, env.RunID, env.ExecutionID, owner, leaseUntil)
}

func (c *localOrchestratorChoreographer) RenewExecutionLease(ctx context.Context, env *cell.ExecutionEnvelope, owner, claimToken string, leaseUntil time.Time) error {
	return c.service.RenewExecutionLease(ctx, env.RunID, env.ExecutionID, owner, claimToken, leaseUntil)
}

func (c *localOrchestratorChoreographer) CompleteExecution(ctx context.Context, env *cell.ExecutionEnvelope, owner, claimToken, status, failureCode, reason string) (dal.ExecutionFinalizationResult, error) {
	return c.service.CompleteExecutionByClaim(ctx, env.RunID, env.ExecutionID, owner, claimToken, status, failureCode, reason)
}

func (c *localOrchestratorChoreographer) RequiresDurableTaskRows() bool {
	return false
}

func TestGRPCExecutionChoreographerLoadRunHydratesSnapshots(t *testing.T) {
	ctx := context.Background()
	service := orchestrator.New(1)
	t.Cleanup(service.Close)

	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	orchestrator.RegisterOrchestratorService(server, service, interfaces.NewLogger("orchestrator-test"))
	go func() {
		_ = server.Serve(listener)
	}()

	conn, err := grpc.NewClient(
		"passthrough:///worker-orchestrator",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	if err != nil {
		t.Fatalf("dial orchestrator: %v", err)
	}

	t.Cleanup(func() {
		_ = conn.Close()
		server.Stop()
		_ = listener.Close()
	})

	runID := "run-grpc-hydrate"
	rootID := "root-node"
	childID := "build"
	rootAction := "builtins/parallel"
	childAction := "builtins/shell"
	j := &api.Job{
		Id:    stringPtr("job-grpc-hydrate"),
		RunId: stringPtr(runID),
		Root: &api.Node{
			Id:   &rootID,
			Uses: &rootAction,
			Steps: []*api.Node{{
				Id:   &childID,
				Uses: &childAction,
				With: map[string]string{"command": "echo build"},
			}},
		},
	}

	root := defaultTaskExecutionRecord(runID, dal.RootTaskKey, "", dal.RootTaskKey, "local")
	child := defaultTaskExecutionRecord(runID, childID, dal.RootTaskKey, childID, "local")
	rootEnv := executionEnvelopeFromRecord(root)
	childEnv := executionEnvelopeFromRecord(child)
	choreographer := newGRPCExecutionChoreographer(api.NewOrchestratorServiceClient(conn))

	err = choreographer.LoadRun(ctx, j, rootEnv, []orchestrator.TaskExecutionSnapshot{
		{Record: root, Status: dal.ExecutionStatusSucceeded},
		{Record: child, Status: dal.ExecutionStatusRunning},
	})

	if err != nil {
		t.Fatalf("load hydrated run over grpc: %v", err)
	}

	claim, err := choreographer.ClaimAndStartExecution(ctx, childEnv, "worker-grpc", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("claim hydrated child over grpc: %v", err)
	}

	if !claim.Claimed || claim.ClaimToken == "" {
		t.Fatalf("expected hydrated child claim over grpc, got %+v", claim)
	}

	result, err := choreographer.CompleteExecution(ctx, childEnv, "worker-grpc", claim.ClaimToken, dal.ExecutionStatusSucceeded, "", "")
	if err != nil {
		t.Fatalf("complete hydrated child over grpc: %v", err)
	}

	if result.Outcome != dal.ExecutionFinalizationOutcomeRunSucceeded {
		t.Fatalf("hydrated grpc finalization outcome: %+v", result)
	}
}

func executionEnvelopeFromRecord(record dal.TaskExecutionRecord) *cell.ExecutionEnvelope {
	return &cell.ExecutionEnvelope{
		RunID:             record.RunID,
		TaskID:            record.TaskID,
		TaskKey:           record.TaskKey,
		TaskName:          record.Name,
		TaskAttemptID:     record.TaskAttemptID,
		TaskAttempt:       record.Attempt,
		SegmentID:         record.SegmentID,
		ExecutionID:       record.ExecutionID,
		CellID:            record.CellID,
		DefinitionVersion: 1,
		DefinitionHash:    "test-definition-hash",
	}
}
