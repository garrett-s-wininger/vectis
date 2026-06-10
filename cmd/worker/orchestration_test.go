package main

import (
	"context"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/orchestrator"
)

type sqlExecutionChoreographer struct {
	runs dal.RunsRepository
}

func (c sqlExecutionChoreographer) LoadRun(context.Context, *api.Job, *cell.ExecutionEnvelope) error {
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

func (c *localOrchestratorChoreographer) LoadRun(ctx context.Context, j *api.Job, env *cell.ExecutionEnvelope) error {
	spec, err := orchestrator.RunSpecFromJobAndEnvelope(j, env)
	if err != nil {
		return err
	}

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
