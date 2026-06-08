package runfixture

import (
	"context"
	"testing"
	"time"

	"vectis/internal/dal"
)

func FinalizeExecutionByClaim(t testing.TB, ctx context.Context, repos *dal.SQLRepositories, executionID, status string) dal.ExecutionFinalizationResult {
	t.Helper()

	return FinalizeExecutionByClaimWithFailure(t, ctx, repos, executionID, status, "", "")
}

func FinalizeExecutionByClaimWithFailure(t testing.TB, ctx context.Context, repos *dal.SQLRepositories, executionID, status, failureCode, reason string) dal.ExecutionFinalizationResult {
	t.Helper()

	dispatch, err := repos.Runs().GetExecutionDispatch(ctx, executionID)
	if err != nil {
		t.Fatalf("get execution dispatch %s: %v", executionID, err)
	}

	owner := "runfixture-finalizer"
	leaseUntil := time.Now().Add(dal.DefaultLeaseTTL)
	_, found, err := repos.Runs().GetRunStatus(ctx, dispatch.RunID)
	if err != nil {
		t.Fatalf("get run status %s: %v", dispatch.RunID, err)
	}
	if !found {
		t.Fatalf("run %s not found for execution %s", dispatch.RunID, executionID)
	}

	claim, err := repos.Runs().TryClaimExecution(ctx, executionID, owner, leaseUntil)
	if err != nil {
		t.Fatalf("claim execution %s: %v", executionID, err)
	}
	if !claim.Claimed || claim.ClaimToken == "" {
		t.Fatalf("execution %s was not claimable", executionID)
	}

	result, err := repos.Runs().CompleteExecutionAndFinalizeRunByClaim(ctx, executionID, owner, claim.ClaimToken, status, failureCode, reason)
	if err != nil {
		t.Fatalf("finalize execution %s as %s: %v", executionID, status, err)
	}

	return result
}
