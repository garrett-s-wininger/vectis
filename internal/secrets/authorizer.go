package secrets

import (
	"context"
	"fmt"
	"strings"
)

type ExecutionClaimValidator interface {
	ValidateActiveExecutionClaim(ctx context.Context, runID, executionID, claimToken string) error
}

type ClaimAuthorizer struct {
	claims ExecutionClaimValidator
}

func NewClaimAuthorizer(claims ExecutionClaimValidator) *ClaimAuthorizer {
	return &ClaimAuthorizer{claims: claims}
}

func (a *ClaimAuthorizer) AuthorizeResolve(ctx context.Context, req ResolveRequest) error {
	if a == nil || a.claims == nil {
		return fmt.Errorf("%w: execution claim validator is not configured", ErrDenied)
	}

	if strings.TrimSpace(req.PeerSPIFFEID) == "" {
		return fmt.Errorf("%w: peer SPIFFE ID is required", ErrDenied)
	}

	runID := strings.TrimSpace(req.RunID)
	executionID := strings.TrimSpace(req.ExecutionID)
	claimToken := strings.TrimSpace(req.ExecutionClaimToken)
	if runID == "" || executionID == "" || claimToken == "" {
		return fmt.Errorf("%w: run_id, execution_id, and execution_claim_token are required", ErrDenied)
	}

	if err := a.claims.ValidateActiveExecutionClaim(ctx, runID, executionID, claimToken); err != nil {
		return fmt.Errorf("%w: execution claim is not active: %v", ErrDenied, err)
	}

	return nil
}
