package secrets

import (
	"context"
	"fmt"
	"strings"

	"vectis/internal/serviceidentity"
)

type ExecutionClaimValidator interface {
	ValidateActiveExecutionClaim(ctx context.Context, runID, executionID, claimToken string) error
}

type ExpectedWorkloadResolver interface {
	ExpectedWorkloadSPIFFEID(ctx context.Context, runID, executionID string) (string, error)
}

type ClaimAuthorizer struct {
	claims            ExecutionClaimValidator
	expectedWorkloads ExpectedWorkloadResolver
}

type ClaimAuthorizerOption func(*ClaimAuthorizer)

func WithExpectedWorkloadResolver(resolver ExpectedWorkloadResolver) ClaimAuthorizerOption {
	return func(a *ClaimAuthorizer) {
		a.expectedWorkloads = resolver
	}
}

func NewClaimAuthorizer(claims ExecutionClaimValidator, opts ...ClaimAuthorizerOption) *ClaimAuthorizer {
	a := &ClaimAuthorizer{claims: claims}
	for _, opt := range opts {
		if opt != nil {
			opt(a)
		}
	}
	return a
}

func (a *ClaimAuthorizer) AuthorizeResolve(ctx context.Context, req ResolveRequest) error {
	if a == nil || a.claims == nil {
		return fmt.Errorf("%w: execution claim validator is not configured", ErrDenied)
	}

	peerSPIFFEID, err := normalizePeerSPIFFEID(req.PeerSPIFFEID)
	if err != nil {
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

	if a.expectedWorkloads != nil {
		expected, err := a.expectedWorkloads.ExpectedWorkloadSPIFFEID(ctx, runID, executionID)
		if err != nil {
			return fmt.Errorf("%w: expected workload identity is unavailable: %v", ErrDenied, err)
		}

		expected, err = normalizePeerSPIFFEID(expected)
		if err != nil {
			return fmt.Errorf("%w: expected workload SPIFFE ID is invalid: %v", ErrDenied, err)
		}

		if peerSPIFFEID != expected {
			return fmt.Errorf("%w: peer SPIFFE ID does not match execution workload identity", ErrDenied)
		}
	}

	return nil
}

func normalizePeerSPIFFEID(raw string) (string, error) {
	normalized, err := serviceidentity.NormalizeSPIFFEAllowlist([]string{raw})
	if err != nil {
		return "", err
	}

	if len(normalized) == 0 {
		return "", fmt.Errorf("empty SPIFFE ID")
	}

	return normalized[0], nil
}
