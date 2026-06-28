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

type ExecutionScopeResolver interface {
	ResolveExecutionScope(ctx context.Context, runID, executionID string) (ExecutionScope, error)
}

type ClaimAuthorizer struct {
	claims          ExecutionClaimValidator
	executionScopes ExecutionScopeResolver
	policy          AccessPolicy
}

type ClaimAuthorizerOption func(*ClaimAuthorizer)

func WithExecutionScopeResolver(resolver ExecutionScopeResolver) ClaimAuthorizerOption {
	return func(a *ClaimAuthorizer) {
		a.executionScopes = resolver
	}
}

func WithAccessPolicy(policy AccessPolicy) ClaimAuthorizerOption {
	return func(a *ClaimAuthorizer) {
		a.policy = policy
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

func (a *ClaimAuthorizer) AuthorizeResolve(ctx context.Context, req *ResolveRequest) error {
	if a == nil || a.claims == nil {
		return fmt.Errorf("%w: execution claim validator is not configured", ErrDenied)
	}

	if req == nil {
		return fmt.Errorf("%w: resolve request is required", ErrDenied)
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

	req.RunID = runID
	req.ExecutionID = executionID
	req.ExecutionClaimToken = claimToken
	req.PeerSPIFFEID = peerSPIFFEID
	if err := ValidateResolveIdentityBinding(req); err != nil {
		return fmt.Errorf("%w: %w", ErrDenied, err)
	}

	if err := a.claims.ValidateActiveExecutionClaim(ctx, runID, executionID, claimToken); err != nil {
		return fmt.Errorf("%w: execution claim is not active: %w", ErrDenied, err)
	}

	if a.executionScopes != nil {
		scope, err := a.executionScopes.ResolveExecutionScope(ctx, runID, executionID)
		if err != nil {
			return fmt.Errorf("%w: execution scope is unavailable: %w", ErrDenied, err)
		}

		req.Scope = scope
		if err := ValidateResolveIdentityBinding(req); err != nil {
			return fmt.Errorf("%w: %w", ErrDenied, err)
		}
	}

	if a.policy != nil {
		if err := a.policy.AuthorizeResolve(ctx, *req); err != nil {
			return err
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
