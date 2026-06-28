package secrets

import (
	"context"
	"errors"
	"testing"

	"vectis/internal/workloadidentity"
)

type fakeClaimValidator struct {
	runID       string
	executionID string
	claimToken  string
	err         error
}

func (v *fakeClaimValidator) ValidateActiveExecutionClaim(_ context.Context, runID, executionID, claimToken string) error {
	v.runID = runID
	v.executionID = executionID
	v.claimToken = claimToken
	return v.err
}

type fakeExecutionScopeResolver struct {
	runID       string
	executionID string
	scope       ExecutionScope
	err         error
}

func (r *fakeExecutionScopeResolver) ResolveExecutionScope(_ context.Context, runID, executionID string) (ExecutionScope, error) {
	r.runID = runID
	r.executionID = executionID
	if r.err != nil {
		return ExecutionScope{}, r.err
	}

	return r.scope, nil
}

func TestClaimAuthorizerAllowsActiveExecutionClaim(t *testing.T) {
	t.Parallel()

	validator := &fakeClaimValidator{}
	authorizer := NewClaimAuthorizer(validator)
	req := ResolveRequest{
		RunID:               "run-1",
		ExecutionID:         "execution-1",
		ExecutionClaimToken: "claim-1",
		PeerSPIFFEID:        "spiffe://vectis.internal/service/worker",
	}

	err := authorizer.AuthorizeResolve(context.Background(), &req)
	if err != nil {
		t.Fatalf("AuthorizeResolve: %v", err)
	}

	if validator.runID != "run-1" || validator.executionID != "execution-1" || validator.claimToken != "claim-1" {
		t.Fatalf("validator received run=%q execution=%q claim=%q", validator.runID, validator.executionID, validator.claimToken)
	}
}

func TestClaimAuthorizerRequiresPeerToMatchExpectedWorkload(t *testing.T) {
	t.Parallel()

	validator := &fakeClaimValidator{}
	expected := &fakeExecutionScopeResolver{
		scope: ExecutionScope{
			SPIFFEID:      "spiffe://vectis.internal/cell/local/job/job-1/run/run-1/execution/execution-1",
			NamespacePath: "/team-a",
			JobID:         "job-1",
			TaskKey:       "publish",
		},
	}

	authorizer := NewClaimAuthorizer(validator, WithExecutionScopeResolver(expected))
	req := ResolveRequest{
		RunID:               "run-1",
		ExecutionID:         "execution-1",
		ExecutionClaimToken: "claim-1",
		PeerSPIFFEID:        "spiffe://vectis.internal/cell/local/job/job-1/run/run-1/execution/execution-1",
	}

	err := authorizer.AuthorizeResolve(context.Background(), &req)
	if err != nil {
		t.Fatalf("AuthorizeResolve: %v", err)
	}

	if req.Scope.NamespacePath != "/team-a" || req.Scope.JobID != "job-1" || req.Scope.TaskKey != "publish" {
		t.Fatalf("request scope was not attached: %+v", req.Scope)
	}

	if expected.runID != "run-1" || expected.executionID != "execution-1" {
		t.Fatalf("expected resolver received run=%q execution=%q", expected.runID, expected.executionID)
	}
}

func TestClaimAuthorizerRejectsMismatchedExpectedWorkload(t *testing.T) {
	t.Parallel()

	authorizer := NewClaimAuthorizer(&fakeClaimValidator{}, WithExecutionScopeResolver(&fakeExecutionScopeResolver{
		scope: ExecutionScope{
			SPIFFEID: "spiffe://vectis.internal/cell/local/job/job-1/run/run-1/execution/execution-1",
		},
	}))

	req := ResolveRequest{
		RunID:               "run-1",
		ExecutionID:         "execution-1",
		ExecutionClaimToken: "claim-1",
		PeerSPIFFEID:        "spiffe://vectis.internal/cell/local/job/job-1/run/run-1/execution/other",
	}

	err := authorizer.AuthorizeResolve(context.Background(), &req)
	if !errors.Is(err, ErrDenied) {
		t.Fatalf("AuthorizeResolve error = %v, want ErrDenied", err)
	}
}

func TestClaimAuthorizerRejectsMismatchedExecutionScopeBinding(t *testing.T) {
	t.Parallel()

	spiffeID := "spiffe://vectis.internal/cell/local/job/job-1/run/run-1/execution/execution-1"
	authorizer := NewClaimAuthorizer(&fakeClaimValidator{}, WithExecutionScopeResolver(&fakeExecutionScopeResolver{
		scope: ExecutionScope{
			SPIFFEID:    spiffeID,
			RunID:       "other-run",
			ExecutionID: "execution-1",
		},
	}))

	req := ResolveRequest{
		RunID:               "run-1",
		ExecutionID:         "execution-1",
		ExecutionClaimToken: "claim-1",
		PeerSPIFFEID:        spiffeID,
	}

	err := authorizer.AuthorizeResolve(context.Background(), &req)
	if !errors.Is(err, ErrDenied) {
		t.Fatalf("AuthorizeResolve error = %v, want ErrDenied", err)
	}
}

func TestClaimAuthorizerRejectsMismatchedRequestWorkload(t *testing.T) {
	t.Parallel()

	spiffeID := "spiffe://vectis.internal/cell/local/job/job-1/run/run-1/execution/execution-1"
	authorizer := NewClaimAuthorizer(&fakeClaimValidator{})
	req := ResolveRequest{
		RunID:               "run-1",
		ExecutionID:         "execution-1",
		ExecutionClaimToken: "claim-1",
		PeerSPIFFEID:        spiffeID,
		Workload: &workloadidentity.Identity{
			SPIFFEID:    spiffeID,
			RunID:       "other-run",
			ExecutionID: "execution-1",
		},
	}

	err := authorizer.AuthorizeResolve(context.Background(), &req)
	if !errors.Is(err, ErrDenied) {
		t.Fatalf("AuthorizeResolve error = %v, want ErrDenied", err)
	}
}

func TestClaimAuthorizerRejectsMissingPeerOrClaimFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		req  ResolveRequest
	}{
		{
			name: "missing peer",
			req: ResolveRequest{
				RunID:               "run-1",
				ExecutionID:         "execution-1",
				ExecutionClaimToken: "claim-1",
			},
		},
		{
			name: "missing run",
			req: ResolveRequest{
				ExecutionID:         "execution-1",
				ExecutionClaimToken: "claim-1",
				PeerSPIFFEID:        "spiffe://vectis.internal/service/worker",
			},
		},
		{
			name: "missing execution",
			req: ResolveRequest{
				RunID:               "run-1",
				ExecutionClaimToken: "claim-1",
				PeerSPIFFEID:        "spiffe://vectis.internal/service/worker",
			},
		},
		{
			name: "missing claim",
			req: ResolveRequest{
				RunID:        "run-1",
				ExecutionID:  "execution-1",
				PeerSPIFFEID: "spiffe://vectis.internal/service/worker",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			authorizer := NewClaimAuthorizer(&fakeClaimValidator{})
			if err := authorizer.AuthorizeResolve(context.Background(), &tt.req); !errors.Is(err, ErrDenied) {
				t.Fatalf("AuthorizeResolve error = %v, want ErrDenied", err)
			}
		})
	}
}

func TestClaimAuthorizerRejectsInactiveExecutionClaim(t *testing.T) {
	t.Parallel()

	authorizer := NewClaimAuthorizer(&fakeClaimValidator{err: errors.New("expired lease")})
	req := ResolveRequest{
		RunID:               "run-1",
		ExecutionID:         "execution-1",
		ExecutionClaimToken: "claim-1",
		PeerSPIFFEID:        "spiffe://vectis.internal/service/worker",
	}

	err := authorizer.AuthorizeResolve(context.Background(), &req)
	if !errors.Is(err, ErrDenied) {
		t.Fatalf("AuthorizeResolve error = %v, want ErrDenied", err)
	}
}

func TestClaimAuthorizerAppliesAccessPolicyAfterScopeResolution(t *testing.T) {
	t.Parallel()

	policy, err := NewAccessPolicy([]string{
		"namespace=/team-a;job=job-1;task=publish;ref=encryptedfs://team-a/npm-token",
	})

	if err != nil {
		t.Fatalf("NewAccessPolicy: %v", err)
	}

	authorizer := NewClaimAuthorizer(
		&fakeClaimValidator{},
		WithExecutionScopeResolver(&fakeExecutionScopeResolver{
			scope: ExecutionScope{
				SPIFFEID:      "spiffe://vectis.internal/cell/local/job/job-1/run/run-1/execution/execution-1",
				NamespacePath: "/team-a",
				JobID:         "job-1",
				TaskKey:       "publish",
			},
		}),
		WithAccessPolicy(policy),
	)

	req := ResolveRequest{
		RunID:               "run-1",
		ExecutionID:         "execution-1",
		ExecutionClaimToken: "claim-1",
		PeerSPIFFEID:        "spiffe://vectis.internal/cell/local/job/job-1/run/run-1/execution/execution-1",
		Secrets: []Reference{{
			ID:  "npm-token",
			Ref: "encryptedfs://team-a/npm-token",
		}},
	}

	if err := authorizer.AuthorizeResolve(context.Background(), &req); err != nil {
		t.Fatalf("AuthorizeResolve: %v", err)
	}

	req.Secrets[0].Ref = "encryptedfs://team-a/deploy-token"
	if err := authorizer.AuthorizeResolve(context.Background(), &req); !errors.Is(err, ErrDenied) {
		t.Fatalf("AuthorizeResolve error = %v, want ErrDenied", err)
	}
}
