package secrets

import (
	"context"
	"errors"
	"testing"
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

type fakeExpectedWorkloadResolver struct {
	runID       string
	executionID string
	spiffeID    string
	err         error
}

func (r *fakeExpectedWorkloadResolver) ExpectedWorkloadSPIFFEID(_ context.Context, runID, executionID string) (string, error) {
	r.runID = runID
	r.executionID = executionID
	if r.err != nil {
		return "", r.err
	}
	return r.spiffeID, nil
}

func TestClaimAuthorizerAllowsActiveExecutionClaim(t *testing.T) {
	t.Parallel()

	validator := &fakeClaimValidator{}
	authorizer := NewClaimAuthorizer(validator)
	err := authorizer.AuthorizeResolve(context.Background(), ResolveRequest{
		RunID:               "run-1",
		ExecutionID:         "execution-1",
		ExecutionClaimToken: "claim-1",
		PeerSPIFFEID:        "spiffe://vectis.local/service/worker",
	})

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
	expected := &fakeExpectedWorkloadResolver{
		spiffeID: "spiffe://vectis.local/cell/local/job/job-1/run/run-1/execution/execution-1",
	}

	authorizer := NewClaimAuthorizer(validator, WithExpectedWorkloadResolver(expected))
	err := authorizer.AuthorizeResolve(context.Background(), ResolveRequest{
		RunID:               "run-1",
		ExecutionID:         "execution-1",
		ExecutionClaimToken: "claim-1",
		PeerSPIFFEID:        "spiffe://vectis.local/cell/local/job/job-1/run/run-1/execution/execution-1",
	})

	if err != nil {
		t.Fatalf("AuthorizeResolve: %v", err)
	}

	if expected.runID != "run-1" || expected.executionID != "execution-1" {
		t.Fatalf("expected resolver received run=%q execution=%q", expected.runID, expected.executionID)
	}
}

func TestClaimAuthorizerRejectsMismatchedExpectedWorkload(t *testing.T) {
	t.Parallel()

	authorizer := NewClaimAuthorizer(&fakeClaimValidator{}, WithExpectedWorkloadResolver(&fakeExpectedWorkloadResolver{
		spiffeID: "spiffe://vectis.local/cell/local/job/job-1/run/run-1/execution/execution-1",
	}))
	err := authorizer.AuthorizeResolve(context.Background(), ResolveRequest{
		RunID:               "run-1",
		ExecutionID:         "execution-1",
		ExecutionClaimToken: "claim-1",
		PeerSPIFFEID:        "spiffe://vectis.local/cell/local/job/job-1/run/run-1/execution/other",
	})

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
				PeerSPIFFEID:        "spiffe://vectis.local/service/worker",
			},
		},
		{
			name: "missing execution",
			req: ResolveRequest{
				RunID:               "run-1",
				ExecutionClaimToken: "claim-1",
				PeerSPIFFEID:        "spiffe://vectis.local/service/worker",
			},
		},
		{
			name: "missing claim",
			req: ResolveRequest{
				RunID:        "run-1",
				ExecutionID:  "execution-1",
				PeerSPIFFEID: "spiffe://vectis.local/service/worker",
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			authorizer := NewClaimAuthorizer(&fakeClaimValidator{})
			if err := authorizer.AuthorizeResolve(context.Background(), tt.req); !errors.Is(err, ErrDenied) {
				t.Fatalf("AuthorizeResolve error = %v, want ErrDenied", err)
			}
		})
	}
}

func TestClaimAuthorizerRejectsInactiveExecutionClaim(t *testing.T) {
	t.Parallel()

	authorizer := NewClaimAuthorizer(&fakeClaimValidator{err: errors.New("expired lease")})
	err := authorizer.AuthorizeResolve(context.Background(), ResolveRequest{
		RunID:               "run-1",
		ExecutionID:         "execution-1",
		ExecutionClaimToken: "claim-1",
		PeerSPIFFEID:        "spiffe://vectis.local/service/worker",
	})

	if !errors.Is(err, ErrDenied) {
		t.Fatalf("AuthorizeResolve error = %v, want ErrDenied", err)
	}
}
