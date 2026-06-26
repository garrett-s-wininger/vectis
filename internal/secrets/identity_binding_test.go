package secrets

import (
	"errors"
	"testing"
)

func validResolveIdentityRequest() ResolveRequest {
	spiffeID := "spiffe://prod.example/cell/iad-a/namespace/team-a/job/job-1/run/run-1/execution/execution-1"
	return ResolveRequest{
		RunID:        "run-1",
		ExecutionID:  "execution-1",
		PeerSPIFFEID: spiffeID,
		Workload: &WorkloadIdentity{
			SPIFFEID:      spiffeID,
			TrustDomain:   "prod.example",
			NamespacePath: "/team-a",
			CellID:        "iad-a",
			JobID:         "job-1",
			RunID:         "run-1",
			ExecutionID:   "execution-1",
			X509SVID: &X509SVID{
				SPIFFEID: spiffeID,
			},
		},
		Scope: ExecutionScope{
			SPIFFEID:      spiffeID,
			TrustDomain:   "prod.example",
			NamespacePath: "/team-a",
			CellID:        "iad-a",
			JobID:         "job-1",
			RunID:         "run-1",
			ExecutionID:   "execution-1",
		},
	}
}

func TestValidateResolveIdentityBindingAllowsMatchingIdentity(t *testing.T) {
	t.Parallel()

	req := validResolveIdentityRequest()
	req.RunID = " run-1 "
	req.ExecutionID = " execution-1 "
	req.Workload.TrustDomain = " PROD.EXAMPLE "
	req.Workload.NamespacePath = " /team-a "
	req.Scope.TrustDomain = " PROD.EXAMPLE "

	if err := ValidateResolveIdentityBinding(&req); err != nil {
		t.Fatalf("ValidateResolveIdentityBinding: %v", err)
	}

	if req.RunID != "run-1" || req.ExecutionID != "execution-1" {
		t.Fatalf("request identity was not canonicalized: %+v", req)
	}

	if req.Workload.TrustDomain != "prod.example" || req.Scope.TrustDomain != "prod.example" {
		t.Fatalf("trust domain was not canonicalized: workload=%q scope=%q", req.Workload.TrustDomain, req.Scope.TrustDomain)
	}
}

func TestValidateResolveIdentityBindingRejectsMismatches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		edit func(*ResolveRequest)
	}{
		{
			name: "workload run",
			edit: func(req *ResolveRequest) {
				req.Workload.RunID = "other-run"
			},
		},
		{
			name: "scope execution",
			edit: func(req *ResolveRequest) {
				req.Scope.ExecutionID = "other-execution"
			},
		},
		{
			name: "peer spiffe",
			edit: func(req *ResolveRequest) {
				req.PeerSPIFFEID = "spiffe://prod.example/cell/iad-a/namespace/team-a/job/job-1/run/run-1/execution/other"
			},
		},
		{
			name: "workload svid",
			edit: func(req *ResolveRequest) {
				req.Workload.X509SVID.SPIFFEID = "spiffe://prod.example/cell/iad-a/namespace/team-a/job/job-1/run/run-1/execution/other"
			},
		},
		{
			name: "scope job",
			edit: func(req *ResolveRequest) {
				req.Scope.JobID = "other-job"
			},
		},
		{
			name: "missing workload spiffe",
			edit: func(req *ResolveRequest) {
				req.Workload.SPIFFEID = ""
			},
		},
		{
			name: "invalid scope spiffe",
			edit: func(req *ResolveRequest) {
				req.Scope.SPIFFEID = "not-spiffe"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := validResolveIdentityRequest()
			tt.edit(&req)
			if err := ValidateResolveIdentityBinding(&req); !errors.Is(err, ErrInvalidResolveIdentity) {
				t.Fatalf("ValidateResolveIdentityBinding error = %v, want ErrInvalidResolveIdentity", err)
			}
		})
	}
}
