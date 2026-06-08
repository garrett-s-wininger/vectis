package spire

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type fakeX509SVIDSource struct {
	svids []X509SVID
	err   error
}

func (s fakeX509SVIDSource) FetchX509SVIDs(context.Context) ([]X509SVID, error) {
	if s.err != nil {
		return nil, s.err
	}

	return append([]X509SVID(nil), s.svids...), nil
}

func TestValidateWorkloadAPIAddress(t *testing.T) {
	if err := ValidateWorkloadAPIAddress("unix:///tmp/spire-agent.sock"); err != nil {
		t.Fatalf("ValidateWorkloadAPIAddress unix: %v", err)
	}

	if err := ValidateWorkloadAPIAddress(""); err == nil {
		t.Fatal("ValidateWorkloadAPIAddress accepted empty address")
	}

	if err := ValidateWorkloadAPIAddress("not a workload api address"); err == nil {
		t.Fatal("ValidateWorkloadAPIAddress accepted invalid address")
	}
}

func TestRequireX509SVIDAcceptsMatchingSVID(t *testing.T) {
	source := fakeX509SVIDSource{svids: []X509SVID{
		{SPIFFEID: "spiffe://prod.example/cell/local/job/other"},
		{SPIFFEID: "spiffe://prod.example/cell/local/job/job-1/run/run-1/execution/execution-1"},
	}}

	err := RequireX509SVID(
		context.Background(),
		source,
		"spiffe://prod.example/cell/local/job/job-1/run/run-1/execution/execution-1",
	)
	if err != nil {
		t.Fatalf("RequireX509SVID: %v", err)
	}
}

func TestRequireX509SVIDRejectsMissingSVID(t *testing.T) {
	source := fakeX509SVIDSource{svids: []X509SVID{
		{SPIFFEID: "spiffe://prod.example/cell/local/job/other"},
	}}

	err := RequireX509SVID(
		context.Background(),
		source,
		"spiffe://prod.example/cell/local/job/job-1/run/run-1/execution/execution-1",
	)
	if err == nil || !strings.Contains(err.Error(), "no X.509-SVID") {
		t.Fatalf("RequireX509SVID error = %v, want missing SVID", err)
	}
}

func TestRequireX509SVIDPropagatesSourceError(t *testing.T) {
	sourceErr := errors.New("workload API unavailable")
	err := RequireX509SVID(
		context.Background(),
		fakeX509SVIDSource{err: sourceErr},
		"spiffe://prod.example/cell/local/job/job-1/run/run-1/execution/execution-1",
	)
	if !errors.Is(err, sourceErr) {
		t.Fatalf("RequireX509SVID error = %v, want %v", err, sourceErr)
	}
}

func TestRequireX509SVIDRejectsBadExpectedID(t *testing.T) {
	err := RequireX509SVID(context.Background(), fakeX509SVIDSource{}, "https://prod.example/not-spiffe")
	if err == nil || !strings.Contains(err.Error(), "spiffe://") {
		t.Fatalf("RequireX509SVID error = %v, want SPIFFE validation error", err)
	}
}
