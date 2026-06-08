package workloadidentity

import "testing"

func TestSPIFFEIDDefaultTemplate(t *testing.T) {
	got, err := SPIFFEID("Prod.Example", "", validExecution())
	if err != nil {
		t.Fatalf("SPIFFEID: %v", err)
	}

	want := "spiffe://prod.example/cell/iad-a/namespace/teams/build/job/job-1/run/run-1/execution/execution-1"
	if got != want {
		t.Fatalf("SPIFFEID = %q, want %q", got, want)
	}
}

func TestSPIFFEIDRootNamespace(t *testing.T) {
	exec := validExecution()
	exec.NamespacePath = "/"

	got, err := SPIFFEID("prod.example", "", exec)
	if err != nil {
		t.Fatalf("SPIFFEID: %v", err)
	}

	want := "spiffe://prod.example/cell/iad-a/namespace/root/job/job-1/run/run-1/execution/execution-1"
	if got != want {
		t.Fatalf("SPIFFEID = %q, want %q", got, want)
	}
}

func TestSPIFFEIDCustomTemplate(t *testing.T) {
	got, err := SPIFFEID("prod.example", "/cell/{cell}/run/{run_index}/attempt/{attempt}/def/{definition_version}", validExecution())
	if err != nil {
		t.Fatalf("SPIFFEID: %v", err)
	}

	want := "spiffe://prod.example/cell/iad-a/run/7/attempt/2/def/3"
	if got != want {
		t.Fatalf("SPIFFEID = %q, want %q", got, want)
	}
}

func TestSPIFFEIDRejectsMissingRequiredField(t *testing.T) {
	exec := validExecution()
	exec.ExecutionID = ""

	if _, err := SPIFFEID("prod.example", "", exec); err == nil {
		t.Fatal("SPIFFEID accepted missing execution ID")
	}
}

func TestSPIFFEIDRejectsBadTrustDomain(t *testing.T) {
	if _, err := SPIFFEID("prod.example/path", "", validExecution()); err == nil {
		t.Fatal("SPIFFEID accepted trust domain with path")
	}
}

func TestSPIFFEIDRejectsBadTemplate(t *testing.T) {
	tests := []struct {
		name     string
		template string
	}{
		{name: "relative", template: "cell/{cell}"},
		{name: "unknown placeholder", template: "/cell/{cluster}"},
		{name: "unterminated", template: "/cell/{cell"},
		{name: "unmatched close", template: "/cell/{cell}}"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := SPIFFEID("prod.example", tt.template, validExecution()); err == nil {
				t.Fatalf("SPIFFEID accepted template %q", tt.template)
			}
		})
	}
}

func TestNewIdentityIncludesPolicyFields(t *testing.T) {
	got, err := NewIdentity("prod.example", "", validExecution())
	if err != nil {
		t.Fatalf("NewIdentity: %v", err)
	}

	if got.SPIFFEID == "" {
		t.Fatal("SPIFFEID is empty")
	}

	if got.TrustDomain != "prod.example" {
		t.Fatalf("TrustDomain = %q, want prod.example", got.TrustDomain)
	}

	if got.NamespacePath != "/teams/build" {
		t.Fatalf("NamespacePath = %q, want /teams/build", got.NamespacePath)
	}

	if got.CellID != "iad-a" || got.JobID != "job-1" || got.RunID != "run-1" || got.ExecutionID != "execution-1" {
		t.Fatalf("unexpected identity fields: %+v", got)
	}
}

func TestIdentityWithX509SVIDReturnsCopy(t *testing.T) {
	identity, err := NewIdentity("prod.example", "", validExecution())
	if err != nil {
		t.Fatalf("NewIdentity: %v", err)
	}

	got := identity.WithX509SVID(X509SVID{SPIFFEID: identity.SPIFFEID})
	if got == nil {
		t.Fatal("WithX509SVID returned nil")
	}

	if got == identity {
		t.Fatal("WithX509SVID returned original identity")
	}

	if identity.X509SVID != nil {
		t.Fatalf("original identity was mutated: %+v", identity.X509SVID)
	}

	if got.X509SVID == nil || got.X509SVID.SPIFFEID != identity.SPIFFEID {
		t.Fatalf("X509SVID = %+v, want %q", got.X509SVID, identity.SPIFFEID)
	}
}

func validExecution() Execution {
	return Execution{
		CellID:            "iad-a",
		NamespacePath:     "/teams/build",
		JobID:             "job-1",
		RunID:             "run-1",
		RunIndex:          7,
		SegmentID:         "segment-1",
		ExecutionID:       "execution-1",
		Attempt:           2,
		DefinitionVersion: 3,
		DefinitionHash:    "sha256:abc123",
	}
}
