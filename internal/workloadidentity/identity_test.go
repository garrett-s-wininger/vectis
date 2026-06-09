package workloadidentity

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"strings"
	"testing"
)

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

	cert := &x509.Certificate{Raw: []byte{1, 2, 3}}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	got := identity.WithX509SVID(X509SVID{
		SPIFFEID:     identity.SPIFFEID,
		Certificates: []*x509.Certificate{cert},
		PrivateKey:   key,
	})

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

	if len(got.X509SVID.Certificates) != 1 || got.X509SVID.Certificates[0] != cert || got.X509SVID.PrivateKey != key {
		t.Fatalf("X509SVID material was not preserved: %+v", got.X509SVID)
	}

	got.X509SVID.Certificates[0] = nil
	if cert == nil || identity.X509SVID != nil {
		t.Fatalf("WithX509SVID did not isolate certificate slice")
	}
}

func TestX509SVIDTLSCertificate(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	leaf := &x509.Certificate{Raw: []byte{1, 2, 3}}
	intermediate := &x509.Certificate{Raw: []byte{4, 5, 6}}
	cert, err := X509SVID{
		SPIFFEID:     "spiffe://prod.example/cell/local/job/job-1/run/run-1/execution/execution-1",
		Certificates: []*x509.Certificate{leaf, intermediate},
		PrivateKey:   key,
	}.TLSCertificate()

	if err != nil {
		t.Fatalf("TLSCertificate: %v", err)
	}

	if cert.Leaf != leaf || cert.PrivateKey != key {
		t.Fatalf("TLSCertificate material = %+v, want leaf/private key", cert)
	}

	if len(cert.Certificate) != 2 || string(cert.Certificate[0]) != string(leaf.Raw) || string(cert.Certificate[1]) != string(intermediate.Raw) {
		t.Fatalf("TLSCertificate chain = %v", cert.Certificate)
	}

	cert.Certificate[0][0] = 9
	if leaf.Raw[0] == 9 {
		t.Fatal("TLSCertificate did not copy certificate DER")
	}
}

func TestX509SVIDTLSCertificateRequiresMaterial(t *testing.T) {
	tests := []struct {
		name string
		svid X509SVID
		want string
	}{
		{name: "missing id", svid: X509SVID{}, want: "SPIFFE ID"},
		{name: "missing cert", svid: X509SVID{SPIFFEID: "spiffe://prod.example/workload"}, want: "certificate chain"},
		{name: "missing key", svid: X509SVID{SPIFFEID: "spiffe://prod.example/workload", Certificates: []*x509.Certificate{{Raw: []byte{1}}}}, want: "private key"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.svid.TLSCertificate()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("TLSCertificate error = %v, want %q", err, tt.want)
			}
		})
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
