package localpki

import (
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/spf13/viper"
)

func TestEnsure_generatesMaterial(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	m, err := Ensure(dir)
	if err != nil {
		t.Fatal(err)
	}

	if m == nil {
		t.Fatal("nil material")
		return
	}

	for _, p := range []string{m.CAFile, m.ServerCert, m.ServerKey} {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("missing %q: %v", p, err)
		}
	}

	ev := m.EnvVars()
	if len(ev) != 15 {
		t.Fatalf("env vars: %d", len(ev))
	}

	if !slices.Contains(ev, "VECTIS_GRPC_TLS_SERVER_NAME=localhost") {
		t.Fatalf("EnvVars must include VECTIS_GRPC_TLS_SERVER_NAME=localhost for registry-backed TLS dials; got %v", ev)
	}

	leaf := readCertificate(t, m.ServerCert)
	if !hasLocalServiceIdentity(leaf) {
		t.Fatalf("generated server certificate missing URI SAN %q", LocalServiceIdentity)
	}

	for _, prefix := range []string{
		"VECTIS_GRPC_TLS_INSECURE=false",
		"VECTIS_GRPC_TLS_CA_FILE=",
		"VECTIS_GRPC_TLS_CERT_FILE=",
		"VECTIS_GRPC_TLS_KEY_FILE=",
		"VECTIS_GRPC_TLS_CLIENT_CA_FILE=",
		"VECTIS_GRPC_TLS_CLIENT_CERT_FILE=",
		"VECTIS_GRPC_TLS_CLIENT_KEY_FILE=",
		"VECTIS_SERVICE_IDENTITY_REGISTRY_ALLOWED_CLIENT_IDENTITIES=" + LocalServiceIdentity,
		"VECTIS_SERVICE_IDENTITY_QUEUE_ALLOWED_CLIENT_IDENTITIES=" + LocalServiceIdentity,
		"VECTIS_SERVICE_IDENTITY_LOG_ALLOWED_CLIENT_IDENTITIES=" + LocalServiceIdentity,
		"VECTIS_SERVICE_IDENTITY_ARTIFACT_ALLOWED_CLIENT_IDENTITIES=" + LocalServiceIdentity,
		"VECTIS_SERVICE_IDENTITY_ORCHESTRATOR_ALLOWED_CLIENT_IDENTITIES=" + LocalServiceIdentity,
		"VECTIS_SERVICE_IDENTITY_WORKER_CONTROL_ALLOWED_CLIENT_IDENTITIES=" + LocalServiceIdentity,
		"VECTIS_SERVICE_IDENTITY_CELL_INGRESS_ALLOWED_PRODUCER_IDENTITIES=" + LocalServiceIdentity,
	} {
		if !slices.ContainsFunc(ev, func(s string) bool { return strings.HasPrefix(s, prefix) }) {
			t.Fatalf("EnvVars missing entry with prefix %q: %v", prefix, ev)
		}
	}

	m2, err := Ensure(dir)
	if err != nil {
		t.Fatal(err)
	}

	if m2 == nil {
		t.Fatal("nil material on second Ensure")
		return
	}

	if m2.CAFile != m.CAFile {
		t.Fatal("paths changed on second Ensure")
	}
}

func TestMaterial_ApplyParentViper_setsTLSAndServerName(t *testing.T) {
	dir := t.TempDir()
	m, err := Ensure(dir)
	if err != nil {
		t.Fatal(err)
	}

	viper.Reset()
	t.Cleanup(viper.Reset)

	m.ApplyParentViper(viper.Set)

	if viper.GetBool("grpc_tls.insecure") {
		t.Fatalf("grpc_tls.insecure: got true want false")
	}

	if viper.GetString("grpc_tls.server_name") != "localhost" {
		t.Fatalf("grpc_tls.server_name: got %q want localhost", viper.GetString("grpc_tls.server_name"))
	}

	if viper.GetString("grpc_tls.ca_file") != m.CAFile {
		t.Fatal("grpc_tls.ca_file mismatch")
	}

	if viper.GetString("grpc_tls.client_ca_file") != m.CAFile {
		t.Fatal("grpc_tls.client_ca_file mismatch")
	}

	if viper.GetString("grpc_tls.client_cert_file") != m.ServerCert {
		t.Fatal("grpc_tls.client_cert_file mismatch")
	}

	if got := viper.GetStringSlice("service_identity.queue_allowed_client_identities"); !slices.Contains(got, LocalServiceIdentity) {
		t.Fatalf("service identity queue allowlist = %v, want %q", got, LocalServiceIdentity)
	}

	if got := viper.GetStringSlice("service_identity.orchestrator_allowed_client_identities"); !slices.Contains(got, LocalServiceIdentity) {
		t.Fatalf("service identity orchestrator allowlist = %v, want %q", got, LocalServiceIdentity)
	}
}

func TestEnsureDir(t *testing.T) {
	t.Parallel()
	want := filepath.Join("/tmp/xdgtest", "vectis", "local-tls")

	if d := EnsureDir("/tmp/xdgtest"); d != want {
		t.Fatalf("got %q want %q", d, want)
	}
}

func readCertificate(t *testing.T, path string) *x509.Certificate {
	t.Helper()

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read certificate %q: %v", path, err)
	}

	block, _ := pem.Decode(b)
	if block == nil {
		t.Fatalf("decode certificate PEM %q: missing block", path)
		return nil
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse certificate %q: %v", path, err)
	}

	return cert
}
