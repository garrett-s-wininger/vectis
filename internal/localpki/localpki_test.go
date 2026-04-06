package localpki

import (
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
	}

	for _, p := range []string{m.CAFile, m.ServerCert, m.ServerKey} {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("missing %q: %v", p, err)
		}
	}

	ev := m.EnvVars()
	if len(ev) != 5 {
		t.Fatalf("env vars: %d", len(ev))
	}

	if !slices.Contains(ev, "VECTIS_GRPC_TLS_SERVER_NAME=localhost") {
		t.Fatalf("EnvVars must include VECTIS_GRPC_TLS_SERVER_NAME=localhost for registry-backed TLS dials; got %v", ev)
	}

	for _, prefix := range []string{
		"VECTIS_GRPC_TLS_INSECURE=false",
		"VECTIS_GRPC_TLS_CA_FILE=",
		"VECTIS_GRPC_TLS_CERT_FILE=",
		"VECTIS_GRPC_TLS_KEY_FILE=",
	} {
		if !slices.ContainsFunc(ev, func(s string) bool { return strings.HasPrefix(s, prefix) }) {
			t.Fatalf("EnvVars missing entry with prefix %q: %v", prefix, ev)
		}
	}

	m2, err := Ensure(dir)
	if err != nil {
		t.Fatal(err)
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
}

func TestEnsureDir(t *testing.T) {
	t.Parallel()
	want := filepath.Join("/tmp/xdgtest", "vectis", "local-tls")

	if d := EnsureDir("/tmp/xdgtest"); d != want {
		t.Fatalf("got %q want %q", d, want)
	}
}
