package config

import (
	"crypto/tls"
	"strings"
	"testing"

	"github.com/spf13/viper"

	"vectis/internal/localpki"
)

func TestGRPCResolverDialOptions_localPKIBootstrapViper(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	dir := t.TempDir()
	m, err := localpki.Ensure(dir)
	if err != nil {
		t.Fatal(err)
	}
	m.ApplyParentViper(viper.Set)

	if err := ValidateGRPCTLSForRole(GRPCTLSDaemonClientOnly); err != nil {
		t.Fatalf("ValidateGRPCTLSForRole(client): %v", err)
	}

	opts, err := GRPCResolverDialOptions()
	if err != nil {
		t.Fatalf("GRPCResolverDialOptions: %v", err)
	}

	if len(opts) == 0 {
		t.Fatal("expected at least one grpc.DialOption (transport credentials)")
	}
}

func TestValidateGRPCTLSForRoleSecretsRequiresClientCA(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	dir := t.TempDir()
	m, err := localpki.Ensure(dir)
	if err != nil {
		t.Fatal(err)
	}

	m.ApplyParentViper(viper.Set)
	viper.Set("grpc_tls.client_ca_file", "")

	err = ValidateGRPCTLSForRole(GRPCTLSDaemonSecrets)
	if err == nil || !strings.Contains(err.Error(), "client_ca_file") {
		t.Fatalf("ValidateGRPCTLSForRole(secrets) error = %v, want client_ca_file", err)
	}
}

func TestValidateGRPCTLSForRoleSecretsAcceptsLocalPKI(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	dir := t.TempDir()
	m, err := localpki.Ensure(dir)
	if err != nil {
		t.Fatal(err)
	}

	m.ApplyParentViper(viper.Set)

	if err := ValidateGRPCTLSForRole(GRPCTLSDaemonSecrets); err != nil {
		t.Fatalf("ValidateGRPCTLSForRole(secrets): %v", err)
	}
}

func TestGRPCClientDialOptionsWithClientCertificateRequiresMTLS(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("grpc_tls.insecure", true)

	_, err := GRPCClientDialOptionsWithClientCertificate("", tls.Certificate{
		Certificate: [][]byte{{1}},
		PrivateKey:  struct{}{},
	})

	if err == nil || !strings.Contains(err.Error(), "grpc_tls.insecure=false") {
		t.Fatalf("GRPCClientDialOptionsWithClientCertificate error = %v, want insecure=false", err)
	}
}

func TestGRPCClientDialOptionsWithClientCertificateRequiresMaterial(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("grpc_tls.insecure", false)

	_, err := GRPCClientDialOptionsWithClientCertificate("", tls.Certificate{})
	if err == nil || !strings.Contains(err.Error(), "client certificate") {
		t.Fatalf("GRPCClientDialOptionsWithClientCertificate error = %v, want material error", err)
	}
}
