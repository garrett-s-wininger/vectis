package config

import (
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
