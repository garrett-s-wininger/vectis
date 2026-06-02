package config

import (
	"testing"
	"time"

	"github.com/spf13/viper"

	"vectis/internal/localpki"
)

func TestAPIHTTPS_DefaultsDisabled(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	if APIHTTPSEnabled() {
		t.Fatal("API HTTPS should be disabled by default")
	}

	if got := APIURLScheme(); got != "http" {
		t.Fatalf("APIURLScheme() = %q, want http", got)
	}

	if err := ValidateAPIHTTPS(); err != nil {
		t.Fatalf("ValidateAPIHTTPS default: %v", err)
	}
}

func TestValidateAPIHTTPS_requiresPair(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("api.tls.cert_file", "/tmp/cert.pem")
	if err := ValidateAPIHTTPS(); err == nil {
		t.Fatal("expected cert without key to be invalid")
	}
}

func TestValidateAPIHTTPS_acceptsLocalPKI(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	m, err := localpki.Ensure(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	viper.Set("api.tls.cert_file", m.ServerCert)
	viper.Set("api.tls.key_file", m.ServerKey)
	viper.Set("api.tls.reload_interval", time.Minute)

	if !APIHTTPSEnabled() {
		t.Fatal("API HTTPS should be enabled when cert/key are set")
	}

	if got := APIURLScheme(); got != "https" {
		t.Fatalf("APIURLScheme() = %q, want https", got)
	}

	if got := APIHTTPSReloadInterval(); got != time.Minute {
		t.Fatalf("APIHTTPSReloadInterval() = %v, want 1m", got)
	}

	if err := ValidateAPIHTTPS(); err != nil {
		t.Fatalf("ValidateAPIHTTPS local PKI: %v", err)
	}
}
