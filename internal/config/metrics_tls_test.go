package config

import (
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/spf13/viper"
)

func TestValidateMetricsTLS_insecureDefault(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	if err := ValidateMetricsTLS(); err != nil {
		t.Fatal(err)
	}
}

func TestValidateMetricsTLS_requiresCertWhenEnabled(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("metrics_tls.insecure", false)
	viper.Set("metrics_tls.cert_file", "")
	viper.Set("metrics_tls.key_file", "")

	if err := ValidateMetricsTLS(); err == nil {
		t.Fatal("expected error when insecure=false and cert paths empty")
	}
}

func TestValidateMetricsTLS_okWithOpenSSLGeneratedPair(t *testing.T) {
	if _, err := exec.LookPath("openssl"); err != nil {
		t.Skip("openssl not in PATH")
	}

	viper.Reset()
	t.Cleanup(viper.Reset)

	dir := t.TempDir()
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")
	cmd := exec.Command("openssl", "req", "-x509", "-newkey", "rsa:2048", "-sha256", "-days", "1", "-nodes",
		"-keyout", keyPath, "-out", certPath, "-subj", "/CN=localhost")

	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("openssl: %v\n%s", err, out)
	}

	viper.Set("metrics_tls.insecure", false)
	viper.Set("metrics_tls.cert_file", certPath)
	viper.Set("metrics_tls.key_file", keyPath)

	if err := ValidateMetricsTLS(); err != nil {
		t.Fatal(err)
	}
}
