package s3

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

func TestConfigBindAndLoadFromViper(t *testing.T) {
	v := viper.New()
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	AddConfigFlags(flags)
	if err := BindConfig(v, flags); err != nil {
		t.Fatalf("BindConfig: %v", err)
	}

	if err := flags.Parse([]string{
		"--s3-endpoint", "http://127.0.0.1:8333",
		"--s3-region", "us-test-1",
		"--s3-bucket", "vectis",
		"--s3-prefix", "ci/artifacts",
		"--s3-access-key-id", "access",
		"--s3-secret-access-key", "secret",
		"--s3-session-token", "token",
		"--s3-path-style=false",
		"--s3-temp-dir", "/data/vectis/artifact/s3-tmp",
	}); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	cfg := ConfigFromViper(v)
	if !cfg.Enabled() {
		t.Fatal("config should be enabled")
	}

	if cfg.Endpoint != "http://127.0.0.1:8333" ||
		cfg.Region != "us-test-1" ||
		cfg.Bucket != "vectis" ||
		cfg.Prefix != "ci/artifacts" ||
		cfg.AccessKeyID != "access" ||
		cfg.SecretAccessKey != "secret" ||
		cfg.SessionToken != "token" ||
		cfg.PathStyle ||
		cfg.TempDir != "/data/vectis/artifact/s3-tmp" {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestConfigReadsSecretAccessKeyFile(t *testing.T) {
	dir := t.TempDir()
	keyFile := filepath.Join(dir, "secret")
	if err := os.WriteFile(keyFile, []byte("from-file\n"), 0o600); err != nil {
		t.Fatalf("write key file: %v", err)
	}

	cfg := Config{
		Endpoint:            "http://127.0.0.1:8333",
		Bucket:              "vectis",
		AccessKeyID:         "access",
		SecretAccessKey:     "inline",
		SecretAccessKeyFile: keyFile,
		PathStyle:           true,
		TempDir:             "/artifact/tmp",
	}

	store, err := cfg.NewStore()
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	if store.secretAccessKey != "from-file" {
		t.Fatalf("secretAccessKey = %q", store.secretAccessKey)
	}

	if store.tempDir != "/artifact/tmp" {
		t.Fatalf("tempDir = %q", store.tempDir)
	}
}

func TestConfigBindsEnv(t *testing.T) {
	t.Setenv(EnvEndpoint, "http://127.0.0.1:9000")
	t.Setenv(EnvBucket, "vectis-env")
	t.Setenv(EnvPathStyle, "false")
	t.Setenv(EnvTempDir, "/env/tmp")

	v := viper.New()
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	AddConfigFlags(flags)
	if err := BindConfig(v, flags); err != nil {
		t.Fatalf("BindConfig: %v", err)
	}

	cfg := ConfigFromViper(v)
	if cfg.Endpoint != "http://127.0.0.1:9000" || cfg.Bucket != "vectis-env" || cfg.PathStyle || cfg.TempDir != "/env/tmp" {
		t.Fatalf("unexpected env config: %+v", cfg)
	}
}
