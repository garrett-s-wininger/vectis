package config

import (
	"testing"

	"github.com/spf13/viper"
)

func TestAPICacheBackendDefaultsToDatabase(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	if got := APICacheBackend(); got != APICacheBackendDatabase {
		t.Fatalf("APICacheBackend() = %q, want %q", got, APICacheBackendDatabase)
	}
}

func TestAPICacheBackendOverrides(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("api.cache.backend", "memory")
	if got := APICacheBackend(); got != APICacheBackendMemory {
		t.Fatalf("APICacheBackend() viper = %q, want %q", got, APICacheBackendMemory)
	}

	t.Setenv(envAPICacheBackend, "sql")
	if got := APICacheBackend(); got != APICacheBackendDatabase {
		t.Fatalf("APICacheBackend() env alias = %q, want %q", got, APICacheBackendDatabase)
	}
}

func TestValidateAPICacheConfig(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("api.cache.backend", "not-real")
	if err := ValidateAPICacheConfig(); err == nil {
		t.Fatal("expected invalid backend error")
	}
}
