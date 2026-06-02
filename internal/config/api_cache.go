package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/viper"
)

const (
	envAPICacheBackend = "VECTIS_API_CACHE_BACKEND"

	APICacheBackendDatabase = "database"
	APICacheBackendMemory   = "memory"
)

// APICacheBackend returns the storage backend used for API sessions and rate-limit buckets.
func APICacheBackend() string {
	if v := strings.TrimSpace(os.Getenv(envAPICacheBackend)); v != "" {
		return normalizeAPICacheBackend(v)
	}

	if viper.IsSet("api.cache.backend") {
		return normalizeAPICacheBackend(viper.GetString("api.cache.backend"))
	}

	return normalizeAPICacheBackend(MustDefaults().API.Cache.Backend)
}

func ValidateAPICacheConfig() error {
	switch APICacheBackend() {
	case APICacheBackendDatabase, APICacheBackendMemory:
		return nil
	default:
		return fmt.Errorf("api.cache.backend must be one of database or memory (got %q)", APICacheBackend())
	}
}

func normalizeAPICacheBackend(backend string) string {
	switch strings.ToLower(strings.TrimSpace(backend)) {
	case "", "db", "sql":
		return APICacheBackendDatabase
	default:
		return strings.ToLower(strings.TrimSpace(backend))
	}
}
