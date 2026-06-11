package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/viper"
)

const (
	envSourceCheckoutRoot                = "VECTIS_SOURCE_CHECKOUT_ROOT"
	envAPIServerSourceCheckoutRoot       = "VECTIS_API_SERVER_SOURCE_CHECKOUT_ROOT"
	envSourceStoredJobsEnabled           = "VECTIS_SOURCE_STORED_JOBS_ENABLED"
	envAPIServerSourceStoredJobsEnabled  = "VECTIS_API_SERVER_SOURCE_STORED_JOBS_ENABLED"
	envSourceSyncRunningTimeout          = "VECTIS_SOURCE_SYNC_RUNNING_TIMEOUT"
	envAPIServerSourceSyncRunningTimeout = "VECTIS_API_SERVER_SOURCE_SYNC_RUNNING_TIMEOUT"
	envSourceRepositories                = "VECTIS_SOURCE_REPOSITORIES"
	envAPIServerSourceRepositories       = "VECTIS_API_SERVER_SOURCE_REPOSITORIES"
	defaultSourceSyncRunningTimeout      = 15 * time.Minute
	sourceStoredJobsEnabledConfigKey     = "source.stored_jobs_enabled"
	sourceSyncRunningTimeoutConfigKey    = "source.sync_running_timeout"
	sourceRepositoriesConfigKey          = "source.repositories"
)

type SourceRepositoryDeclaration struct {
	RepositoryID  string `json:"repository_id" mapstructure:"repository_id" toml:"repository_id"`
	Namespace     string `json:"namespace" mapstructure:"namespace" toml:"namespace"`
	SourceKind    string `json:"source_kind" mapstructure:"source_kind" toml:"source_kind"`
	CheckoutPath  string `json:"checkout_path" mapstructure:"checkout_path" toml:"checkout_path"`
	CheckoutMode  string `json:"checkout_mode" mapstructure:"checkout_mode" toml:"checkout_mode"`
	AuthoringMode string `json:"authoring_mode" mapstructure:"authoring_mode" toml:"authoring_mode"`
	CanonicalURL  string `json:"canonical_url" mapstructure:"canonical_url" toml:"canonical_url"`
	DefaultRef    string `json:"default_ref" mapstructure:"default_ref" toml:"default_ref"`
	CredentialRef string `json:"credential_ref" mapstructure:"credential_ref" toml:"credential_ref"`
	Enabled       *bool  `json:"enabled" mapstructure:"enabled" toml:"enabled"`
}

// SourceCheckoutRoot returns the root directory for Vectis-managed source checkouts.
func SourceCheckoutRoot(dataHome string) string {
	root := strings.TrimSpace(os.Getenv(envSourceCheckoutRoot))
	if root == "" {
		root = strings.TrimSpace(os.Getenv(envAPIServerSourceCheckoutRoot))
	}

	if root == "" && viper.IsSet("source.checkout_root") {
		root = strings.TrimSpace(viper.GetString("source.checkout_root"))
	}

	if root == "" {
		root = strings.TrimSpace(MustDefaults().Source.CheckoutRoot)
	}

	return strings.NewReplacer(
		"{{data_home}}", dataHome,
	).Replace(root)
}

// SourceStoredJobsEnabled reports whether stored job definition APIs are enabled.
func SourceStoredJobsEnabled() bool {
	for _, envName := range []string{envSourceStoredJobsEnabled, envAPIServerSourceStoredJobsEnabled} {
		if v := strings.TrimSpace(os.Getenv(envName)); v != "" {
			return parseTruthy(v)
		}
	}

	if viper.IsSet(sourceStoredJobsEnabledConfigKey) {
		return viper.GetBool(sourceStoredJobsEnabledConfigKey)
	}

	return MustDefaults().Source.StoredJobsEnabled
}

// SourceSyncRunningTimeout returns how long a running source sync reservation may live
// before another caller can reclaim it.
func SourceSyncRunningTimeout() time.Duration {
	for _, envName := range []string{envSourceSyncRunningTimeout, envAPIServerSourceSyncRunningTimeout} {
		if v := strings.TrimSpace(os.Getenv(envName)); v != "" {
			if d, err := time.ParseDuration(v); err == nil && d > 0 {
				return d
			}
		}
	}

	if viper.IsSet(sourceSyncRunningTimeoutConfigKey) {
		if d := viper.GetDuration(sourceSyncRunningTimeoutConfigKey); d > 0 {
			return d
		}
	}

	if d := time.Duration(MustDefaults().Source.SyncRunningTimeout); d > 0 {
		return d
	}

	return defaultSourceSyncRunningTimeout
}

func SourceRepositoryDeclarations() ([]SourceRepositoryDeclaration, error) {
	for _, envName := range []string{envSourceRepositories, envAPIServerSourceRepositories} {
		raw := strings.TrimSpace(os.Getenv(envName))
		if raw == "" {
			continue
		}

		var repos []SourceRepositoryDeclaration
		dec := json.NewDecoder(strings.NewReader(raw))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&repos); err != nil {
			return nil, fmt.Errorf("%s: parse source repositories JSON: %w", envName, err)
		}

		return normalizeSourceRepositoryDeclarations(repos)
	}

	if viper.IsSet(sourceRepositoriesConfigKey) {
		var repos []SourceRepositoryDeclaration
		if err := viper.UnmarshalKey(sourceRepositoriesConfigKey, &repos); err != nil {
			return nil, fmt.Errorf("%s: parse source repositories: %w", sourceRepositoriesConfigKey, err)
		}

		return normalizeSourceRepositoryDeclarations(repos)
	}

	return normalizeSourceRepositoryDeclarations(MustDefaults().Source.Repositories)
}

func normalizeSourceRepositoryDeclarations(in []SourceRepositoryDeclaration) ([]SourceRepositoryDeclaration, error) {
	if len(in) == 0 {
		return nil, nil
	}

	out := make([]SourceRepositoryDeclaration, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for i, repo := range in {
		repo.RepositoryID = strings.TrimSpace(repo.RepositoryID)
		repo.Namespace = strings.TrimSpace(repo.Namespace)
		repo.SourceKind = strings.TrimSpace(repo.SourceKind)
		repo.CheckoutPath = strings.TrimSpace(repo.CheckoutPath)
		repo.CheckoutMode = strings.TrimSpace(repo.CheckoutMode)
		repo.AuthoringMode = strings.TrimSpace(repo.AuthoringMode)
		repo.CanonicalURL = strings.TrimSpace(repo.CanonicalURL)
		repo.DefaultRef = strings.TrimSpace(repo.DefaultRef)
		repo.CredentialRef = strings.TrimSpace(repo.CredentialRef)

		if repo.RepositoryID == "" {
			return nil, fmt.Errorf("source.repositories[%d].repository_id is required", i)
		}

		if _, ok := seen[repo.RepositoryID]; ok {
			return nil, fmt.Errorf("source.repositories[%d].repository_id %q is duplicated", i, repo.RepositoryID)
		}

		seen[repo.RepositoryID] = struct{}{}
		out = append(out, repo)
	}

	return out, nil
}
