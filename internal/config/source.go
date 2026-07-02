package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/viper"

	"vectis/internal/source/refspec"
)

const (
	envSourceCheckoutRoot                                      = "VECTIS_SOURCE_CHECKOUT_ROOT"
	envAPIServerSourceCheckoutRoot                             = "VECTIS_API_SERVER_SOURCE_CHECKOUT_ROOT"
	envSourceSyncConfiguredRepositoriesOnStartup               = "VECTIS_SOURCE_SYNC_CONFIGURED_REPOSITORIES_ON_STARTUP"
	envAPIServerSourceSyncConfiguredRepositoriesOnStartup      = "VECTIS_API_SERVER_SOURCE_SYNC_CONFIGURED_REPOSITORIES_ON_STARTUP"
	envSourceSyncConfiguredRepositoriesInterval                = "VECTIS_SOURCE_SYNC_CONFIGURED_REPOSITORIES_INTERVAL"
	envAPIServerSourceSyncConfiguredRepositoriesInterval       = "VECTIS_API_SERVER_SOURCE_SYNC_CONFIGURED_REPOSITORIES_INTERVAL"
	envSourceSyncConfiguredRepositoriesMaxConcurrency          = "VECTIS_SOURCE_SYNC_CONFIGURED_REPOSITORIES_MAX_CONCURRENCY"
	envAPIServerSourceSyncConfiguredRepositoriesMaxConcurrency = "VECTIS_API_SERVER_SOURCE_SYNC_CONFIGURED_REPOSITORIES_MAX_CONCURRENCY"
	envSourceSyncConfiguredRepositoriesFailureBackoff          = "VECTIS_SOURCE_SYNC_CONFIGURED_REPOSITORIES_FAILURE_BACKOFF"
	envAPIServerSourceSyncConfiguredRepositoriesFailureBackoff = "VECTIS_API_SERVER_SOURCE_SYNC_CONFIGURED_REPOSITORIES_FAILURE_BACKOFF"
	envSourceSyncRunningTimeout                                = "VECTIS_SOURCE_SYNC_RUNNING_TIMEOUT"
	envAPIServerSourceSyncRunningTimeout                       = "VECTIS_API_SERVER_SOURCE_SYNC_RUNNING_TIMEOUT"
	envSourceRepositories                                      = "VECTIS_SOURCE_REPOSITORIES"
	envAPIServerSourceRepositories                             = "VECTIS_API_SERVER_SOURCE_REPOSITORIES"
	envWorkerSourceRepositories                                = "VECTIS_WORKER_SOURCE_REPOSITORIES"
	envWorkerCoreSourceRepositories                            = "VECTIS_WORKER_CORE_SOURCE_REPOSITORIES"
	envSourceSchedules                                         = "VECTIS_SOURCE_SCHEDULES"
	envAPIServerSourceSchedules                                = "VECTIS_API_SERVER_SOURCE_SCHEDULES"
	defaultSourceSyncConfiguredRepositoriesMaxConcurrency      = 1
	defaultSourceSyncRunningTimeout                            = 15 * time.Minute
	sourceSyncConfiguredRepositoriesOnStartupConfigKey         = "source.sync_configured_repositories_on_startup"
	sourceSyncConfiguredRepositoriesIntervalConfigKey          = "source.sync_configured_repositories_interval"
	sourceSyncConfiguredRepositoriesMaxConcurrencyConfigKey    = "source.sync_configured_repositories_max_concurrency"
	sourceSyncConfiguredRepositoriesFailureBackoffConfigKey    = "source.sync_configured_repositories_failure_backoff"
	sourceSyncRunningTimeoutConfigKey                          = "source.sync_running_timeout"
	sourceRepositoriesConfigKey                                = "source.repositories"
	sourceSchedulesConfigKey                                   = "source.schedules"
	sourceKindLocalCheckout                                    = "local_checkout"
	sourceCheckoutModeExternal                                 = "external"
	sourceCheckoutModeManaged                                  = "managed"
	sourceAuthoringModeReadOnly                                = "read_only"
	sourceAuthoringModeLocalCommit                             = "local_commit"
	sourceAuthoringModeExternalChangeRequest                   = "external_change_request"
	sourceWorkerCacheModeEphemeral                             = "ephemeral"
	sourceWorkerCacheModePersistent                            = "persistent"
)

type SourceRepositoryDeclaration struct {
	RepositoryID            string   `json:"repository_id" mapstructure:"repository_id" toml:"repository_id"`
	Namespace               string   `json:"namespace" mapstructure:"namespace" toml:"namespace"`
	SourceKind              string   `json:"source_kind" mapstructure:"source_kind" toml:"source_kind"`
	CheckoutPath            string   `json:"checkout_path" mapstructure:"checkout_path" toml:"checkout_path"`
	CheckoutMode            string   `json:"checkout_mode" mapstructure:"checkout_mode" toml:"checkout_mode"`
	AuthoringMode           string   `json:"authoring_mode" mapstructure:"authoring_mode" toml:"authoring_mode"`
	WorkerCacheMode         string   `json:"worker_cache_mode" mapstructure:"worker_cache_mode" toml:"worker_cache_mode"`
	CanonicalURL            string   `json:"canonical_url" mapstructure:"canonical_url" toml:"canonical_url"`
	FallbackRemoteURLs      []string `json:"fallback_remote_urls" mapstructure:"fallback_remote_urls" toml:"fallback_remote_urls"`
	WorkerCacheWarmRefspecs []string `json:"worker_cache_warm_refspecs" mapstructure:"worker_cache_warm_refspecs" toml:"worker_cache_warm_refspecs"`
	DefaultRef              string   `json:"default_ref" mapstructure:"default_ref" toml:"default_ref"`
	CredentialRef           string   `json:"credential_ref" mapstructure:"credential_ref" toml:"credential_ref"`
	Enabled                 *bool    `json:"enabled" mapstructure:"enabled" toml:"enabled"`
}

type SourceScheduleDeclaration struct {
	ScheduleID   string `json:"schedule_id" mapstructure:"schedule_id" toml:"schedule_id"`
	RepositoryID string `json:"repository_id" mapstructure:"repository_id" toml:"repository_id"`
	JobID        string `json:"job_id" mapstructure:"job_id" toml:"job_id"`
	CronSpec     string `json:"cron_spec" mapstructure:"cron_spec" toml:"cron_spec"`
	Ref          string `json:"ref" mapstructure:"ref" toml:"ref"`
	Path         string `json:"path" mapstructure:"path" toml:"path"`
	Enabled      *bool  `json:"enabled" mapstructure:"enabled" toml:"enabled"`
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

// SourceSyncConfiguredRepositoriesOnStartup reports whether vectis-api should
// sync configured source repositories during startup.
func SourceSyncConfiguredRepositoriesOnStartup() bool {
	for _, envName := range []string{envSourceSyncConfiguredRepositoriesOnStartup, envAPIServerSourceSyncConfiguredRepositoriesOnStartup} {
		if v := strings.TrimSpace(os.Getenv(envName)); v != "" {
			return parseTruthy(v)
		}
	}

	if viper.IsSet(sourceSyncConfiguredRepositoriesOnStartupConfigKey) {
		return viper.GetBool(sourceSyncConfiguredRepositoriesOnStartupConfigKey)
	}

	return MustDefaults().Source.SyncConfiguredRepositoriesOnStartup
}

// SourceSyncConfiguredRepositoriesInterval returns how often vectis-api should
// refresh configured source repositories in the background. A zero duration
// disables periodic sync.
func SourceSyncConfiguredRepositoriesInterval() time.Duration {
	for _, envName := range []string{envSourceSyncConfiguredRepositoriesInterval, envAPIServerSourceSyncConfiguredRepositoriesInterval} {
		if v := strings.TrimSpace(os.Getenv(envName)); v != "" {
			if d, err := time.ParseDuration(v); err == nil && d >= 0 {
				return d
			}
		}
	}

	if viper.IsSet(sourceSyncConfiguredRepositoriesIntervalConfigKey) {
		if d := viper.GetDuration(sourceSyncConfiguredRepositoriesIntervalConfigKey); d >= 0 {
			return d
		}
	}

	return time.Duration(MustDefaults().Source.SyncConfiguredRepositoriesInterval)
}

// SourceSyncConfiguredRepositoriesMaxConcurrency returns the maximum number of
// configured source repositories vectis-api may refresh at once.
func SourceSyncConfiguredRepositoriesMaxConcurrency() int {
	for _, envName := range []string{envSourceSyncConfiguredRepositoriesMaxConcurrency, envAPIServerSourceSyncConfiguredRepositoriesMaxConcurrency} {
		if v := strings.TrimSpace(os.Getenv(envName)); v != "" {
			if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
				return parsed
			}
		}
	}

	if viper.IsSet(sourceSyncConfiguredRepositoriesMaxConcurrencyConfigKey) {
		if v := viper.GetInt(sourceSyncConfiguredRepositoriesMaxConcurrencyConfigKey); v > 0 {
			return v
		}
	}

	if v := MustDefaults().Source.SyncConfiguredRepositoriesMaxConcurrency; v > 0 {
		return v
	}

	return defaultSourceSyncConfiguredRepositoriesMaxConcurrency
}

// SourceSyncConfiguredRepositoriesFailureBackoff returns how long a repository
// with a recent failed sync should be skipped by periodic background sync. A
// zero duration disables failure backoff.
func SourceSyncConfiguredRepositoriesFailureBackoff() time.Duration {
	for _, envName := range []string{envSourceSyncConfiguredRepositoriesFailureBackoff, envAPIServerSourceSyncConfiguredRepositoriesFailureBackoff} {
		if v := strings.TrimSpace(os.Getenv(envName)); v != "" {
			if d, err := time.ParseDuration(v); err == nil && d >= 0 {
				return d
			}
		}
	}

	if viper.IsSet(sourceSyncConfiguredRepositoriesFailureBackoffConfigKey) {
		if d := viper.GetDuration(sourceSyncConfiguredRepositoriesFailureBackoffConfigKey); d >= 0 {
			return d
		}
	}

	return time.Duration(MustDefaults().Source.SyncConfiguredRepositoriesFailureBackoff)
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
	for _, envName := range []string{envSourceRepositories, envAPIServerSourceRepositories, envWorkerSourceRepositories, envWorkerCoreSourceRepositories} {
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

func SourceScheduleDeclarations() ([]SourceScheduleDeclaration, error) {
	for _, envName := range []string{envSourceSchedules, envAPIServerSourceSchedules} {
		raw := strings.TrimSpace(os.Getenv(envName))
		if raw == "" {
			continue
		}

		var schedules []SourceScheduleDeclaration
		dec := json.NewDecoder(strings.NewReader(raw))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&schedules); err != nil {
			return nil, fmt.Errorf("%s: parse source schedules JSON: %w", envName, err)
		}

		return normalizeSourceScheduleDeclarations(schedules)
	}

	if viper.IsSet(sourceSchedulesConfigKey) {
		var schedules []SourceScheduleDeclaration
		if err := viper.UnmarshalKey(sourceSchedulesConfigKey, &schedules); err != nil {
			return nil, fmt.Errorf("%s: parse source schedules: %w", sourceSchedulesConfigKey, err)
		}

		return normalizeSourceScheduleDeclarations(schedules)
	}

	return normalizeSourceScheduleDeclarations(MustDefaults().Source.Schedules)
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
		repo.WorkerCacheMode = strings.TrimSpace(repo.WorkerCacheMode)
		repo.CanonicalURL = strings.TrimSpace(repo.CanonicalURL)
		fallbackRemoteURLs, err := normalizeSourceRepositoryFallbackRemoteURLs(repo.FallbackRemoteURLs)
		if err != nil {
			return nil, fmt.Errorf("source.repositories[%d].fallback_remote_urls: %w", i, err)
		}
		repo.FallbackRemoteURLs = fallbackRemoteURLs
		workerCacheWarmRefspecs, err := refspec.NormalizeFetchRefspecs(repo.WorkerCacheWarmRefspecs)
		if err != nil {
			return nil, fmt.Errorf("source.repositories[%d].worker_cache_warm_refspecs: %w", i, err)
		}
		repo.WorkerCacheWarmRefspecs = workerCacheWarmRefspecs
		repo.DefaultRef = strings.TrimSpace(repo.DefaultRef)
		repo.CredentialRef = strings.TrimSpace(repo.CredentialRef)

		if repo.RepositoryID == "" {
			return nil, fmt.Errorf("source.repositories[%d].repository_id is required", i)
		}

		if repo.SourceKind == "" {
			repo.SourceKind = sourceKindLocalCheckout
		}

		if repo.SourceKind != sourceKindLocalCheckout {
			return nil, fmt.Errorf("source.repositories[%d].source_kind %q is not supported", i, repo.SourceKind)
		}

		if repo.CheckoutMode == "" {
			repo.CheckoutMode = sourceCheckoutModeExternal
		}

		if !validSourceRepositoryCheckoutMode(repo.CheckoutMode) {
			return nil, fmt.Errorf("source.repositories[%d].checkout_mode %q is not supported", i, repo.CheckoutMode)
		}

		if repo.AuthoringMode == "" {
			repo.AuthoringMode = sourceAuthoringModeReadOnly
		}

		if !validSourceRepositoryAuthoringMode(repo.AuthoringMode) {
			return nil, fmt.Errorf("source.repositories[%d].authoring_mode %q is not supported", i, repo.AuthoringMode)
		}

		if repo.WorkerCacheMode == "" {
			repo.WorkerCacheMode = sourceWorkerCacheModeEphemeral
		}

		if !validSourceRepositoryWorkerCacheMode(repo.WorkerCacheMode) {
			return nil, fmt.Errorf("source.repositories[%d].worker_cache_mode %q is not supported", i, repo.WorkerCacheMode)
		}

		if repo.AuthoringMode == sourceAuthoringModeLocalCommit && repo.CheckoutMode != sourceCheckoutModeManaged {
			return nil, fmt.Errorf("source.repositories[%d].authoring_mode %q requires checkout_mode %q", i, repo.AuthoringMode, sourceCheckoutModeManaged)
		}

		if repo.CheckoutPath == "" && repo.CheckoutMode != sourceCheckoutModeManaged {
			return nil, fmt.Errorf("source.repositories[%d].checkout_path is required unless checkout_mode is %q", i, sourceCheckoutModeManaged)
		}

		if repo.DefaultRef != "" {
			ref, err := refspec.NormalizeRef(repo.DefaultRef)
			if err != nil {
				return nil, fmt.Errorf("source.repositories[%d].default_ref: %w", i, err)
			}

			repo.DefaultRef = ref
		}

		if _, ok := seen[repo.RepositoryID]; ok {
			return nil, fmt.Errorf("source.repositories[%d].repository_id %q is duplicated", i, repo.RepositoryID)
		}

		seen[repo.RepositoryID] = struct{}{}
		out = append(out, repo)
	}

	return out, nil
}

func normalizeSourceRepositoryFallbackRemoteURLs(in []string) ([]string, error) {
	if len(in) == 0 {
		return nil, nil
	}

	out := make([]string, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for i, raw := range in {
		if strings.TrimSpace(raw) == "" {
			continue
		}

		remoteURL := strings.TrimSpace(raw)
		if !safeSourceRepositoryGitRemoteURL(remoteURL) {
			return nil, fmt.Errorf("[%d] is not a safe Git remote", i)
		}

		if _, ok := seen[remoteURL]; ok {
			continue
		}

		seen[remoteURL] = struct{}{}
		out = append(out, remoteURL)
	}

	if len(out) == 0 {
		return nil, nil
	}

	return out, nil
}

func safeSourceRepositoryGitRemoteURL(remoteURL string) bool {
	return remoteURL != "" &&
		!strings.HasPrefix(remoteURL, "-") &&
		!strings.ContainsAny(remoteURL, "\x00\n\r")
}

func validSourceRepositoryCheckoutMode(mode string) bool {
	switch mode {
	case sourceCheckoutModeExternal, sourceCheckoutModeManaged:
		return true
	default:
		return false
	}
}

func validSourceRepositoryAuthoringMode(mode string) bool {
	switch mode {
	case sourceAuthoringModeReadOnly, sourceAuthoringModeLocalCommit, sourceAuthoringModeExternalChangeRequest:
		return true
	default:
		return false
	}
}

func validSourceRepositoryWorkerCacheMode(mode string) bool {
	switch mode {
	case sourceWorkerCacheModeEphemeral, sourceWorkerCacheModePersistent:
		return true
	default:
		return false
	}
}

func normalizeSourceScheduleDeclarations(in []SourceScheduleDeclaration) ([]SourceScheduleDeclaration, error) {
	if len(in) == 0 {
		return nil, nil
	}

	out := make([]SourceScheduleDeclaration, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for i, sched := range in {
		sched.ScheduleID = strings.TrimSpace(sched.ScheduleID)
		sched.RepositoryID = strings.TrimSpace(sched.RepositoryID)
		sched.JobID = strings.TrimSpace(sched.JobID)
		sched.CronSpec = strings.TrimSpace(sched.CronSpec)
		sched.Ref = strings.TrimSpace(sched.Ref)
		sched.Path = strings.TrimSpace(sched.Path)

		if sched.ScheduleID == "" {
			return nil, fmt.Errorf("source.schedules[%d].schedule_id is required", i)
		}

		if sched.RepositoryID == "" {
			return nil, fmt.Errorf("source.schedules[%d].repository_id is required", i)
		}

		if sched.JobID == "" {
			return nil, fmt.Errorf("source.schedules[%d].job_id is required", i)
		}

		if sched.Ref != "" {
			ref, err := refspec.NormalizeRef(sched.Ref)
			if err != nil {
				return nil, fmt.Errorf("source.schedules[%d].ref: %w", i, err)
			}

			sched.Ref = ref
		}

		if sched.Path != "" {
			filePath, err := refspec.NormalizeTreePath(sched.Path)
			if err != nil {
				return nil, fmt.Errorf("source.schedules[%d].path: %w", i, err)
			}

			sched.Path = filePath
		} else if _, err := refspec.DefinitionPathForJobID(sched.JobID); err != nil {
			return nil, fmt.Errorf("source.schedules[%d].job_id: %w", i, err)
		}

		if sched.CronSpec == "" {
			return nil, fmt.Errorf("source.schedules[%d].cron_spec is required", i)
		}

		if _, ok := seen[sched.ScheduleID]; ok {
			return nil, fmt.Errorf("source.schedules[%d].schedule_id %q is duplicated", i, sched.ScheduleID)
		}

		seen[sched.ScheduleID] = struct{}{}
		out = append(out, sched)
	}

	return out, nil
}
