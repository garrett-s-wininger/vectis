package config

import (
	"fmt"
	"strings"
	"time"

	"vectis/internal/serviceidentity"
	"vectis/internal/spire"

	"github.com/spf13/viper"
)

func init() {
	_ = viper.BindEnv("worker.spire.enabled", "VECTIS_WORKER_SPIRE_ENABLED")
	_ = viper.BindEnv("worker.spire.workload_api_address", "VECTIS_WORKER_SPIRE_WORKLOAD_API_ADDRESS")
	_ = viper.BindEnv("worker.spire.fetch_timeout", "VECTIS_WORKER_SPIRE_FETCH_TIMEOUT")
	_ = viper.BindEnv("worker.spire.registration.enabled", "VECTIS_WORKER_SPIRE_REGISTRATION_ENABLED")
	_ = viper.BindEnv("worker.spire.registration.server_address", "VECTIS_WORKER_SPIRE_REGISTRATION_SERVER_ADDRESS")
	_ = viper.BindEnv("worker.spire.registration.parent_id", "VECTIS_WORKER_SPIRE_REGISTRATION_PARENT_ID")
	_ = viper.BindEnv("worker.spire.registration.selectors", "VECTIS_WORKER_SPIRE_REGISTRATION_SELECTORS")
	_ = viper.BindEnv("worker.spire.registration.x509_svid_ttl", "VECTIS_WORKER_SPIRE_REGISTRATION_X509_SVID_TTL")
	_ = viper.BindEnv("worker.spire.registration.min_ttl", "VECTIS_WORKER_SPIRE_REGISTRATION_MIN_TTL")
	_ = viper.BindEnv("worker.spire.registration.max_ttl", "VECTIS_WORKER_SPIRE_REGISTRATION_MAX_TTL")
}

func WorkerSPIREEnabled() bool {
	if viper.IsSet("worker.spire.enabled") {
		return viper.GetBool("worker.spire.enabled")
	}

	return MustDefaults().Worker.SPIRE.Enabled
}

func WorkerSPIREWorkloadAPIAddress() string {
	if v := strings.TrimSpace(viper.GetString("worker.spire.workload_api_address")); v != "" {
		return v
	}

	return strings.TrimSpace(MustDefaults().Worker.SPIRE.WorkloadAPIAddress)
}

func WorkerSPIREFetchTimeout() time.Duration {
	d, err := workerSPIREFetchTimeout()
	if err != nil {
		return 0
	}

	return d
}

func WorkerSPIRERegistrationEnabled() bool {
	if viper.IsSet("worker.spire.registration.enabled") {
		return viper.GetBool("worker.spire.registration.enabled")
	}

	return MustDefaults().Worker.SPIRE.Registration.Enabled
}

func WorkerSPIRERegistrationServerAddress() string {
	if v := strings.TrimSpace(viper.GetString("worker.spire.registration.server_address")); v != "" {
		return v
	}

	return strings.TrimSpace(MustDefaults().Worker.SPIRE.Registration.ServerAddress)
}

func WorkerSPIRERegistrationParentID() string {
	if v := strings.TrimSpace(viper.GetString("worker.spire.registration.parent_id")); v != "" {
		return v
	}

	return strings.TrimSpace(MustDefaults().Worker.SPIRE.Registration.ParentID)
}

func WorkerSPIRERegistrationSelectorSpecs() []string {
	return coalesceStringSlices(
		stringSliceFromViper("worker.spire.registration.selectors"),
		MustDefaults().Worker.SPIRE.Registration.Selectors,
	)
}

func WorkerSPIRERegistrationSelectors() ([]spire.Selector, error) {
	specs := WorkerSPIRERegistrationSelectorSpecs()
	if len(specs) == 0 {
		return nil, nil
	}

	selectors := make([]spire.Selector, 0, len(specs))
	for _, spec := range specs {
		selector, err := spire.ParseSelector(spec)
		if err != nil {
			return nil, fmt.Errorf("worker.spire.registration.selectors: %w", err)
		}

		selectors = append(selectors, selector)
	}

	selectors, err := spire.NormalizeSelectors(selectors)
	if err != nil {
		return nil, fmt.Errorf("worker.spire.registration.selectors: %w", err)
	}

	return selectors, nil
}

func WorkerSPIRERegistrationX509SVIDTTL() time.Duration {
	d, err := workerSPIRERegistrationDuration(
		"worker.spire.registration.x509_svid_ttl",
		"worker.spire.registration.x509_svid_ttl",
		time.Duration(MustDefaults().Worker.SPIRE.Registration.X509SVIDTTL),
	)

	if err != nil {
		return 0
	}

	return d
}

func WorkerSPIRERegistrationMinTTL() time.Duration {
	d, err := workerSPIRERegistrationDuration(
		"worker.spire.registration.min_ttl",
		"worker.spire.registration.min_ttl",
		time.Duration(MustDefaults().Worker.SPIRE.Registration.MinTTL),
	)

	if err != nil {
		return 0
	}

	return d
}

func WorkerSPIRERegistrationMaxTTL() time.Duration {
	d, err := workerSPIRERegistrationDuration(
		"worker.spire.registration.max_ttl",
		"worker.spire.registration.max_ttl",
		time.Duration(MustDefaults().Worker.SPIRE.Registration.MaxTTL),
	)

	if err != nil {
		return 0
	}

	return d
}

func ValidateWorkerSPIREConfig() error {
	address := WorkerSPIREWorkloadAPIAddress()
	if _, err := workerSPIREFetchTimeout(); err != nil {
		return err
	}

	if err := validateWorkerSPIRERegistrationConfig(); err != nil {
		return err
	}

	if !WorkerSPIREEnabled() {
		if address != "" {
			if err := spire.ValidateWorkloadAPIAddress(address); err != nil {
				return fmt.Errorf("worker.spire: %w", err)
			}
		}

		return nil
	}

	if address == "" {
		return fmt.Errorf("worker.spire: workload_api_address is required when enabled")
	}

	if err := spire.ValidateWorkloadAPIAddress(address); err != nil {
		return fmt.Errorf("worker.spire: %w", err)
	}

	if !WorkerExecutionIdentityEnabled() {
		return fmt.Errorf("worker.spire: enabled requires worker.execution_identity.enabled")
	}

	return nil
}

func validateWorkerSPIRERegistrationConfig() error {
	enabled := WorkerSPIRERegistrationEnabled()
	address := WorkerSPIRERegistrationServerAddress()
	if address != "" {
		if err := spire.ValidateServerAPIAddress(address); err != nil {
			return fmt.Errorf("worker.spire.registration: %w", err)
		}
	}

	parentID := WorkerSPIRERegistrationParentID()
	if parentID != "" {
		if _, err := serviceidentity.NormalizeSPIFFEAllowlist([]string{parentID}); err != nil {
			return fmt.Errorf("worker.spire.registration.parent_id: %w", err)
		}
	}

	selectors, err := WorkerSPIRERegistrationSelectors()
	if err != nil {
		return err
	}

	x509SVIDTTL, err := workerSPIRERegistrationDuration(
		"worker.spire.registration.x509_svid_ttl",
		"worker.spire.registration.x509_svid_ttl",
		time.Duration(MustDefaults().Worker.SPIRE.Registration.X509SVIDTTL),
	)

	if err != nil {
		return err
	}

	if x509SVIDTTL > 0 && x509SVIDTTL < time.Second {
		return fmt.Errorf("worker.spire.registration.x509_svid_ttl must be >= 1s when set")
	}

	minTTL, err := workerSPIRERegistrationDuration(
		"worker.spire.registration.min_ttl",
		"worker.spire.registration.min_ttl",
		time.Duration(MustDefaults().Worker.SPIRE.Registration.MinTTL),
	)

	if err != nil {
		return err
	}

	maxTTL, err := workerSPIRERegistrationDuration(
		"worker.spire.registration.max_ttl",
		"worker.spire.registration.max_ttl",
		time.Duration(MustDefaults().Worker.SPIRE.Registration.MaxTTL),
	)

	if err != nil {
		return err
	}

	if minTTL > 0 && maxTTL > 0 && minTTL > maxTTL {
		return fmt.Errorf("worker.spire.registration.min_ttl must not exceed max_ttl")
	}

	if !enabled {
		return nil
	}

	if !WorkerSPIREEnabled() {
		return fmt.Errorf("worker.spire.registration: enabled requires worker.spire.enabled")
	}

	if address == "" {
		return fmt.Errorf("worker.spire.registration: server_address is required when enabled")
	}

	if parentID == "" {
		return fmt.Errorf("worker.spire.registration: parent_id is required when enabled")
	}

	if len(selectors) == 0 {
		return fmt.Errorf("worker.spire.registration: at least one selector is required when enabled")
	}

	return nil
}

func workerSPIREFetchTimeout() (time.Duration, error) {
	if viper.IsSet("worker.spire.fetch_timeout") {
		raw := strings.TrimSpace(viper.GetString("worker.spire.fetch_timeout"))
		if raw != "" {
			d, err := time.ParseDuration(raw)
			if err != nil {
				return 0, fmt.Errorf("worker.spire: fetch_timeout must be a valid duration (got %q): %w", raw, err)
			}

			if d <= 0 {
				return 0, fmt.Errorf("worker.spire: fetch_timeout must be > 0 (got %s)", d)
			}

			return d, nil
		}

		d := viper.GetDuration("worker.spire.fetch_timeout")
		if d <= 0 {
			return 0, fmt.Errorf("worker.spire: fetch_timeout must be > 0 (got %s)", d)
		}

		return d, nil
	}

	d := time.Duration(MustDefaults().Worker.SPIRE.FetchTimeout)
	if d <= 0 {
		return 0, fmt.Errorf("worker.spire: fetch_timeout must be > 0 (got %s)", d)
	}

	return d, nil
}

func workerSPIRERegistrationDuration(key, label string, fallback time.Duration) (time.Duration, error) {
	if viper.IsSet(key) {
		raw := strings.TrimSpace(viper.GetString(key))
		if raw != "" {
			d, err := time.ParseDuration(raw)
			if err != nil {
				return 0, fmt.Errorf("%s must be a valid duration (got %q): %w", label, raw, err)
			}

			if d < 0 {
				return 0, fmt.Errorf("%s must be >= 0 (got %s)", label, d)
			}

			return d, nil
		}

		d := viper.GetDuration(key)
		if d < 0 {
			return 0, fmt.Errorf("%s must be >= 0 (got %s)", label, d)
		}

		return d, nil
	}

	if fallback < 0 {
		return 0, fmt.Errorf("%s must be >= 0 (got %s)", label, fallback)
	}

	return fallback, nil
}
