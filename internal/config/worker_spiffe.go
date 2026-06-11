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
	_ = viper.BindEnv("worker.spiffe.enabled", "VECTIS_WORKER_SPIFFE_ENABLED")
	_ = viper.BindEnv("worker.spiffe.workload_api_address", "VECTIS_WORKER_SPIFFE_WORKLOAD_API_ADDRESS")
	_ = viper.BindEnv("worker.spiffe.fetch_timeout", "VECTIS_WORKER_SPIFFE_FETCH_TIMEOUT")
	_ = viper.BindEnv("worker.spiffe.registration.enabled", "VECTIS_WORKER_SPIFFE_REGISTRATION_ENABLED")
	_ = viper.BindEnv("worker.spiffe.registration.server_address", "VECTIS_WORKER_SPIFFE_REGISTRATION_SERVER_ADDRESS")
	_ = viper.BindEnv("worker.spiffe.registration.parent_id", "VECTIS_WORKER_SPIFFE_REGISTRATION_PARENT_ID")
	_ = viper.BindEnv("worker.spiffe.registration.selectors", "VECTIS_WORKER_SPIFFE_REGISTRATION_SELECTORS")
	_ = viper.BindEnv("worker.spiffe.registration.x509_svid_ttl", "VECTIS_WORKER_SPIFFE_REGISTRATION_X509_SVID_TTL")
	_ = viper.BindEnv("worker.spiffe.registration.min_ttl", "VECTIS_WORKER_SPIFFE_REGISTRATION_MIN_TTL")
	_ = viper.BindEnv("worker.spiffe.registration.max_ttl", "VECTIS_WORKER_SPIFFE_REGISTRATION_MAX_TTL")
}

func WorkerSPIFFEEnabled() bool {
	if viper.IsSet("worker.spiffe.enabled") {
		return viper.GetBool("worker.spiffe.enabled")
	}

	return MustDefaults().Worker.SPIFFE.Enabled
}

func WorkerSPIFFEWorkloadAPIAddress() string {
	if v := strings.TrimSpace(viper.GetString("worker.spiffe.workload_api_address")); v != "" {
		return v
	}

	return strings.TrimSpace(MustDefaults().Worker.SPIFFE.WorkloadAPIAddress)
}

func WorkerSPIFFEFetchTimeout() time.Duration {
	d, err := workerSPIFFEFetchTimeout()
	if err != nil {
		return 0
	}

	return d
}

func WorkerSPIFFERegistrationEnabled() bool {
	if viper.IsSet("worker.spiffe.registration.enabled") {
		return viper.GetBool("worker.spiffe.registration.enabled")
	}

	return MustDefaults().Worker.SPIFFE.Registration.Enabled
}

func WorkerSPIFFERegistrationServerAddress() string {
	if v := strings.TrimSpace(viper.GetString("worker.spiffe.registration.server_address")); v != "" {
		return v
	}

	return strings.TrimSpace(MustDefaults().Worker.SPIFFE.Registration.ServerAddress)
}

func WorkerSPIFFERegistrationParentID() string {
	if v := strings.TrimSpace(viper.GetString("worker.spiffe.registration.parent_id")); v != "" {
		return v
	}

	return strings.TrimSpace(MustDefaults().Worker.SPIFFE.Registration.ParentID)
}

func WorkerSPIFFERegistrationSelectorSpecs() []string {
	return coalesceStringSlices(
		stringSliceFromViper("worker.spiffe.registration.selectors"),
		MustDefaults().Worker.SPIFFE.Registration.Selectors,
	)
}

func WorkerSPIFFERegistrationSelectors() ([]spire.Selector, error) {
	specs := WorkerSPIFFERegistrationSelectorSpecs()
	if len(specs) == 0 {
		return nil, nil
	}

	selectors := make([]spire.Selector, 0, len(specs))
	for _, spec := range specs {
		selector, err := spire.ParseSelector(spec)
		if err != nil {
			return nil, fmt.Errorf("worker.spiffe.registration.selectors: %w", err)
		}

		selectors = append(selectors, selector)
	}

	selectors, err := spire.NormalizeSelectors(selectors)
	if err != nil {
		return nil, fmt.Errorf("worker.spiffe.registration.selectors: %w", err)
	}

	return selectors, nil
}

func WorkerSPIFFERegistrationX509SVIDTTL() time.Duration {
	d, err := workerSPIFFERegistrationDuration(
		"worker.spiffe.registration.x509_svid_ttl",
		"worker.spiffe.registration.x509_svid_ttl",
		time.Duration(MustDefaults().Worker.SPIFFE.Registration.X509SVIDTTL),
	)

	if err != nil {
		return 0
	}

	return d
}

func WorkerSPIFFERegistrationMinTTL() time.Duration {
	d, err := workerSPIFFERegistrationDuration(
		"worker.spiffe.registration.min_ttl",
		"worker.spiffe.registration.min_ttl",
		time.Duration(MustDefaults().Worker.SPIFFE.Registration.MinTTL),
	)

	if err != nil {
		return 0
	}

	return d
}

func WorkerSPIFFERegistrationMaxTTL() time.Duration {
	d, err := workerSPIFFERegistrationDuration(
		"worker.spiffe.registration.max_ttl",
		"worker.spiffe.registration.max_ttl",
		time.Duration(MustDefaults().Worker.SPIFFE.Registration.MaxTTL),
	)

	if err != nil {
		return 0
	}

	return d
}

func ValidateWorkerSPIFFEConfig() error {
	address := WorkerSPIFFEWorkloadAPIAddress()
	if _, err := workerSPIFFEFetchTimeout(); err != nil {
		return err
	}

	if err := validateWorkerSPIFFERegistrationConfig(); err != nil {
		return err
	}

	if !WorkerSPIFFEEnabled() {
		if address != "" {
			if err := spire.ValidateWorkloadAPIAddress(address); err != nil {
				return fmt.Errorf("worker.spiffe: %w", err)
			}
		}

		return nil
	}

	if address == "" {
		return fmt.Errorf("worker.spiffe: workload_api_address is required when enabled")
	}

	if err := spire.ValidateWorkloadAPIAddress(address); err != nil {
		return fmt.Errorf("worker.spiffe: %w", err)
	}

	if !WorkerExecutionIdentityEnabled() {
		return fmt.Errorf("worker.spiffe: enabled requires worker.execution_identity.enabled")
	}

	return nil
}

func validateWorkerSPIFFERegistrationConfig() error {
	enabled := WorkerSPIFFERegistrationEnabled()
	address := WorkerSPIFFERegistrationServerAddress()
	if address != "" {
		if err := spire.ValidateServerAPIAddress(address); err != nil {
			return fmt.Errorf("worker.spiffe.registration: %w", err)
		}
	}

	parentID := WorkerSPIFFERegistrationParentID()
	if parentID != "" {
		if _, err := serviceidentity.NormalizeSPIFFEAllowlist([]string{parentID}); err != nil {
			return fmt.Errorf("worker.spiffe.registration.parent_id: %w", err)
		}
	}

	selectors, err := WorkerSPIFFERegistrationSelectors()
	if err != nil {
		return err
	}

	x509SVIDTTL, err := workerSPIFFERegistrationDuration(
		"worker.spiffe.registration.x509_svid_ttl",
		"worker.spiffe.registration.x509_svid_ttl",
		time.Duration(MustDefaults().Worker.SPIFFE.Registration.X509SVIDTTL),
	)

	if err != nil {
		return err
	}

	if x509SVIDTTL > 0 && x509SVIDTTL < time.Second {
		return fmt.Errorf("worker.spiffe.registration.x509_svid_ttl must be >= 1s when set")
	}

	minTTL, err := workerSPIFFERegistrationDuration(
		"worker.spiffe.registration.min_ttl",
		"worker.spiffe.registration.min_ttl",
		time.Duration(MustDefaults().Worker.SPIFFE.Registration.MinTTL),
	)

	if err != nil {
		return err
	}

	maxTTL, err := workerSPIFFERegistrationDuration(
		"worker.spiffe.registration.max_ttl",
		"worker.spiffe.registration.max_ttl",
		time.Duration(MustDefaults().Worker.SPIFFE.Registration.MaxTTL),
	)

	if err != nil {
		return err
	}

	if minTTL > 0 && maxTTL > 0 && minTTL > maxTTL {
		return fmt.Errorf("worker.spiffe.registration.min_ttl must not exceed max_ttl")
	}

	if !enabled {
		return nil
	}

	if !WorkerSPIFFEEnabled() {
		return fmt.Errorf("worker.spiffe.registration: enabled requires worker.spiffe.enabled")
	}

	if address == "" {
		return fmt.Errorf("worker.spiffe.registration: server_address is required when enabled")
	}

	if parentID == "" {
		return fmt.Errorf("worker.spiffe.registration: parent_id is required when enabled")
	}

	if len(selectors) == 0 {
		return fmt.Errorf("worker.spiffe.registration: at least one selector is required when enabled")
	}

	return nil
}

func workerSPIFFEFetchTimeout() (time.Duration, error) {
	if viper.IsSet("worker.spiffe.fetch_timeout") {
		raw := strings.TrimSpace(viper.GetString("worker.spiffe.fetch_timeout"))
		if raw != "" {
			d, err := time.ParseDuration(raw)
			if err != nil {
				return 0, fmt.Errorf("worker.spiffe: fetch_timeout must be a valid duration (got %q): %w", raw, err)
			}

			if d <= 0 {
				return 0, fmt.Errorf("worker.spiffe: fetch_timeout must be > 0 (got %s)", d)
			}

			return d, nil
		}

		d := viper.GetDuration("worker.spiffe.fetch_timeout")
		if d <= 0 {
			return 0, fmt.Errorf("worker.spiffe: fetch_timeout must be > 0 (got %s)", d)
		}

		return d, nil
	}

	d := time.Duration(MustDefaults().Worker.SPIFFE.FetchTimeout)
	if d <= 0 {
		return 0, fmt.Errorf("worker.spiffe: fetch_timeout must be > 0 (got %s)", d)
	}

	return d, nil
}

func workerSPIFFERegistrationDuration(key, label string, fallback time.Duration) (time.Duration, error) {
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
