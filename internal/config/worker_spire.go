package config

import (
	"fmt"
	"strings"
	"time"

	"vectis/internal/spire"

	"github.com/spf13/viper"
)

func init() {
	_ = viper.BindEnv("worker.spire.enabled", "VECTIS_WORKER_SPIRE_ENABLED")
	_ = viper.BindEnv("worker.spire.workload_api_address", "VECTIS_WORKER_SPIRE_WORKLOAD_API_ADDRESS")
	_ = viper.BindEnv("worker.spire.require_execution_svid", "VECTIS_WORKER_SPIRE_REQUIRE_EXECUTION_SVID")
	_ = viper.BindEnv("worker.spire.fetch_timeout", "VECTIS_WORKER_SPIRE_FETCH_TIMEOUT")
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

func WorkerSPIRERequireExecutionSVID() bool {
	if viper.IsSet("worker.spire.require_execution_svid") {
		return viper.GetBool("worker.spire.require_execution_svid")
	}

	return MustDefaults().Worker.SPIRE.RequireExecutionSVID
}

func WorkerSPIREFetchTimeout() time.Duration {
	d, err := workerSPIREFetchTimeout()
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

	if !WorkerSPIREEnabled() {
		if WorkerSPIRERequireExecutionSVID() {
			return fmt.Errorf("worker.spire: require_execution_svid requires worker.spire.enabled")
		}

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

	if WorkerSPIRERequireExecutionSVID() && !WorkerExecutionIdentityEnabled() {
		return fmt.Errorf("worker.spire: require_execution_svid requires worker.execution_identity.enabled")
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
