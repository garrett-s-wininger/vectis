package config

import (
	"fmt"
	"strings"

	"vectis/internal/spire"

	"github.com/spf13/viper"
)

func init() {
	_ = viper.BindEnv("worker.spire.enabled", "VECTIS_WORKER_SPIRE_ENABLED")
	_ = viper.BindEnv("worker.spire.workload_api_address", "VECTIS_WORKER_SPIRE_WORKLOAD_API_ADDRESS")
	_ = viper.BindEnv("worker.spire.require_execution_svid", "VECTIS_WORKER_SPIRE_REQUIRE_EXECUTION_SVID")
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

func ValidateWorkerSPIREConfig() error {
	address := WorkerSPIREWorkloadAPIAddress()

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
