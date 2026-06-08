package config

import (
	"strings"
	"testing"

	"github.com/spf13/viper"
)

func TestWorkerSPIREDefaultsDisabled(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	if WorkerSPIREEnabled() {
		t.Fatal("WorkerSPIREEnabled = true, want false")
	}

	if WorkerSPIRERequireExecutionSVID() {
		t.Fatal("WorkerSPIRERequireExecutionSVID = true, want false")
	}

	if err := ValidateWorkerSPIREConfig(); err != nil {
		t.Fatalf("ValidateWorkerSPIREConfig disabled defaults: %v", err)
	}
}

func TestValidateWorkerSPIRERequiresAddressWhenEnabled(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("worker.spire.enabled", true)

	err := ValidateWorkerSPIREConfig()
	if err == nil || !strings.Contains(err.Error(), "workload_api_address is required") {
		t.Fatalf("ValidateWorkerSPIREConfig error = %v, want workload_api_address", err)
	}
}

func TestValidateWorkerSPIRERejectsBadAddress(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("worker.spire.enabled", true)
	viper.Set("worker.spire.workload_api_address", "not a workload api address")

	err := ValidateWorkerSPIREConfig()
	if err == nil || !strings.Contains(err.Error(), "invalid workload API address") {
		t.Fatalf("ValidateWorkerSPIREConfig error = %v, want invalid address", err)
	}
}

func TestValidateWorkerSPIREAcceptsAddress(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("worker.spire.enabled", true)
	viper.Set("worker.spire.workload_api_address", "unix:///tmp/spire-agent.sock")

	if err := ValidateWorkerSPIREConfig(); err != nil {
		t.Fatalf("ValidateWorkerSPIREConfig: %v", err)
	}
}

func TestValidateWorkerSPIRERequireExecutionSVIDNeedsExecutionIdentity(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("worker.spire.enabled", true)
	viper.Set("worker.spire.workload_api_address", "unix:///tmp/spire-agent.sock")
	viper.Set("worker.spire.require_execution_svid", true)

	err := ValidateWorkerSPIREConfig()
	if err == nil || !strings.Contains(err.Error(), "worker.execution_identity.enabled") {
		t.Fatalf("ValidateWorkerSPIREConfig error = %v, want execution identity requirement", err)
	}
}

func TestValidateWorkerSPIRERequireExecutionSVIDAcceptsExecutionIdentity(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("worker.execution_identity.enabled", true)
	viper.Set("worker.execution_identity.trust_domain", "prod.example")
	viper.Set("worker.spire.enabled", true)
	viper.Set("worker.spire.workload_api_address", "unix:///tmp/spire-agent.sock")
	viper.Set("worker.spire.require_execution_svid", true)

	if err := ValidateWorkerExecutionIdentityConfig(); err != nil {
		t.Fatalf("ValidateWorkerExecutionIdentityConfig: %v", err)
	}

	if err := ValidateWorkerSPIREConfig(); err != nil {
		t.Fatalf("ValidateWorkerSPIREConfig: %v", err)
	}
}
