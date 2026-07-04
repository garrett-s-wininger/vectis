package config

import (
	"strings"
	"testing"

	"github.com/spf13/viper"
)

func TestWorkerExecutionIdentityDefaultsDisabled(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	if WorkerExecutionIdentityEnabled() {
		t.Fatal("WorkerExecutionIdentityEnabled = true, want false")
	}

	if got := WorkerExecutionIdentityPathTemplate(); got == "" {
		t.Fatal("WorkerExecutionIdentityPathTemplate is empty")
	}

	if err := ValidateWorkerExecutionIdentityConfig(); err != nil {
		t.Fatalf("ValidateWorkerExecutionIdentityConfig disabled defaults: %v", err)
	}
}

func TestValidateWorkerExecutionIdentityRequiresTrustDomainWhenEnabled(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("worker.execution_identity.enabled", true)

	err := ValidateWorkerExecutionIdentityConfig()
	if err == nil || !strings.Contains(err.Error(), "trust_domain") {
		t.Fatalf("ValidateWorkerExecutionIdentityConfig error = %v, want trust_domain", err)
	}
}

func TestValidateWorkerExecutionIdentityAcceptsConfiguredTrustDomain(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("worker.execution_identity.enabled", true)
	viper.Set("worker.execution_identity.trust_domain", "prod.example")

	if err := ValidateWorkerExecutionIdentityConfig(); err != nil {
		t.Fatalf("ValidateWorkerExecutionIdentityConfig: %v", err)
	}
}

func TestValidateWorkerExecutionIdentityRejectsBadTemplate(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("worker.execution_identity.enabled", true)
	viper.Set("worker.execution_identity.trust_domain", "prod.example")
	viper.Set("worker.execution_identity.path_template", "relative/{cell}")

	err := ValidateWorkerExecutionIdentityConfig()
	if err == nil || !strings.Contains(err.Error(), "path template") {
		t.Fatalf("ValidateWorkerExecutionIdentityConfig error = %v, want path template", err)
	}
}
