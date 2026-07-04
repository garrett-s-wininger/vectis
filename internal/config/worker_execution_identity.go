package config

import (
	"fmt"
	"strings"

	"vectis/internal/workloadidentity"

	"github.com/spf13/viper"
)

func init() {
	_ = viper.BindEnv("worker.execution_identity.enabled", "VECTIS_WORKER_EXECUTION_IDENTITY_ENABLED")
	_ = viper.BindEnv("worker.execution_identity.trust_domain", "VECTIS_WORKER_EXECUTION_IDENTITY_TRUST_DOMAIN")
	_ = viper.BindEnv("worker.execution_identity.path_template", "VECTIS_WORKER_EXECUTION_IDENTITY_PATH_TEMPLATE")
}

func WorkerExecutionIdentityEnabled() bool {
	if viper.IsSet("worker.execution_identity.enabled") {
		return viper.GetBool("worker.execution_identity.enabled")
	}

	return MustDefaults().Worker.ExecutionIdentity.Enabled
}

func WorkerExecutionIdentityTrustDomain() string {
	if v := strings.TrimSpace(viper.GetString("worker.execution_identity.trust_domain")); v != "" {
		return v
	}

	return strings.TrimSpace(MustDefaults().Worker.ExecutionIdentity.TrustDomain)
}

func WorkerExecutionIdentityPathTemplate() string {
	if v := strings.TrimSpace(viper.GetString("worker.execution_identity.path_template")); v != "" {
		return v
	}

	if v := strings.TrimSpace(MustDefaults().Worker.ExecutionIdentity.PathTemplate); v != "" {
		return v
	}

	return workloadidentity.DefaultSPIFFEPathTemplate
}

func ValidateWorkerExecutionIdentityConfig() error {
	template := WorkerExecutionIdentityPathTemplate()
	if template == "" {
		return fmt.Errorf("worker.execution_identity: path_template is required")
	}

	sample := workloadidentity.Execution{
		CellID:            "local",
		NamespacePath:     "/",
		JobID:             "job",
		RunID:             "run",
		RunIndex:          1,
		SegmentID:         "segment",
		ExecutionID:       "execution",
		Attempt:           1,
		DefinitionVersion: 1,
		DefinitionHash:    "sha256:sample",
	}

	trustDomain := WorkerExecutionIdentityTrustDomain()
	if !WorkerExecutionIdentityEnabled() {
		if trustDomain == "" {
			trustDomain = "example.invalid"
		}

		_, err := workloadidentity.SPIFFEID(trustDomain, template, sample)
		if err != nil {
			return fmt.Errorf("worker.execution_identity: %w", err)
		}

		return nil
	}

	if trustDomain == "" {
		return fmt.Errorf("worker.execution_identity: trust_domain is required when enabled")
	}

	if _, err := workloadidentity.SPIFFEID(trustDomain, template, sample); err != nil {
		return fmt.Errorf("worker.execution_identity: %w", err)
	}

	return nil
}
