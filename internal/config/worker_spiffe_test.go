package config

import (
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
)

func TestWorkerSPIFFEDefaultsDisabled(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	if WorkerSPIFFEEnabled() {
		t.Fatal("WorkerSPIFFEEnabled = true, want false")
	}

	if got := WorkerSPIFFEFetchTimeout(); got <= 0 {
		t.Fatalf("WorkerSPIFFEFetchTimeout = %v, want > 0", got)
	}

	if WorkerSPIFFERegistrationEnabled() {
		t.Fatal("WorkerSPIFFERegistrationEnabled = true, want false")
	}

	if got := WorkerSPIFFERegistrationServerAddress(); got != "" {
		t.Fatalf("WorkerSPIFFERegistrationServerAddress = %q, want empty", got)
	}

	if got, err := WorkerSPIFFERegistrationSelectors(); err != nil || len(got) != 0 {
		t.Fatalf("WorkerSPIFFERegistrationSelectors = %+v, %v; want empty nil", got, err)
	}

	if err := ValidateWorkerSPIFFEConfig(); err != nil {
		t.Fatalf("ValidateWorkerSPIFFEConfig disabled defaults: %v", err)
	}
}

func TestValidateWorkerSPIFFEFetchTimeoutRejectsInvalidDuration(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("worker.spiffe.fetch_timeout", "not a duration")

	err := ValidateWorkerSPIFFEConfig()
	if err == nil || !strings.Contains(err.Error(), "fetch_timeout must be a valid duration") {
		t.Fatalf("ValidateWorkerSPIFFEConfig error = %v, want invalid fetch_timeout", err)
	}
}

func TestValidateWorkerSPIFFEFetchTimeoutRejectsNonPositiveDuration(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("worker.spiffe.fetch_timeout", 0)

	err := ValidateWorkerSPIFFEConfig()
	if err == nil || !strings.Contains(err.Error(), "fetch_timeout must be > 0") {
		t.Fatalf("ValidateWorkerSPIFFEConfig error = %v, want non-positive fetch_timeout", err)
	}
}

func TestWorkerSPIFFEFetchTimeoutAcceptsConfiguredDuration(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("worker.spiffe.fetch_timeout", 250*time.Millisecond)

	if got := WorkerSPIFFEFetchTimeout(); got != 250*time.Millisecond {
		t.Fatalf("WorkerSPIFFEFetchTimeout = %v, want 250ms", got)
	}

	if err := ValidateWorkerSPIFFEConfig(); err != nil {
		t.Fatalf("ValidateWorkerSPIFFEConfig: %v", err)
	}
}

func TestValidateWorkerSPIFFERequiresAddressWhenEnabled(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("worker.spiffe.enabled", true)

	err := ValidateWorkerSPIFFEConfig()
	if err == nil || !strings.Contains(err.Error(), "workload_api_address is required") {
		t.Fatalf("ValidateWorkerSPIFFEConfig error = %v, want workload_api_address", err)
	}
}

func TestValidateWorkerSPIFFERejectsBadAddress(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("worker.spiffe.enabled", true)
	viper.Set("worker.spiffe.workload_api_address", "not a workload api address")

	err := ValidateWorkerSPIFFEConfig()
	if err == nil || !strings.Contains(err.Error(), "invalid workload API address") {
		t.Fatalf("ValidateWorkerSPIFFEConfig error = %v, want invalid address", err)
	}
}

func TestValidateWorkerSPIFFEAcceptsAddress(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("worker.execution_identity.enabled", true)
	viper.Set("worker.execution_identity.trust_domain", "prod.example")
	viper.Set("worker.spiffe.enabled", true)
	viper.Set("worker.spiffe.workload_api_address", "unix:///tmp/spiffe-workload.sock")

	if err := ValidateWorkerExecutionIdentityConfig(); err != nil {
		t.Fatalf("ValidateWorkerExecutionIdentityConfig: %v", err)
	}

	if err := ValidateWorkerSPIFFEConfig(); err != nil {
		t.Fatalf("ValidateWorkerSPIFFEConfig: %v", err)
	}
}

func TestValidateWorkerSPIFFEEnabledNeedsExecutionIdentity(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("worker.spiffe.enabled", true)
	viper.Set("worker.spiffe.workload_api_address", "unix:///tmp/spiffe-workload.sock")

	err := ValidateWorkerSPIFFEConfig()
	if err == nil || !strings.Contains(err.Error(), "worker.execution_identity.enabled") {
		t.Fatalf("ValidateWorkerSPIFFEConfig error = %v, want execution identity requirement", err)
	}
}

func TestValidateWorkerSPIFFERegistrationRequiresSPIFFEGate(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("worker.spiffe.registration.enabled", true)
	viper.Set("worker.spiffe.registration.server_address", "unix:///run/vectis/spiffe/registration.sock")
	viper.Set("worker.spiffe.registration.parent_id", "spiffe://prod.example/vectis-spiffe/agent/worker")
	viper.Set("worker.spiffe.registration.selectors", []string{"unix:uid:1000"})

	err := ValidateWorkerSPIFFEConfig()
	if err == nil || !strings.Contains(err.Error(), "requires worker.spiffe.enabled") {
		t.Fatalf("ValidateWorkerSPIFFEConfig error = %v, want worker.spiffe.enabled requirement", err)
	}
}

func TestValidateWorkerSPIFFERegistrationAcceptsConfig(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("worker.execution_identity.enabled", true)
	viper.Set("worker.execution_identity.trust_domain", "prod.example")
	viper.Set("worker.spiffe.enabled", true)
	viper.Set("worker.spiffe.workload_api_address", "unix:///run/vectis/spiffe/workload.sock")
	viper.Set("worker.spiffe.registration.enabled", true)
	viper.Set("worker.spiffe.registration.server_address", "unix:///run/vectis/spiffe/registration.sock")
	viper.Set("worker.spiffe.registration.parent_id", "spiffe://prod.example/vectis-spiffe/agent/worker")
	viper.Set("worker.spiffe.registration.selectors", []string{"unix:uid:1000", "k8s:sa:vectis:worker"})
	viper.Set("worker.spiffe.registration.x509_svid_ttl", "30m")
	viper.Set("worker.spiffe.registration.min_ttl", "1m")
	viper.Set("worker.spiffe.registration.max_ttl", "30m")

	if err := ValidateWorkerSPIFFEConfig(); err != nil {
		t.Fatalf("ValidateWorkerSPIFFEConfig: %v", err)
	}

	selectors, err := WorkerSPIFFERegistrationSelectors()
	if err != nil {
		t.Fatalf("WorkerSPIFFERegistrationSelectors: %v", err)
	}

	if len(selectors) != 2 || selectors[0].Type != "k8s" || selectors[1].Type != "unix" {
		t.Fatalf("selectors = %+v, want normalized k8s/unix selectors", selectors)
	}

	if got := WorkerSPIFFERegistrationX509SVIDTTL(); got != 30*time.Minute {
		t.Fatalf("WorkerSPIFFERegistrationX509SVIDTTL = %v, want 30m", got)
	}

	if got := WorkerSPIFFERegistrationMinTTL(); got != time.Minute {
		t.Fatalf("WorkerSPIFFERegistrationMinTTL = %v, want 1m", got)
	}

	if got := WorkerSPIFFERegistrationMaxTTL(); got != 30*time.Minute {
		t.Fatalf("WorkerSPIFFERegistrationMaxTTL = %v, want 30m", got)
	}
}

func TestValidateWorkerSPIFFERegistrationRejectsMissingEnabledFields(t *testing.T) {
	tests := []struct {
		name    string
		setup   func()
		wantErr string
	}{
		{
			name: "server address",
			setup: func() {
				viper.Set("worker.spiffe.registration.parent_id", "spiffe://prod.example/vectis-spiffe/agent/worker")
				viper.Set("worker.spiffe.registration.selectors", []string{"unix:uid:1000"})
			},
			wantErr: "server_address is required",
		},
		{
			name: "parent id",
			setup: func() {
				viper.Set("worker.spiffe.registration.server_address", "unix:///run/vectis/spiffe/registration.sock")
				viper.Set("worker.spiffe.registration.selectors", []string{"unix:uid:1000"})
			},
			wantErr: "parent_id is required",
		},
		{
			name: "selectors",
			setup: func() {
				viper.Set("worker.spiffe.registration.server_address", "unix:///run/vectis/spiffe/registration.sock")
				viper.Set("worker.spiffe.registration.parent_id", "spiffe://prod.example/vectis-spiffe/agent/worker")
			},
			wantErr: "selector",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			viper.Reset()
			t.Cleanup(viper.Reset)
			viper.Set("worker.execution_identity.enabled", true)
			viper.Set("worker.execution_identity.trust_domain", "prod.example")
			viper.Set("worker.spiffe.enabled", true)
			viper.Set("worker.spiffe.workload_api_address", "unix:///run/vectis/spiffe/workload.sock")
			viper.Set("worker.spiffe.registration.enabled", true)
			tt.setup()

			err := ValidateWorkerSPIFFEConfig()
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("ValidateWorkerSPIFFEConfig error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestValidateWorkerSPIFFERegistrationRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name    string
		setup   func()
		wantErr string
	}{
		{
			name: "server address",
			setup: func() {
				viper.Set("worker.spiffe.registration.server_address", "tcp://127.0.0.1:8081")
			},
			wantErr: "unix://",
		},
		{
			name: "parent id",
			setup: func() {
				viper.Set("worker.spiffe.registration.parent_id", "not-spiffe")
			},
			wantErr: "spiffe://",
		},
		{
			name: "selector",
			setup: func() {
				viper.Set("worker.spiffe.registration.selectors", []string{"missing-colon"})
			},
			wantErr: "type:value",
		},
		{
			name: "x509 ttl",
			setup: func() {
				viper.Set("worker.spiffe.registration.x509_svid_ttl", time.Millisecond)
			},
			wantErr: "x509_svid_ttl must be >= 1s",
		},
		{
			name: "min above max",
			setup: func() {
				viper.Set("worker.spiffe.registration.min_ttl", "10m")
				viper.Set("worker.spiffe.registration.max_ttl", "1m")
			},
			wantErr: "min_ttl must not exceed max_ttl",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			viper.Reset()
			t.Cleanup(viper.Reset)
			tt.setup()

			err := ValidateWorkerSPIFFEConfig()
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("ValidateWorkerSPIFFEConfig error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}
