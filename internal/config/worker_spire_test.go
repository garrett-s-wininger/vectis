package config

import (
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
)

func TestWorkerSPIREDefaultsDisabled(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	if WorkerSPIREEnabled() {
		t.Fatal("WorkerSPIREEnabled = true, want false")
	}

	if got := WorkerSPIREFetchTimeout(); got <= 0 {
		t.Fatalf("WorkerSPIREFetchTimeout = %v, want > 0", got)
	}

	if WorkerSPIRERegistrationEnabled() {
		t.Fatal("WorkerSPIRERegistrationEnabled = true, want false")
	}

	if got := WorkerSPIRERegistrationServerAddress(); got != "" {
		t.Fatalf("WorkerSPIRERegistrationServerAddress = %q, want empty", got)
	}

	if got, err := WorkerSPIRERegistrationSelectors(); err != nil || len(got) != 0 {
		t.Fatalf("WorkerSPIRERegistrationSelectors = %+v, %v; want empty nil", got, err)
	}

	if err := ValidateWorkerSPIREConfig(); err != nil {
		t.Fatalf("ValidateWorkerSPIREConfig disabled defaults: %v", err)
	}
}

func TestValidateWorkerSPIREFetchTimeoutRejectsInvalidDuration(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("worker.spire.fetch_timeout", "not a duration")

	err := ValidateWorkerSPIREConfig()
	if err == nil || !strings.Contains(err.Error(), "fetch_timeout must be a valid duration") {
		t.Fatalf("ValidateWorkerSPIREConfig error = %v, want invalid fetch_timeout", err)
	}
}

func TestValidateWorkerSPIREFetchTimeoutRejectsNonPositiveDuration(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("worker.spire.fetch_timeout", 0)

	err := ValidateWorkerSPIREConfig()
	if err == nil || !strings.Contains(err.Error(), "fetch_timeout must be > 0") {
		t.Fatalf("ValidateWorkerSPIREConfig error = %v, want non-positive fetch_timeout", err)
	}
}

func TestWorkerSPIREFetchTimeoutAcceptsConfiguredDuration(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("worker.spire.fetch_timeout", 250*time.Millisecond)

	if got := WorkerSPIREFetchTimeout(); got != 250*time.Millisecond {
		t.Fatalf("WorkerSPIREFetchTimeout = %v, want 250ms", got)
	}

	if err := ValidateWorkerSPIREConfig(); err != nil {
		t.Fatalf("ValidateWorkerSPIREConfig: %v", err)
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
	viper.Set("worker.execution_identity.enabled", true)
	viper.Set("worker.execution_identity.trust_domain", "prod.example")
	viper.Set("worker.spire.enabled", true)
	viper.Set("worker.spire.workload_api_address", "unix:///tmp/spire-agent.sock")

	if err := ValidateWorkerExecutionIdentityConfig(); err != nil {
		t.Fatalf("ValidateWorkerExecutionIdentityConfig: %v", err)
	}

	if err := ValidateWorkerSPIREConfig(); err != nil {
		t.Fatalf("ValidateWorkerSPIREConfig: %v", err)
	}
}

func TestValidateWorkerSPIREEnabledNeedsExecutionIdentity(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("worker.spire.enabled", true)
	viper.Set("worker.spire.workload_api_address", "unix:///tmp/spire-agent.sock")

	err := ValidateWorkerSPIREConfig()
	if err == nil || !strings.Contains(err.Error(), "worker.execution_identity.enabled") {
		t.Fatalf("ValidateWorkerSPIREConfig error = %v, want execution identity requirement", err)
	}
}

func TestValidateWorkerSPIRERegistrationRequiresSPIREGate(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("worker.spire.registration.enabled", true)
	viper.Set("worker.spire.registration.server_address", "unix:///run/spire/server.sock")
	viper.Set("worker.spire.registration.parent_id", "spiffe://prod.example/spire/agent/worker")
	viper.Set("worker.spire.registration.selectors", []string{"unix:uid:1000"})

	err := ValidateWorkerSPIREConfig()
	if err == nil || !strings.Contains(err.Error(), "requires worker.spire.enabled") {
		t.Fatalf("ValidateWorkerSPIREConfig error = %v, want worker.spire.enabled requirement", err)
	}
}

func TestValidateWorkerSPIRERegistrationAcceptsConfig(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("worker.execution_identity.enabled", true)
	viper.Set("worker.execution_identity.trust_domain", "prod.example")
	viper.Set("worker.spire.enabled", true)
	viper.Set("worker.spire.workload_api_address", "unix:///run/spire/agent.sock")
	viper.Set("worker.spire.registration.enabled", true)
	viper.Set("worker.spire.registration.server_address", "unix:///run/spire/server.sock")
	viper.Set("worker.spire.registration.parent_id", "spiffe://prod.example/spire/agent/worker")
	viper.Set("worker.spire.registration.selectors", []string{"unix:uid:1000", "k8s:sa:vectis:worker"})
	viper.Set("worker.spire.registration.x509_svid_ttl", "30m")
	viper.Set("worker.spire.registration.min_ttl", "1m")
	viper.Set("worker.spire.registration.max_ttl", "30m")

	if err := ValidateWorkerSPIREConfig(); err != nil {
		t.Fatalf("ValidateWorkerSPIREConfig: %v", err)
	}

	selectors, err := WorkerSPIRERegistrationSelectors()
	if err != nil {
		t.Fatalf("WorkerSPIRERegistrationSelectors: %v", err)
	}

	if len(selectors) != 2 || selectors[0].Type != "k8s" || selectors[1].Type != "unix" {
		t.Fatalf("selectors = %+v, want normalized k8s/unix selectors", selectors)
	}

	if got := WorkerSPIRERegistrationX509SVIDTTL(); got != 30*time.Minute {
		t.Fatalf("WorkerSPIRERegistrationX509SVIDTTL = %v, want 30m", got)
	}

	if got := WorkerSPIRERegistrationMinTTL(); got != time.Minute {
		t.Fatalf("WorkerSPIRERegistrationMinTTL = %v, want 1m", got)
	}

	if got := WorkerSPIRERegistrationMaxTTL(); got != 30*time.Minute {
		t.Fatalf("WorkerSPIRERegistrationMaxTTL = %v, want 30m", got)
	}
}

func TestValidateWorkerSPIRERegistrationRejectsMissingEnabledFields(t *testing.T) {
	tests := []struct {
		name    string
		setup   func()
		wantErr string
	}{
		{
			name: "server address",
			setup: func() {
				viper.Set("worker.spire.registration.parent_id", "spiffe://prod.example/spire/agent/worker")
				viper.Set("worker.spire.registration.selectors", []string{"unix:uid:1000"})
			},
			wantErr: "server_address is required",
		},
		{
			name: "parent id",
			setup: func() {
				viper.Set("worker.spire.registration.server_address", "unix:///run/spire/server.sock")
				viper.Set("worker.spire.registration.selectors", []string{"unix:uid:1000"})
			},
			wantErr: "parent_id is required",
		},
		{
			name: "selectors",
			setup: func() {
				viper.Set("worker.spire.registration.server_address", "unix:///run/spire/server.sock")
				viper.Set("worker.spire.registration.parent_id", "spiffe://prod.example/spire/agent/worker")
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
			viper.Set("worker.spire.enabled", true)
			viper.Set("worker.spire.workload_api_address", "unix:///run/spire/agent.sock")
			viper.Set("worker.spire.registration.enabled", true)
			tt.setup()

			err := ValidateWorkerSPIREConfig()
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("ValidateWorkerSPIREConfig error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestValidateWorkerSPIRERegistrationRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name    string
		setup   func()
		wantErr string
	}{
		{
			name: "server address",
			setup: func() {
				viper.Set("worker.spire.registration.server_address", "tcp://127.0.0.1:8081")
			},
			wantErr: "unix://",
		},
		{
			name: "parent id",
			setup: func() {
				viper.Set("worker.spire.registration.parent_id", "not-spiffe")
			},
			wantErr: "spiffe://",
		},
		{
			name: "selector",
			setup: func() {
				viper.Set("worker.spire.registration.selectors", []string{"missing-colon"})
			},
			wantErr: "type:value",
		},
		{
			name: "x509 ttl",
			setup: func() {
				viper.Set("worker.spire.registration.x509_svid_ttl", time.Millisecond)
			},
			wantErr: "x509_svid_ttl must be >= 1s",
		},
		{
			name: "min above max",
			setup: func() {
				viper.Set("worker.spire.registration.min_ttl", "10m")
				viper.Set("worker.spire.registration.max_ttl", "1m")
			},
			wantErr: "min_ttl must not exceed max_ttl",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			viper.Reset()
			t.Cleanup(viper.Reset)
			tt.setup()

			err := ValidateWorkerSPIREConfig()
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("ValidateWorkerSPIREConfig error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}
