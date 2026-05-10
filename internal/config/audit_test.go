package config

import (
	"testing"

	"github.com/spf13/viper"
)

func TestAPIAudit_Defaults(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv(envAPIAuditEnabled, "")
	t.Setenv(envAPIAuditDurabilityOverrides, "")

	if !APIAuditEnabled() {
		t.Fatal("expected API audit enabled by default")
	}

	if got := APIAuditDurabilityOverrides(); got != "" {
		t.Fatalf("expected empty overrides, got %q", got)
	}
}

func TestAPIAudit_EnvOverrides(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv(envAPIAuditEnabled, "false")
	t.Setenv(envAPIAuditDurabilityOverrides, "auth.success=disabled")

	if APIAuditEnabled() {
		t.Fatal("expected env to disable API audit")
	}

	if got := APIAuditDurabilityOverrides(); got != "auth.success=disabled" {
		t.Fatalf("overrides: got %q", got)
	}
}
