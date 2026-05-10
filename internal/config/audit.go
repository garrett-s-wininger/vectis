package config

import (
	"os"
	"strings"

	"github.com/spf13/viper"
)

const (
	envAPIAuditEnabled             = "VECTIS_API_AUDIT_ENABLED"
	envAPIAuditDurabilityOverrides = "VECTIS_API_AUDIT_DURABILITY_OVERRIDES"
)

// APIAuditEnabled reports whether API audit events should be emitted.
func APIAuditEnabled() bool {
	if v := strings.TrimSpace(os.Getenv(envAPIAuditEnabled)); v != "" {
		return parseTruthy(v)
	}

	if viper.IsSet("api.audit.enabled") {
		return viper.GetBool("api.audit.enabled")
	}

	return MustDefaults().API.Audit.Enabled
}

// APIAuditDurabilityOverrides returns comma-separated event=durability policy overrides.
func APIAuditDurabilityOverrides() string {
	if v := os.Getenv(envAPIAuditDurabilityOverrides); v != "" {
		return v
	}

	if viper.IsSet("api.audit.durability_overrides") {
		return viper.GetString("api.audit.durability_overrides")
	}

	return MustDefaults().API.Audit.DurabilityOverrides
}
