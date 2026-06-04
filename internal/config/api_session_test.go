package config

import (
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"

	"vectis/internal/localpki"
)

func TestAPISessionTTL(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	if got := APISessionTTL(); got != 7*24*time.Hour {
		t.Fatalf("APISessionTTL() = %v, want 168h", got)
	}

	viper.Set("api.session.ttl", 2*time.Hour)
	if got := APISessionTTL(); got != 2*time.Hour {
		t.Fatalf("APISessionTTL() viper = %v, want 2h", got)
	}

	t.Setenv(envAPISessionTTL, "30m")
	if got := APISessionTTL(); got != 30*time.Minute {
		t.Fatalf("APISessionTTL() env = %v, want 30m", got)
	}
}

func TestAPISessionIdleTTL(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	if got := APISessionIdleTTL(); got != 24*time.Hour {
		t.Fatalf("APISessionIdleTTL() = %v, want 24h", got)
	}

	viper.Set("api.session.idle_ttl", 2*time.Hour)
	if got := APISessionIdleTTL(); got != 2*time.Hour {
		t.Fatalf("APISessionIdleTTL() viper = %v, want 2h", got)
	}

	t.Setenv(envAPISessionIdleTTL, "30m")
	if got := APISessionIdleTTL(); got != 30*time.Minute {
		t.Fatalf("APISessionIdleTTL() env = %v, want 30m", got)
	}
}

func TestValidateAPISessionConfig(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	if err := ValidateAPISessionConfig(); err != nil {
		t.Fatalf("default config should validate: %v", err)
	}

	t.Setenv(envAPISessionTTL, "0s")
	if err := ValidateAPISessionConfig(); err == nil {
		t.Fatal("expected zero ttl to be invalid")
	}

	t.Setenv(envAPISessionTTL, "not-a-duration")
	if err := ValidateAPISessionConfig(); err == nil {
		t.Fatal("expected invalid duration error")
	}
}

func TestValidateAPISessionConfig_invalidIdleTTL(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	t.Setenv(envAPISessionIdleTTL, "0s")
	if err := ValidateAPISessionConfig(); err == nil {
		t.Fatal("expected zero idle ttl to be invalid")
	}
}

func TestValidateAPISessionConfig_idleTTLGreaterThanTTL(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	t.Setenv(envAPISessionTTL, "1h")
	t.Setenv(envAPISessionIdleTTL, "2h")
	if err := ValidateAPISessionConfig(); err == nil {
		t.Fatal("expected idle ttl greater than ttl to be invalid")
	}
}

func TestValidateAPISessionConfig_invalidCookieSecure(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	t.Setenv(envAPISessionCookieSecure, "tru")
	if err := ValidateAPISessionConfig(); err == nil {
		t.Fatal("expected invalid cookie secure error")
	}
}

func TestValidateAPISessionConfig_requiresSecureCookiesWhenAuthEnabled(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	t.Setenv(envAPIAuthEnabled, "true")
	if err := ValidateAPISessionConfig(); err == nil {
		t.Fatal("expected auth-enabled insecure cookies to be invalid")
	}

	t.Setenv(envAPISessionAllowInsecureCookies, "true")
	if err := ValidateAPISessionConfig(); err != nil {
		t.Fatalf("expected explicit insecure cookie opt-in to validate: %v", err)
	}
}

func TestValidateAPISessionConfig_allowsExplicitSecureCookiesWhenAuthEnabled(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	t.Setenv(envAPIAuthEnabled, "true")
	t.Setenv(envAPISessionCookieSecure, "true")

	if err := ValidateAPISessionConfig(); err != nil {
		t.Fatalf("expected explicit secure cookie setting to validate: %v", err)
	}
}

func TestValidateAPISessionConfig_allowsDirectAPITLSWhenAuthEnabled(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	m, err := localpki.Ensure(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	t.Setenv(envAPIAuthEnabled, "true")
	viper.Set("api.tls.cert_file", m.ServerCert)
	viper.Set("api.tls.key_file", m.ServerKey)
	if err := ValidateAPISessionConfig(); err != nil {
		t.Fatalf("expected direct API TLS to satisfy secure-cookie validation: %v", err)
	}
}

func TestValidateAPISessionConfig_trustedProxyDoesNotSatisfySecureCookieValidation(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	t.Setenv(envAPIAuthEnabled, "true")
	t.Setenv(envAPIClientIPTrustedProxyCIDRs, "10.0.0.0/8")

	err := ValidateAPISessionConfig()
	if err == nil {
		t.Fatal("expected trusted proxy headers alone to be insufficient for secure-cookie validation")
	}

	if !strings.Contains(err.Error(), "trusted proxy") {
		t.Fatalf("error=%q, want trusted proxy guidance", err)
	}
}

func TestAPISessionCookieSecure(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	if got := APISessionCookieSecure(); got {
		t.Fatal("default cookie secure should be false for local defaults")
	}

	viper.Set("api.session.cookie_secure", true)
	if got := APISessionCookieSecure(); !got {
		t.Fatal("viper cookie secure override should be true")
	}

	t.Setenv(envAPISessionCookieSecure, "false")
	if got := APISessionCookieSecure(); got {
		t.Fatal("env cookie secure override should be false")
	}
}

func TestAPISessionAllowInsecureCookies(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	if got := APISessionAllowInsecureCookies(); got {
		t.Fatal("default insecure cookie opt-in should be false")
	}

	viper.Set("api.session.allow_insecure_cookies", true)
	if got := APISessionAllowInsecureCookies(); !got {
		t.Fatal("viper insecure cookie opt-in should be true")
	}

	t.Setenv(envAPISessionAllowInsecureCookies, "false")
	if got := APISessionAllowInsecureCookies(); got {
		t.Fatal("env insecure cookie opt-in override should be false")
	}
}
