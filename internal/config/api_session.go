package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/viper"
)

const envAPISessionTTL = "VECTIS_API_SESSION_TTL"
const envAPISessionIdleTTL = "VECTIS_API_SESSION_IDLE_TTL"
const envAPISessionCookieSecure = "VECTIS_API_SESSION_COOKIE_SECURE"
const envAPISessionAllowInsecureCookies = "VECTIS_API_SESSION_ALLOW_INSECURE_COOKIES"

func APISessionTTL() time.Duration {
	if v := strings.TrimSpace(os.Getenv(envAPISessionTTL)); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}

	if viper.IsSet("api.session.ttl") {
		return viper.GetDuration("api.session.ttl")
	}

	d := time.Duration(MustDefaults().API.Session.TTL)
	if d > 0 {
		return d
	}

	return 7 * 24 * time.Hour
}

func ValidateAPISessionConfig() error {
	if v := strings.TrimSpace(os.Getenv(envAPISessionCookieSecure)); v != "" {
		if _, err := strconv.ParseBool(v); err != nil {
			return fmt.Errorf("api.session.cookie_secure must be a boolean (got %q): %w", v, err)
		}
	}

	if v := strings.TrimSpace(os.Getenv(envAPISessionAllowInsecureCookies)); v != "" {
		if _, err := strconv.ParseBool(v); err != nil {
			return fmt.Errorf("api.session.allow_insecure_cookies must be a boolean (got %q): %w", v, err)
		}
	}

	ttl, err := validateAPISessionDuration(envAPISessionTTL, "api.session.ttl", APISessionTTL())
	if err != nil {
		return err
	}

	idleTTL, err := validateAPISessionDuration(envAPISessionIdleTTL, "api.session.idle_ttl", APISessionIdleTTL())
	if err != nil {
		return err
	}

	if idleTTL > ttl {
		return fmt.Errorf("api.session.idle_ttl must be <= api.session.ttl (got %s > %s)", idleTTL, ttl)
	}

	if APIAuthEnabled() && !APISessionCookieSecure() && !APIHTTPSEnabled() && !APISessionAllowInsecureCookies() {
		return fmt.Errorf("api.session.cookie_secure must be true when api.auth.enabled is true for HTTPS deployments, or set api.session.allow_insecure_cookies=true for local HTTP-only development; internal gRPC and metrics TLS do not secure API cookies")
	}

	return nil
}

func validateAPISessionDuration(envName, key string, fallback time.Duration) (time.Duration, error) {
	if v := strings.TrimSpace(os.Getenv(envName)); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return 0, fmt.Errorf("%s must be a valid duration (got %q): %w", key, v, err)
		}

		if d <= 0 {
			return 0, fmt.Errorf("%s must be > 0 (got %q)", key, v)
		}

		return d, nil
	}

	if fallback <= 0 {
		return 0, fmt.Errorf("%s must be > 0 (got %s)", key, fallback)
	}

	return fallback, nil
}

func APISessionIdleTTL() time.Duration {
	if v := strings.TrimSpace(os.Getenv(envAPISessionIdleTTL)); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}

	if viper.IsSet("api.session.idle_ttl") {
		return viper.GetDuration("api.session.idle_ttl")
	}

	d := time.Duration(MustDefaults().API.Session.IdleTTL)
	if d > 0 {
		return d
	}

	return 24 * time.Hour
}

func APISessionCookieSecure() bool {
	if v := strings.TrimSpace(os.Getenv(envAPISessionCookieSecure)); v != "" {
		parsed, err := strconv.ParseBool(v)
		return err == nil && parsed
	}

	if viper.IsSet("api.session.cookie_secure") {
		return viper.GetBool("api.session.cookie_secure")
	}

	return MustDefaults().API.Session.CookieSecure
}

func APISessionAllowInsecureCookies() bool {
	if v := strings.TrimSpace(os.Getenv(envAPISessionAllowInsecureCookies)); v != "" {
		parsed, err := strconv.ParseBool(v)
		return err == nil && parsed
	}

	if viper.IsSet("api.session.allow_insecure_cookies") {
		return viper.GetBool("api.session.allow_insecure_cookies")
	}

	return MustDefaults().API.Session.AllowInsecureCookies
}
