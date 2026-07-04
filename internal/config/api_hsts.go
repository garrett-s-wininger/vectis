package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/viper"
)

const (
	envAPIHSTSMaxAgeSeconds     = "VECTIS_API_HSTS_MAX_AGE_SECONDS"
	envAPIHSTSIncludeSubDomains = "VECTIS_API_HSTS_INCLUDE_SUBDOMAINS"
	envAPIHSTSPreload           = "VECTIS_API_HSTS_PRELOAD"

	apiHSTSPreloadMinMaxAgeSeconds = 31536000
)

func init() {
	_ = viper.BindEnv("api.hsts.max_age_seconds", envAPIHSTSMaxAgeSeconds, "VECTIS_API_SERVER_HSTS_MAX_AGE_SECONDS")
	_ = viper.BindEnv("api.hsts.include_subdomains", envAPIHSTSIncludeSubDomains, "VECTIS_API_SERVER_HSTS_INCLUDE_SUBDOMAINS")
	_ = viper.BindEnv("api.hsts.preload", envAPIHSTSPreload, "VECTIS_API_SERVER_HSTS_PRELOAD")
}

// APIHSTSMaxAgeSeconds returns the Strict-Transport-Security max-age value.
func APIHSTSMaxAgeSeconds() int {
	if v := strings.TrimSpace(os.Getenv(envAPIHSTSMaxAgeSeconds)); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			return parsed
		}
	}

	if viper.IsSet("api.hsts.max_age_seconds") {
		return viper.GetInt("api.hsts.max_age_seconds")
	}

	return MustDefaults().API.HSTS.MaxAgeSeconds
}

func APIHSTSIncludeSubDomains() bool {
	if v := strings.TrimSpace(os.Getenv(envAPIHSTSIncludeSubDomains)); v != "" {
		parsed, err := strconv.ParseBool(v)
		return err == nil && parsed
	}

	if viper.IsSet("api.hsts.include_subdomains") {
		return viper.GetBool("api.hsts.include_subdomains")
	}

	return MustDefaults().API.HSTS.IncludeSubDomains
}

func APIHSTSPreload() bool {
	if v := strings.TrimSpace(os.Getenv(envAPIHSTSPreload)); v != "" {
		parsed, err := strconv.ParseBool(v)
		return err == nil && parsed
	}

	if viper.IsSet("api.hsts.preload") {
		return viper.GetBool("api.hsts.preload")
	}

	return MustDefaults().API.HSTS.Preload
}

func APIHSTSHeaderValue() string {
	parts := []string{fmt.Sprintf("max-age=%d", APIHSTSMaxAgeSeconds())}
	if APIHSTSIncludeSubDomains() {
		parts = append(parts, "includeSubDomains")
	}

	if APIHSTSPreload() {
		parts = append(parts, "preload")
	}

	return strings.Join(parts, "; ")
}

func ValidateAPIHSTSConfig() error {
	maxAgeSeconds, err := validateAPIHSTSMaxAgeSeconds()
	if err != nil {
		return err
	}

	includeSubDomains, err := validateAPIHSTSBool(envAPIHSTSIncludeSubDomains, "api.hsts.include_subdomains", APIHSTSIncludeSubDomains())
	if err != nil {
		return err
	}

	preload, err := validateAPIHSTSBool(envAPIHSTSPreload, "api.hsts.preload", APIHSTSPreload())
	if err != nil {
		return err
	}

	if preload && !includeSubDomains {
		return errors.New("api.hsts.preload requires api.hsts.include_subdomains=true")
	}

	if preload && maxAgeSeconds < apiHSTSPreloadMinMaxAgeSeconds {
		return fmt.Errorf("api.hsts.preload requires api.hsts.max_age_seconds >= %d (got %d)", apiHSTSPreloadMinMaxAgeSeconds, maxAgeSeconds)
	}

	return nil
}

func validateAPIHSTSMaxAgeSeconds() (int, error) {
	if v := strings.TrimSpace(os.Getenv(envAPIHSTSMaxAgeSeconds)); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil {
			return 0, fmt.Errorf("api.hsts.max_age_seconds must be an integer number of seconds (got %q): %w", v, err)
		}

		if parsed < 0 {
			return 0, fmt.Errorf("api.hsts.max_age_seconds must be >= 0 (got %d)", parsed)
		}

		return parsed, nil
	}

	if viper.IsSet("api.hsts.max_age_seconds") {
		raw := strings.TrimSpace(viper.GetString("api.hsts.max_age_seconds"))
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return 0, fmt.Errorf("api.hsts.max_age_seconds must be an integer number of seconds (got %q): %w", raw, err)
		}

		if parsed < 0 {
			return 0, fmt.Errorf("api.hsts.max_age_seconds must be >= 0 (got %d)", parsed)
		}

		return parsed, nil
	}

	maxAgeSeconds := APIHSTSMaxAgeSeconds()
	if maxAgeSeconds < 0 {
		return 0, fmt.Errorf("api.hsts.max_age_seconds must be >= 0 (got %d)", maxAgeSeconds)
	}

	return maxAgeSeconds, nil
}

func validateAPIHSTSBool(envName, key string, fallback bool) (bool, error) {
	if v := strings.TrimSpace(os.Getenv(envName)); v != "" {
		parsed, err := strconv.ParseBool(v)
		if err != nil {
			return false, fmt.Errorf("%s must be a boolean (got %q): %w", key, v, err)
		}

		return parsed, nil
	}

	if viper.IsSet(key) {
		raw := strings.TrimSpace(viper.GetString(key))
		parsed, err := strconv.ParseBool(raw)
		if err != nil {
			return false, fmt.Errorf("%s must be a boolean (got %q): %w", key, raw, err)
		}

		return parsed, nil
	}

	return fallback, nil
}
