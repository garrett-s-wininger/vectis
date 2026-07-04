package config

import (
	"testing"

	"github.com/spf13/viper"
)

func TestAPIHSTSDefaults(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	if got := APIHSTSMaxAgeSeconds(); got != 31536000 {
		t.Fatalf("APIHSTSMaxAgeSeconds() = %d, want 31536000", got)
	}

	if got := APIHSTSIncludeSubDomains(); got {
		t.Fatal("APIHSTSIncludeSubDomains() default = true, want false")
	}

	if got := APIHSTSPreload(); got {
		t.Fatal("APIHSTSPreload() default = true, want false")
	}

	if got := APIHSTSHeaderValue(); got != "max-age=31536000" {
		t.Fatalf("APIHSTSHeaderValue() = %q, want %q", got, "max-age=31536000")
	}
}

func TestAPIHSTSOverrides(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("api.hsts.max_age_seconds", 63072000)
	viper.Set("api.hsts.include_subdomains", true)
	viper.Set("api.hsts.preload", true)

	if got := APIHSTSHeaderValue(); got != "max-age=63072000; includeSubDomains; preload" {
		t.Fatalf("APIHSTSHeaderValue() viper = %q, want %q", got, "max-age=63072000; includeSubDomains; preload")
	}

	t.Setenv(envAPIHSTSMaxAgeSeconds, "60")
	t.Setenv(envAPIHSTSIncludeSubDomains, "false")
	t.Setenv(envAPIHSTSPreload, "false")

	if got := APIHSTSHeaderValue(); got != "max-age=60" {
		t.Fatalf("APIHSTSHeaderValue() env = %q, want %q", got, "max-age=60")
	}
}

func TestValidateAPIHSTSConfig(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	if err := ValidateAPIHSTSConfig(); err != nil {
		t.Fatalf("default config should validate: %v", err)
	}

	t.Setenv(envAPIHSTSMaxAgeSeconds, "-1")
	if err := ValidateAPIHSTSConfig(); err == nil {
		t.Fatal("expected negative max-age to be invalid")
	}
}

func TestValidateAPIHSTSConfig_invalidValues(t *testing.T) {
	tests := []struct {
		name string
		env  string
		val  string
	}{
		{name: "max age", env: envAPIHSTSMaxAgeSeconds, val: "not-a-number"},
		{name: "include subdomains", env: envAPIHSTSIncludeSubDomains, val: "tru"},
		{name: "preload", env: envAPIHSTSPreload, val: "tru"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			viper.Reset()
			t.Cleanup(viper.Reset)

			t.Setenv(tt.env, tt.val)
			if err := ValidateAPIHSTSConfig(); err == nil {
				t.Fatal("expected invalid HSTS config error")
			}
		})
	}
}

func TestValidateAPIHSTSConfig_invalidViperValues(t *testing.T) {
	tests := []struct {
		name string
		key  string
		val  string
	}{
		{name: "max age", key: "api.hsts.max_age_seconds", val: "not-a-number"},
		{name: "include subdomains", key: "api.hsts.include_subdomains", val: "tru"},
		{name: "preload", key: "api.hsts.preload", val: "tru"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			viper.Reset()
			t.Cleanup(viper.Reset)

			viper.Set(tt.key, tt.val)
			if err := ValidateAPIHSTSConfig(); err == nil {
				t.Fatal("expected invalid HSTS config error")
			}
		})
	}
}

func TestValidateAPIHSTSConfig_preloadRequiresIncludeSubDomains(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("api.hsts.preload", true)
	if err := ValidateAPIHSTSConfig(); err == nil {
		t.Fatal("expected preload without includeSubDomains to be invalid")
	}
}

func TestValidateAPIHSTSConfig_preloadRequiresOneYearMaxAge(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("api.hsts.max_age_seconds", apiHSTSPreloadMinMaxAgeSeconds-1)
	viper.Set("api.hsts.include_subdomains", true)
	viper.Set("api.hsts.preload", true)

	if err := ValidateAPIHSTSConfig(); err == nil {
		t.Fatal("expected preload with short max-age to be invalid")
	}

	viper.Set("api.hsts.max_age_seconds", apiHSTSPreloadMinMaxAgeSeconds)
	if err := ValidateAPIHSTSConfig(); err != nil {
		t.Fatalf("expected preload with one-year max-age and includeSubDomains to validate: %v", err)
	}
}
