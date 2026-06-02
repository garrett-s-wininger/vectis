package config

import (
	"reflect"
	"testing"

	"github.com/spf13/viper"
)

func TestAPICORSAllowedOriginsDefaultsClosed(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	if got := APICORSAllowedOrigins(); len(got) != 0 {
		t.Fatalf("APICORSAllowedOrigins() = %v, want empty", got)
	}
}

func TestAPICORSAllowedOriginsOverrides(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("api.cors.allowed_origins", []string{"https://ui.example", "https://ui.example/"})
	if got, want := APICORSAllowedOrigins(), []string{"https://ui.example"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("APICORSAllowedOrigins() viper = %v, want %v", got, want)
	}

	t.Setenv(envAPICORSAllowedOrigins, "https://a.example, https://b.example/")
	if got, want := APICORSAllowedOrigins(), []string{"https://a.example", "https://b.example"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("APICORSAllowedOrigins() env = %v, want %v", got, want)
	}
}

func TestValidateAPICORSConfigRejectsUnsafeOrigins(t *testing.T) {
	tests := []string{
		"*",
		"null",
		"file://example.test",
		"https://example.test/path",
		"https://example.test?query=1",
		"https://user@example.test",
	}

	for _, origin := range tests {
		t.Run(origin, func(t *testing.T) {
			t.Setenv(envAPICORSAllowedOrigins, origin)
			if err := ValidateAPICORSConfig(); err == nil {
				t.Fatal("expected invalid CORS origin error")
			}
		})
	}
}

func TestValidateAPICORSConfigAllowsExactHTTPOrigins(t *testing.T) {
	t.Setenv(envAPICORSAllowedOrigins, "https://ui.example,http://localhost:3000")
	if err := ValidateAPICORSConfig(); err != nil {
		t.Fatalf("ValidateAPICORSConfig(): %v", err)
	}
}
