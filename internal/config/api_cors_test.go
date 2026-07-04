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
		"http://ui.example",
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

func TestValidateAPICORSConfigAllowsHTTPSAndLocalHTTPOrigins(t *testing.T) {
	t.Setenv(envAPICORSAllowedOrigins, "https://ui.example,http://localhost:3000,http://127.0.0.1:3000,http://[::1]:3000,http://dev.localhost:3000")
	if err := ValidateAPICORSConfig(); err != nil {
		t.Fatalf("ValidateAPICORSConfig(): %v", err)
	}
}

func TestIsLoopbackCORSOriginHost(t *testing.T) {
	tests := []struct {
		host string
		want bool
	}{
		{host: "localhost", want: true},
		{host: "localhost.", want: true},
		{host: "dev.localhost", want: true},
		{host: "127.0.0.1", want: true},
		{host: "::1", want: true},
		{host: "ui.example", want: false},
		{host: "10.0.0.1", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.host, func(t *testing.T) {
			if got := isLoopbackCORSOriginHost(tt.host); got != tt.want {
				t.Fatalf("isLoopbackCORSOriginHost(%q) = %v, want %v", tt.host, got, tt.want)
			}
		})
	}
}
