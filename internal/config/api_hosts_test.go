package config

import (
	"reflect"
	"testing"

	"github.com/spf13/viper"
)

func TestAPIAllowedHostsDefaultsToLocalhostAndLoopback(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	want := []string{"localhost", "127.0.0.1", "::1"}
	if got := APIAllowedHosts(); !reflect.DeepEqual(got, want) {
		t.Fatalf("APIAllowedHosts() = %v, want %v", got, want)
	}
}

func TestAPIAllowedHostsDerivesSpecificListenHost(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("host", "api.example.com")

	want := []string{"api.example.com", "localhost", "127.0.0.1", "::1"}
	if got := APIAllowedHosts(); !reflect.DeepEqual(got, want) {
		t.Fatalf("APIAllowedHosts() = %v, want %v", got, want)
	}
}

func TestAPIAllowedHostsDoesNotTrustUnspecifiedListenHost(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("host", "0.0.0.0")

	want := []string{"localhost", "127.0.0.1", "::1"}
	if got := APIAllowedHosts(); !reflect.DeepEqual(got, want) {
		t.Fatalf("APIAllowedHosts() = %v, want %v", got, want)
	}
}

func TestAPIAllowedHostsOverrides(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("api.host_validation.allowed_hosts", []string{"API.Example.com.", "api.example.com", "localhost:8080"})
	if got, want := APIAllowedHosts(), []string{"api.example.com", "localhost:8080"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("APIAllowedHosts() viper = %v, want %v", got, want)
	}

	t.Setenv(envAPIAllowedHosts, "vectis.example, [::1]:8080")
	if got, want := APIAllowedHosts(), []string{"vectis.example", "[::1]:8080"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("APIAllowedHosts() env = %v, want %v", got, want)
	}
}

func TestValidateAPIHostConfigRejectsUnsafeHosts(t *testing.T) {
	tests := []string{
		"*",
		"https://api.example",
		"api.example/path",
		"api.example?x=1",
		"user@api.example",
		"api example",
		"api.example:bad",
		"api.example:0",
	}

	for _, host := range tests {
		t.Run(host, func(t *testing.T) {
			viper.Reset()
			t.Cleanup(viper.Reset)
			t.Setenv(envAPIAllowedHosts, host)
			if err := ValidateAPIHostConfig(); err == nil {
				t.Fatalf("ValidateAPIHostConfig(%q) succeeded, want error", host)
			}
		})
	}
}

func TestValidateAPIHostConfigAllowsExactHosts(t *testing.T) {
	t.Setenv(envAPIAllowedHosts, "api.example,api.example:8443,127.0.0.1,[::1]:8080")
	if err := ValidateAPIHostConfig(); err != nil {
		t.Fatalf("ValidateAPIHostConfig(): %v", err)
	}
}

func TestAPIHostAllowed(t *testing.T) {
	t.Setenv(envAPIAllowedHosts, "api.example,localhost:8080,[::1]:9090")

	tests := []struct {
		host string
		want bool
	}{
		{host: "api.example", want: true},
		{host: "api.example:8443", want: true},
		{host: "api.example.", want: true},
		{host: "localhost:8080", want: true},
		{host: "localhost:9090", want: false},
		{host: "[::1]:9090", want: true},
		{host: "[::1]:8080", want: false},
		{host: "evil.example", want: false},
		{host: "https://api.example", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.host, func(t *testing.T) {
			if got := APIHostAllowed(tt.host); got != tt.want {
				t.Fatalf("APIHostAllowed(%q) = %v, want %v", tt.host, got, tt.want)
			}
		})
	}
}

func TestDocsAllowedHostsDefaultsToBindHostAndLoopback(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	want := []string{"docs.example.com", "localhost", "127.0.0.1", "::1"}
	if got := DocsAllowedHosts("docs.example.com"); !reflect.DeepEqual(got, want) {
		t.Fatalf("DocsAllowedHosts() = %v, want %v", got, want)
	}
}

func TestDocsAllowedHostsDoesNotTrustUnspecifiedBindHost(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	want := []string{"localhost", "127.0.0.1", "::1"}
	if got := DocsAllowedHosts("0.0.0.0"); !reflect.DeepEqual(got, want) {
		t.Fatalf("DocsAllowedHosts() = %v, want %v", got, want)
	}
}

func TestDocsAllowedHostsOverrides(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("allowed_hosts", []string{"Docs.Example.com.", "docs.example.com", "localhost:8088"})
	if got, want := DocsAllowedHosts("localhost"), []string{"docs.example.com", "localhost:8088"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("DocsAllowedHosts() viper = %v, want %v", got, want)
	}

	t.Setenv(envDocsAllowedHosts, "docs-env.example, [::1]:8088")
	if got, want := DocsAllowedHosts("localhost"), []string{"docs-env.example", "[::1]:8088"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("DocsAllowedHosts() env = %v, want %v", got, want)
	}
}

func TestValidateDocsHostConfigRejectsUnsafeHosts(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv(envDocsAllowedHosts, "https://docs.example")

	if err := ValidateDocsHostConfig("localhost"); err == nil {
		t.Fatal("ValidateDocsHostConfig() succeeded, want error")
	}
}

func TestDocsHostAllowed(t *testing.T) {
	t.Setenv(envDocsAllowedHosts, "docs.example,localhost:8088,[::1]:9090")

	tests := []struct {
		host string
		want bool
	}{
		{host: "docs.example", want: true},
		{host: "docs.example:8443", want: true},
		{host: "localhost:8088", want: true},
		{host: "localhost:9090", want: false},
		{host: "[::1]:9090", want: true},
		{host: "evil.example", want: false},
		{host: "https://docs.example", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.host, func(t *testing.T) {
			if got := DocsHostAllowed("localhost", tt.host); got != tt.want {
				t.Fatalf("DocsHostAllowed(%q) = %v, want %v", tt.host, got, tt.want)
			}
		})
	}
}
