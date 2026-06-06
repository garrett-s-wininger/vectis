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

func TestMetricsAllowedHostsDefaultsToBindHostAndLoopback(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	want := []string{"metrics.example.com", "localhost", "127.0.0.1", "::1"}
	if got := MetricsAllowedHosts("metrics.example.com"); !reflect.DeepEqual(got, want) {
		t.Fatalf("MetricsAllowedHosts() = %v, want %v", got, want)
	}
}

func TestMetricsAllowedHostsDoesNotTrustUnspecifiedBindHost(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	want := []string{"localhost", "127.0.0.1", "::1"}
	if got := MetricsAllowedHosts("0.0.0.0"); !reflect.DeepEqual(got, want) {
		t.Fatalf("MetricsAllowedHosts() = %v, want %v", got, want)
	}
}

func TestMetricsAllowedHostsOverrides(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("metrics_allowed_hosts", []string{"Metrics.Example.com.", "metrics.example.com", "localhost:9090"})
	if got, want := MetricsAllowedHosts("localhost"), []string{"metrics.example.com", "localhost:9090"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("MetricsAllowedHosts() viper = %v, want %v", got, want)
	}

	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv(envMetricsAllowedHosts, "metrics-env.example, [::1]:9090")
	if got, want := MetricsAllowedHosts("localhost"), []string{"metrics-env.example", "[::1]:9090"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("MetricsAllowedHosts() global env = %v, want %v", got, want)
	}

	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.SetEnvPrefix("VECTIS_QUEUE")
	viper.AutomaticEnv()
	t.Setenv("VECTIS_QUEUE_METRICS_ALLOWED_HOSTS", "queue-metrics.example")
	t.Setenv(envMetricsAllowedHosts, "global-metrics.example")
	if got, want := MetricsAllowedHosts("localhost"), []string{"queue-metrics.example"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("MetricsAllowedHosts() service env = %v, want %v", got, want)
	}
}

func TestValidateMetricsHostConfigRejectsUnsafeHosts(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv(envMetricsAllowedHosts, "https://metrics.example")

	if err := ValidateMetricsHostConfig("localhost"); err == nil {
		t.Fatal("ValidateMetricsHostConfig() succeeded, want error")
	}
}

func TestMetricsHostAllowed(t *testing.T) {
	t.Setenv(envMetricsAllowedHosts, "metrics.example,localhost:9090,[::1]:9091")

	tests := []struct {
		host string
		want bool
	}{
		{host: "metrics.example", want: true},
		{host: "metrics.example:19090", want: true},
		{host: "localhost:9090", want: true},
		{host: "localhost:9091", want: false},
		{host: "[::1]:9091", want: true},
		{host: "evil.example", want: false},
		{host: "https://metrics.example", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.host, func(t *testing.T) {
			if got := MetricsHostAllowed("localhost", tt.host); got != tt.want {
				t.Fatalf("MetricsHostAllowed(%q) = %v, want %v", tt.host, got, tt.want)
			}
		})
	}
}

func TestCellIngressAllowedHostsDefaultsToBindHostLoopbackAndStaticLocalEndpoint(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("cell_ingress_endpoints", []string{
		"iad-a=http://Ingress.Example.com:8085",
		"pdx-b=http://pdx.example:8085",
	})

	want := []string{"localhost", "127.0.0.1", "::1", "ingress.example.com:8085"}
	if got := CellIngressAllowedHosts("iad-a", "0.0.0.0"); !reflect.DeepEqual(got, want) {
		t.Fatalf("CellIngressAllowedHosts() = %v, want %v", got, want)
	}
}

func TestCellIngressAllowedHostsOverrides(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("cell_ingress_endpoints", []string{"iad-a=http://ingress.example:8085"})
	viper.Set("allowed_hosts", []string{"Ingress.Override.Example.", "ingress.override.example:8085"})

	if got, want := CellIngressAllowedHosts("iad-a", "localhost"), []string{"ingress.override.example", "ingress.override.example:8085"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("CellIngressAllowedHosts() viper = %v, want %v", got, want)
	}

	t.Setenv(envCellIngressAllowedHosts, "ingress-env.example, [::1]:8085")
	if got, want := CellIngressAllowedHosts("iad-a", "localhost"), []string{"ingress-env.example", "[::1]:8085"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("CellIngressAllowedHosts() env = %v, want %v", got, want)
	}
}

func TestValidateCellIngressHostConfigRejectsUnsafeHosts(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv(envCellIngressAllowedHosts, "https://ingress.example")

	if err := ValidateCellIngressHostConfig("iad-a", "localhost"); err == nil {
		t.Fatal("ValidateCellIngressHostConfig() succeeded, want error")
	}
}

func TestValidateCellIngressHostConfigRejectsInvalidStaticLocalEndpoint(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("cell_ingress_endpoints", []string{"iad-a=ftp://ingress.example:8085"})

	if err := ValidateCellIngressHostConfig("iad-a", "localhost"); err == nil {
		t.Fatal("ValidateCellIngressHostConfig() succeeded, want error")
	}
}

func TestCellIngressHostAllowedFailsClosedForInvalidStaticLocalEndpoint(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("cell_ingress_endpoints", []string{"iad-a=ftp://ingress.example:8085"})

	if CellIngressHostAllowed("iad-a", "localhost", "localhost") {
		t.Fatal("CellIngressHostAllowed() accepted host with invalid static local endpoint")
	}
}

func TestCellIngressHostAllowed(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("cell_ingress_endpoints", []string{"iad-a=http://ingress.example:8085"})

	tests := []struct {
		host string
		want bool
	}{
		{host: "localhost", want: true},
		{host: "ingress.example:8085", want: true},
		{host: "ingress.example:8086", want: false},
		{host: "pdx.example:8085", want: false},
		{host: "https://ingress.example:8085", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.host, func(t *testing.T) {
			if got := CellIngressHostAllowed("iad-a", "localhost", tt.host); got != tt.want {
				t.Fatalf("CellIngressHostAllowed(%q) = %v, want %v", tt.host, got, tt.want)
			}
		})
	}
}
