package config

import (
	"testing"
	"time"

	"github.com/spf13/viper"
)

func TestMustDefaults_ReconcilerInterval(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	d := MustDefaults()
	if time.Duration(d.Reconciler.Interval) != 30*time.Second {
		t.Fatalf("expected reconciler.interval 30s, got %v", time.Duration(d.Reconciler.Interval))
	}

	if got := ReconcilerInterval(); got != 30*time.Second {
		t.Fatalf("ReconcilerInterval() with empty viper: got %v", got)
	}

	if d.Reconciler.MetricsPort != 9085 {
		t.Fatalf("expected reconciler.metrics_port 9085, got %d", d.Reconciler.MetricsPort)
	}

	if got := ReconcilerMetricsPort(); got != 9085 {
		t.Fatalf("ReconcilerMetricsPort() with empty viper: got %d", got)
	}
}

func TestRegistryResolverPollInterval_FromDefaults(t *testing.T) {
	if got := RegistryResolverPollInterval(); got != 10*time.Second {
		t.Fatalf("expected discovery.registry_resolver_refresh 10s, got %v", got)
	}
}

func TestRegistryResolverPollTimeout_FromDefaults(t *testing.T) {
	if got := RegistryResolverPollTimeout(); got != 5*time.Second {
		t.Fatalf("expected discovery.registry_resolver_poll_timeout 5s, got %v", got)
	}
}

func TestRegistryResolverErrorRefresh_FromDefaults(t *testing.T) {
	if got := RegistryResolverErrorRefresh(); got != 2*time.Second {
		t.Fatalf("expected discovery.registry_resolver_error_refresh 2s, got %v", got)
	}
}

func TestRegistryRegistrationRefresh_FromDefaults(t *testing.T) {
	if got := RegistryRegistrationRefresh(); got != 45*time.Second {
		t.Fatalf("expected discovery.registry_registration_refresh 45s, got %v", got)
	}
}

func TestQueueRegistryPublishAddress_AdvertiseOverride(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("queue.advertise_address", "q.example:9000")
	if got := QueueRegistryPublishAddress(":8081"); got != "q.example:9000" {
		t.Fatalf("got %q", got)
	}
}

func TestQueueRegistryPublishAddress_FallbackToBind(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	if got := QueueRegistryPublishAddress(":8081"); got != ":8081" {
		t.Fatalf("got %q", got)
	}
}

func TestWorkerRegistryDialAddress_FallbackToListenAddr(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	if got := WorkerRegistryDialAddress(); got != RegistryListenAddr() {
		t.Fatalf("with empty viper expected default listen addr %q, got %q", RegistryListenAddr(), got)
	}
}

func TestWorkerRegisterWithRegistry_DefaultsTrue(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	if !WorkerRegisterWithRegistry() {
		t.Fatalf("expected WorkerRegisterWithRegistry() to use defaults and return true")
	}
}

func TestWorkerRegisterWithRegistry_OverrideFalse(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("worker.register_with_registry", false)

	if WorkerRegisterWithRegistry() {
		t.Fatalf("expected WorkerRegisterWithRegistry() to honor worker.register_with_registry=false")
	}
}

func TestDiscovery_RegistryFallback(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("discovery.registry.address", ":disc")
	if got := APIRegistryAddress(); got != ":disc" {
		t.Fatalf("APIRegistryAddress: got %q", got)
	}
}
