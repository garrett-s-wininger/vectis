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

func TestLogMaxRunBuffers_DefaultAndOverride(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	if got := LogMaxRunBuffers(); got != 1024 {
		t.Fatalf("LogMaxRunBuffers default: got %d", got)
	}

	viper.Set("max_run_buffers", 7)
	if got := LogMaxRunBuffers(); got != 7 {
		t.Fatalf("LogMaxRunBuffers override: got %d", got)
	}

	viper.Set("max_run_buffers", 0)
	if got := LogMaxRunBuffers(); got != 1024 {
		t.Fatalf("LogMaxRunBuffers nonpositive override should fall back to default: got %d", got)
	}
}

func TestAPIHostAndListenAddr_DefaultAndOverride(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	if got := APIHost(); got != "localhost" {
		t.Fatalf("APIHost default: got %q", got)
	}
	if got := APIListenAddr(); got != "localhost:8080" {
		t.Fatalf("APIListenAddr default: got %q", got)
	}

	viper.Set("host", "0.0.0.0")
	if got := APIHost(); got != "0.0.0.0" {
		t.Fatalf("APIHost override: got %q", got)
	}
	if got := APIListenAddr(); got != "0.0.0.0:8080" {
		t.Fatalf("APIListenAddr override: got %q", got)
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

func TestRegistryClusterDefaults(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	if got := RegistryClusterGossipInterval(); got != 2*time.Second {
		t.Fatalf("expected registry.cluster.gossip_interval 2s, got %v", got)
	}

	if got := RegistryClusterAntiEntropyInterval(); got != 30*time.Second {
		t.Fatalf("expected registry.cluster.anti_entropy_interval 30s, got %v", got)
	}

	if got := RegistryClusterLeaseTTL(); got != 2*time.Minute {
		t.Fatalf("expected registry.cluster.lease_ttl 2m, got %v", got)
	}

	if got := RegistryClusterTombstoneTTL(); got != 5*time.Minute {
		t.Fatalf("expected registry.cluster.tombstone_ttl 5m, got %v", got)
	}

	if got := RegistryClusterPeerDialTimeout(); got != 3*time.Second {
		t.Fatalf("expected registry.cluster.peer_dial_timeout 3s, got %v", got)
	}
}

func TestRegistryClusterPeerAddresses_CleansList(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("registry.cluster.peer_addresses", []string{"reg-a:8082", "reg-b:8082, reg-a:8082", ""})
	got := RegistryClusterPeerAddresses()
	if len(got) != 2 || got[0] != "reg-a:8082" || got[1] != "reg-b:8082" {
		t.Fatalf("unexpected peers: %#v", got)
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

func TestAPIRegistryDialAddress_FallbackToListenAddr(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	if got := APIRegistryDialAddress(); got != RegistryListenAddr() {
		t.Fatalf("with empty viper expected default listen addr %q, got %q", RegistryListenAddr(), got)
	}
}

func TestWorkerRegistryDialAddress_UsesRegistryAddressList(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("discovery.registry.addresses", []string{"reg-a:8082", "reg-b:8082"})
	if got := WorkerRegistryDialAddress(); got != "reg-a:8082,reg-b:8082" {
		t.Fatalf("expected registry address list, got %q", got)
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
