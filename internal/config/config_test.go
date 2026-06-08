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
	if time.Duration(d.Cron.ClaimTTL) != 5*time.Minute {
		t.Fatalf("expected cron.claim_ttl 5m, got %v", time.Duration(d.Cron.ClaimTTL))
	}

	if got := CronClaimTTL(); got != 5*time.Minute {
		t.Fatalf("CronClaimTTL() with empty viper: got %v", got)
	}

	viper.Set("claim_ttl", 90*time.Second)
	if got := CronClaimTTL(); got != 90*time.Second {
		t.Fatalf("CronClaimTTL() with flat viper override: got %v", got)
	}

	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("cron.claim_ttl", 2*time.Minute)
	if got := CronClaimTTL(); got != 2*time.Minute {
		t.Fatalf("CronClaimTTL() with namespaced viper override: got %v", got)
	}

	if time.Duration(d.Reconciler.Interval) != 30*time.Second {
		t.Fatalf("expected reconciler.interval 30s, got %v", time.Duration(d.Reconciler.Interval))
	}

	if got := ReconcilerInterval(); got != 30*time.Second {
		t.Fatalf("ReconcilerInterval() with empty viper: got %v", got)
	}

	if time.Duration(d.Reconciler.LeaseTTL) != 2*time.Minute {
		t.Fatalf("expected reconciler.lease_ttl 2m, got %v", time.Duration(d.Reconciler.LeaseTTL))
	}

	if got := ReconcilerLeaseTTL(); got != 2*time.Minute {
		t.Fatalf("ReconcilerLeaseTTL() with empty viper: got %v", got)
	}

	if d.Reconciler.MetricsPort != 9085 {
		t.Fatalf("expected reconciler.metrics_port 9085, got %d", d.Reconciler.MetricsPort)
	}

	if d.Reconciler.MetricsHost != "localhost" {
		t.Fatalf("expected reconciler.metrics_host localhost, got %q", d.Reconciler.MetricsHost)
	}

	if got := ReconcilerMetricsHost(); got != "localhost" {
		t.Fatalf("ReconcilerMetricsHost() with empty viper: got %q", got)
	}

	if got := ReconcilerMetricsListenAddr(); got != "localhost:9085" {
		t.Fatalf("ReconcilerMetricsListenAddr() with empty viper: got %q", got)
	}

	if got := ReconcilerMetricsPort(); got != 9085 {
		t.Fatalf("ReconcilerMetricsPort() with empty viper: got %d", got)
	}

	if d.LogForwarder.MetricsPort != 9088 {
		t.Fatalf("expected log_forwarder.metrics_port 9088, got %d", d.LogForwarder.MetricsPort)
	}

	if d.LogForwarder.MetricsHost != "localhost" {
		t.Fatalf("expected log_forwarder.metrics_host localhost, got %q", d.LogForwarder.MetricsHost)
	}

	if got := LogForwarderMetricsHost(); got != "localhost" {
		t.Fatalf("LogForwarderMetricsHost() with empty viper: got %q", got)
	}

	if got := LogForwarderMetricsListenAddr(); got != "localhost:9088" {
		t.Fatalf("LogForwarderMetricsListenAddr() with empty viper: got %q", got)
	}

	if got := LogForwarderMetricsPort(); got != 9088 {
		t.Fatalf("LogForwarderMetricsPort() with empty viper: got %d", got)
	}

	viper.Set("metrics_host", "0.0.0.0")
	viper.Set("metrics_port", 19086)
	if got := LogForwarderMetricsHost(); got != "0.0.0.0" {
		t.Fatalf("LogForwarderMetricsHost() override: got %q", got)
	}

	if got := LogForwarderMetricsListenAddr(); got != "0.0.0.0:19086" {
		t.Fatalf("LogForwarderMetricsListenAddr() override: got %q", got)
	}

	if got := LogForwarderMetricsEffectiveListenPort(); got != 19086 {
		t.Fatalf("LogForwarderMetricsEffectiveListenPort() override: got %d", got)
	}
}

func TestMustDefaults_Catalog(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	d := MustDefaults()
	if time.Duration(d.Catalog.Interval) != time.Second {
		t.Fatalf("expected catalog.interval 1s, got %v", time.Duration(d.Catalog.Interval))
	}

	if got := CatalogInterval(); got != time.Second {
		t.Fatalf("CatalogInterval() with empty viper: got %v", got)
	}

	if d.Catalog.BatchSize != 100 {
		t.Fatalf("expected catalog.batch_size 100, got %d", d.Catalog.BatchSize)
	}

	if got := CatalogBatchSize(); got != 100 {
		t.Fatalf("CatalogBatchSize() with empty viper: got %d", got)
	}

	if d.Catalog.MetricsPort != 9086 {
		t.Fatalf("expected catalog.metrics_port 9086, got %d", d.Catalog.MetricsPort)
	}

	if d.Catalog.MetricsHost != "localhost" {
		t.Fatalf("expected catalog.metrics_host localhost, got %q", d.Catalog.MetricsHost)
	}

	if got := CatalogMetricsHost(); got != "localhost" {
		t.Fatalf("CatalogMetricsHost() with empty viper: got %q", got)
	}

	if got := CatalogMetricsListenAddr(); got != "localhost:9086" {
		t.Fatalf("CatalogMetricsListenAddr() with empty viper: got %q", got)
	}

	if got := CatalogMetricsPort(); got != 9086 {
		t.Fatalf("CatalogMetricsPort() with empty viper: got %d", got)
	}
}

func TestMustDefaults_CellIngress(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	d := MustDefaults()
	if d.CellIngress.Host != "localhost" {
		t.Fatalf("expected cell_ingress.host localhost, got %q", d.CellIngress.Host)
	}

	if got := CellIngressHost(); got != "localhost" {
		t.Fatalf("CellIngressHost() with empty viper: got %q", got)
	}

	if d.CellIngress.Port != 8085 {
		t.Fatalf("expected cell_ingress.port 8085, got %d", d.CellIngress.Port)
	}

	if got := CellIngressEffectiveListenPort(); got != 8085 {
		t.Fatalf("CellIngressEffectiveListenPort() with empty viper: got %d", got)
	}

	if got := CellIngressListenAddr(); got != "localhost:8085" {
		t.Fatalf("CellIngressListenAddr() with empty viper: got %q", got)
	}

	if d.CellIngress.MetricsPort != 9087 {
		t.Fatalf("expected cell_ingress.metrics_port 9087, got %d", d.CellIngress.MetricsPort)
	}

	if d.CellIngress.MetricsHost != "localhost" {
		t.Fatalf("expected cell_ingress.metrics_host localhost, got %q", d.CellIngress.MetricsHost)
	}

	if got := CellIngressMetricsHost(); got != "localhost" {
		t.Fatalf("CellIngressMetricsHost() with empty viper: got %q", got)
	}

	if got := CellIngressMetricsEffectiveListenPort(); got != 9087 {
		t.Fatalf("CellIngressMetricsEffectiveListenPort() with empty viper: got %d", got)
	}

	if got := CellIngressMetricsListenAddr(); got != "localhost:9087" {
		t.Fatalf("CellIngressMetricsListenAddr() with empty viper: got %q", got)
	}

	if time.Duration(d.CellIngress.RepairInterval) != 30*time.Second {
		t.Fatalf("expected cell_ingress.repair_interval 30s, got %v", time.Duration(d.CellIngress.RepairInterval))
	}

	if got := CellIngressRepairInterval(); got != 30*time.Second {
		t.Fatalf("CellIngressRepairInterval() with empty viper: got %v", got)
	}
}

func TestCellIngressConfigOverrides(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("host", "0.0.0.0")
	viper.Set("port", 18085)
	viper.Set("metrics_host", "127.0.0.1")
	viper.Set("metrics_port", 19087)
	viper.Set("repair_interval", 2*time.Second)
	viper.Set("cell_ingress.queue.address", "queue.local:8081")
	viper.Set("cell_ingress.registry.address", "registry.local:8082")

	if got := CellIngressListenAddr(); got != "0.0.0.0:18085" {
		t.Fatalf("CellIngressListenAddr() override: got %q", got)
	}

	if got := CellIngressMetricsEffectiveListenPort(); got != 19087 {
		t.Fatalf("CellIngressMetricsEffectiveListenPort() override: got %d", got)
	}

	if got := CellIngressMetricsListenAddr(); got != "127.0.0.1:19087" {
		t.Fatalf("CellIngressMetricsListenAddr() override: got %q", got)
	}

	if got := CellIngressRepairInterval(); got != 2*time.Second {
		t.Fatalf("CellIngressRepairInterval() override: got %v", got)
	}

	if got := CellIngressQueueAddress(); got != "queue.local:8081" {
		t.Fatalf("CellIngressQueueAddress() override: got %q", got)
	}

	if got := CellIngressRegistryAddress(); got != "registry.local:8082" {
		t.Fatalf("CellIngressRegistryAddress() override: got %q", got)
	}
}

func TestMetricsListenAddressesDefaultToLocalhost(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	d := MustDefaults()
	cases := []struct {
		name string
		host string
		addr string
	}{
		{name: "queue", host: d.Queue.MetricsHost, addr: QueueMetricsListenAddr()},
		{name: "log", host: d.Log.MetricsHost, addr: LogMetricsListenAddr()},
		{name: "worker", host: d.Worker.MetricsHost, addr: WorkerMetricsListenAddr()},
	}

	for _, tc := range cases {
		if tc.host != "localhost" {
			t.Fatalf("%s metrics_host default = %q, want localhost", tc.name, tc.host)
		}
	}

	wantAddr := map[string]string{
		"queue":  "localhost:9081",
		"log":    "localhost:9083",
		"worker": "localhost:9082",
	}

	for _, tc := range cases {
		if tc.addr != wantAddr[tc.name] {
			t.Fatalf("%s metrics listen addr = %q, want %q", tc.name, tc.addr, wantAddr[tc.name])
		}
	}

	viper.Set("metrics_host", "::1")
	viper.Set("metrics_port", 19081)
	if got := QueueMetricsListenAddr(); got != "[::1]:19081" {
		t.Fatalf("QueueMetricsListenAddr() override = %q, want [::1]:19081", got)
	}
}

func TestCellID_DefaultViperAndEnv(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	if got := CellID(); got != "local" {
		t.Fatalf("CellID default: got %q", got)
	}

	viper.Set("cell.id", "iad-a")
	if got := CellID(); got != "iad-a" {
		t.Fatalf("CellID viper override: got %q", got)
	}

	t.Setenv(envCellID, "  dfw-b  ")
	if got := CellID(); got != "dfw-b" {
		t.Fatalf("CellID env override: got %q", got)
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

func TestLogStorageReadOnlyMinFreeBytes_DefaultAndOverride(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	if got := LogStorageReadOnlyMinFreeBytes(); got != 1<<30 {
		t.Fatalf("LogStorageReadOnlyMinFreeBytes default: got %d", got)
	}

	viper.Set("storage_read_only_min_free_bytes", uint64(2048))
	if got := LogStorageReadOnlyMinFreeBytes(); got != 2048 {
		t.Fatalf("LogStorageReadOnlyMinFreeBytes override: got %d", got)
	}

	viper.Set("storage_read_only_min_free_bytes", uint64(0))
	if got := LogStorageReadOnlyMinFreeBytes(); got != 0 {
		t.Fatalf("LogStorageReadOnlyMinFreeBytes should allow disabling threshold: got %d", got)
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

func TestAPICellIngressEndpoints_DefaultAndOverride(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	if got := APICellIngressEndpointSpecs(); len(got) != 0 {
		t.Fatalf("APICellIngressEndpointSpecs default: got %+v, want empty", got)
	}

	viper.Set("cell_ingress_endpoints", []string{"iad-a=https://iad.example:8085", "pdx-b=https://pdx.example"})
	got, err := APICellIngressEndpoints()
	if err != nil {
		t.Fatalf("APICellIngressEndpoints: %v", err)
	}

	if got["iad-a"] != "https://iad.example:8085" {
		t.Fatalf("iad endpoint: got %q", got["iad-a"])
	}

	if got["pdx-b"] != "https://pdx.example" {
		t.Fatalf("pdx endpoint: got %q", got["pdx-b"])
	}
}

func TestCellIngressEndpoints_GenericConfig(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("cell_ingress_endpoints", "local=http://localhost:8085")
	got, err := CellIngressEndpoints()
	if err != nil {
		t.Fatalf("CellIngressEndpoints: %v", err)
	}

	if got["local"] != "http://localhost:8085" {
		t.Fatalf("local endpoint: got %q", got["local"])
	}
}

func TestCatalogCellDatabaseDSNs(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("cell_database_dsns", []string{"local=/tmp/local.db", "pdx-b=/tmp/pdx.db"})
	got, err := CatalogCellDatabaseDSNs()
	if err != nil {
		t.Fatalf("CatalogCellDatabaseDSNs: %v", err)
	}

	if got["local"] != "/tmp/local.db" {
		t.Fatalf("local DSN: got %q", got["local"])
	}

	if got["pdx-b"] != "/tmp/pdx.db" {
		t.Fatalf("pdx DSN: got %q", got["pdx-b"])
	}
}

func TestParseCellIngressEndpointsRejectsInvalidSpec(t *testing.T) {
	if _, err := ParseCellIngressEndpoints([]string{"iad-a"}); err == nil {
		t.Fatal("ParseCellIngressEndpoints succeeded, want error")
	}
}

func TestParseCellDatabaseDSNsRejectsInvalidSpec(t *testing.T) {
	if _, err := ParseCellDatabaseDSNs([]string{"iad-a"}); err == nil {
		t.Fatal("ParseCellDatabaseDSNs succeeded, want error")
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

func TestWorkerExecutionDefaultsAndOverrides(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	if got := WorkerExecutionBackend(); got != "host" {
		t.Fatalf("default backend = %q, want host", got)
	}
	if got := WorkerExecutionLimaPath(); got != "limactl" {
		t.Fatalf("default lima path = %q, want limactl", got)
	}
	if got := WorkerExecutionWorkspaceRoot(); got != "" {
		t.Fatalf("default workspace root = %q, want empty", got)
	}
	if got := WorkerExecutionLimaInstance(); got != "" {
		t.Fatalf("default lima instance = %q, want empty", got)
	}
	if got := WorkerExecutionLimaGuestWorkspaceRoot(); got != "" {
		t.Fatalf("default lima guest workspace root = %q, want empty", got)
	}
	if WorkerExecutionLimaStart() {
		t.Fatal("default lima start = true, want false")
	}
	if WorkerExecutionLimaPreserveEnv() {
		t.Fatal("default lima preserve env = true, want false")
	}

	viper.Set("worker.execution.backend", " LIMA ")
	viper.Set("worker.execution.workspace_root", "/Users/me/vectis-work")
	viper.Set("worker.execution.lima.path", "/opt/homebrew/bin/limactl")
	viper.Set("worker.execution.lima.instance", "vectis-worker")
	viper.Set("worker.execution.lima.guest_workspace_root", "/tmp/vectis-workspaces")
	viper.Set("worker.execution.lima.start", true)
	viper.Set("worker.execution.lima.preserve_env", true)

	if got := WorkerExecutionBackend(); got != "lima" {
		t.Fatalf("override backend = %q, want lima", got)
	}
	if got := WorkerExecutionWorkspaceRoot(); got != "/Users/me/vectis-work" {
		t.Fatalf("override workspace root = %q", got)
	}
	if got := WorkerExecutionLimaPath(); got != "/opt/homebrew/bin/limactl" {
		t.Fatalf("override lima path = %q", got)
	}
	if got := WorkerExecutionLimaInstance(); got != "vectis-worker" {
		t.Fatalf("override lima instance = %q", got)
	}
	if got := WorkerExecutionLimaGuestWorkspaceRoot(); got != "/tmp/vectis-workspaces" {
		t.Fatalf("override lima guest workspace root = %q", got)
	}
	if !WorkerExecutionLimaStart() {
		t.Fatal("override lima start = false, want true")
	}
	if !WorkerExecutionLimaPreserveEnv() {
		t.Fatal("override lima preserve env = false, want true")
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
