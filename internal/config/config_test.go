package config

import (
	"path/filepath"
	"strings"
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

	if d.Reconciler.RedispatchLimit != 1000 {
		t.Fatalf("expected reconciler.redispatch_limit 1000, got %d", d.Reconciler.RedispatchLimit)
	}

	if got := ReconcilerRedispatchLimit(); got != 1000 {
		t.Fatalf("ReconcilerRedispatchLimit() with empty viper: got %d", got)
	}

	viper.Set("redispatch_limit", 250)
	if got := ReconcilerRedispatchLimit(); got != 250 {
		t.Fatalf("ReconcilerRedispatchLimit() with flat viper override: got %d", got)
	}

	viper.Set("redispatch_limit", 0)

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

	viper.Set("metrics_host", "")
	viper.Set("metrics_port", 0)

	if d.WorkerCore.MetricsPort != 9092 {
		t.Fatalf("expected worker_core.metrics_port 9092, got %d", d.WorkerCore.MetricsPort)
	}

	if d.WorkerCore.MetricsHost != "localhost" {
		t.Fatalf("expected worker_core.metrics_host localhost, got %q", d.WorkerCore.MetricsHost)
	}

	if got := WorkerCoreMetricsHost(); got != "localhost" {
		t.Fatalf("WorkerCoreMetricsHost() with empty viper: got %q", got)
	}

	if got := WorkerCoreMetricsListenAddr(); got != "localhost:9092" {
		t.Fatalf("WorkerCoreMetricsListenAddr() with empty viper: got %q", got)
	}

	if got := WorkerCoreMetricsPort(); got != 9092 {
		t.Fatalf("WorkerCoreMetricsPort() with empty viper: got %d", got)
	}

	viper.Set("metrics_host", "127.0.0.1")
	viper.Set("metrics_port", 19092)
	if got := WorkerCoreMetricsListenAddr(); got != "127.0.0.1:19092" {
		t.Fatalf("WorkerCoreMetricsListenAddr() override: got %q", got)
	}

	if got := WorkerCoreMetricsEffectiveListenPort(); got != 19092 {
		t.Fatalf("WorkerCoreMetricsEffectiveListenPort() override: got %d", got)
	}
}

func TestSecretsPolicyAllowRules(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	if got := SecretsPolicyAllowRules(); len(got) != 0 {
		t.Fatalf("SecretsPolicyAllowRules defaults = %v, want empty", got)
	}

	viper.Set("policy_allow", []string{
		"namespace=/team-a;job=job-1;task=publish;ref=encryptedfs://team-a/npm-token",
		"namespace=/team-a;job=job-1;task=publish;ref=encryptedfs://team-a/npm-token",
	})

	got := SecretsPolicyAllowRules()
	if len(got) != 1 || got[0] != "namespace=/team-a;job=job-1;task=publish;ref=encryptedfs://team-a/npm-token" {
		t.Fatalf("SecretsPolicyAllowRules flat override = %v", got)
	}

	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("secrets.policy.allow", "namespace=*;job=*;task=*;ref=encryptedfs://*")
	got = SecretsPolicyAllowRules()
	if len(got) != 1 || got[0] != "namespace=*;job=*;task=*;ref=encryptedfs://*" {
		t.Fatalf("SecretsPolicyAllowRules namespaced override = %v", got)
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

func TestMustDefaults_Dispatch(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	d := MustDefaults()
	if time.Duration(d.Dispatch.StartTTL) != 24*time.Hour {
		t.Fatalf("expected dispatch.start_ttl 24h, got %v", time.Duration(d.Dispatch.StartTTL))
	}

	if got := DispatchStartTTL(); got != 24*time.Hour {
		t.Fatalf("DispatchStartTTL() with empty viper: got %v", got)
	}

	viper.Set("dispatch.start_ttl", 2*time.Hour)
	if got := DispatchStartTTL(); got != 2*time.Hour {
		t.Fatalf("DispatchStartTTL() with viper override: got %v", got)
	}
}

func TestRetentionCleanupPolicyDefaultsAndOverrides(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	wantDefault := RetentionCleanupPolicyDefaults{
		TerminalRuns:    30 * 24 * time.Hour,
		JobDefinitions:  30 * 24 * time.Hour,
		IdempotencyKeys: 24 * time.Hour,
		AuditLog:        365 * 24 * time.Hour,
		ArtifactBlobs:   30 * 24 * time.Hour,
	}
	if got, want := RetentionCleanupPolicy(), wantDefault; got != want {
		t.Fatalf("RetentionCleanupPolicy() defaults = %+v, want %+v", got, want)
	}
	if got := RetentionCleanupBackupMaxAge(); got != 0 {
		t.Fatalf("RetentionCleanupBackupMaxAge() default = %v, want 0", got)
	}
	if got := RetentionCleanupBackupStorageMaxAge(); got != 0 {
		t.Fatalf("RetentionCleanupBackupStorageMaxAge() default = %v, want 0", got)
	}
	if got := RetentionCleanupAuditExportMaxAge(); got != 0 {
		t.Fatalf("RetentionCleanupAuditExportMaxAge() default = %v, want 0", got)
	}
	if got := RetentionCleanupHoldReviewMaxAge(); got != 0 {
		t.Fatalf("RetentionCleanupHoldReviewMaxAge() default = %v, want 0", got)
	}
	if RetentionCleanupRequireBackupManifest() {
		t.Fatal("RetentionCleanupRequireBackupManifest() default = true, want false")
	}
	if RetentionCleanupRequireAuditExport() {
		t.Fatal("RetentionCleanupRequireAuditExport() default = true, want false")
	}
	if RetentionCleanupRequireHoldReview() {
		t.Fatal("RetentionCleanupRequireHoldReview() default = true, want false")
	}

	viper.Set("retention.cleanup.terminal_run_age", 2*time.Hour)
	viper.Set("retention.cleanup.job_definition_age", 3*time.Hour)
	viper.Set("retention.cleanup.idempotency_age", 4*time.Hour)
	viper.Set("retention.cleanup.audit_age", 5*time.Hour)
	viper.Set("retention.cleanup.artifact_blob_age", 6*time.Hour)
	viper.Set("retention.cleanup.backup_max_age", time.Hour)
	viper.Set("retention.cleanup.backup_storage_max_age", 30*time.Minute)
	viper.Set("retention.cleanup.audit_export_max_age", 45*time.Minute)
	viper.Set("retention.cleanup.hold_review_max_age", 15*time.Minute)
	viper.Set("retention.cleanup.require_backup_manifest", true)
	viper.Set("retention.cleanup.require_audit_export", true)
	viper.Set("retention.cleanup.require_hold_review", true)

	got := RetentionCleanupPolicy()
	want := RetentionCleanupPolicyDefaults{
		TerminalRuns:    2 * time.Hour,
		JobDefinitions:  3 * time.Hour,
		IdempotencyKeys: 4 * time.Hour,
		AuditLog:        5 * time.Hour,
		ArtifactBlobs:   6 * time.Hour,
	}
	if got != want {
		t.Fatalf("RetentionCleanupPolicy() override = %+v, want %+v", got, want)
	}
	if got := RetentionCleanupBackupMaxAge(); got != time.Hour {
		t.Fatalf("RetentionCleanupBackupMaxAge() override = %v, want 1h", got)
	}
	if got := RetentionCleanupBackupStorageMaxAge(); got != 30*time.Minute {
		t.Fatalf("RetentionCleanupBackupStorageMaxAge() override = %v, want 30m", got)
	}
	if got := RetentionCleanupAuditExportMaxAge(); got != 45*time.Minute {
		t.Fatalf("RetentionCleanupAuditExportMaxAge() override = %v, want 45m", got)
	}
	if got := RetentionCleanupHoldReviewMaxAge(); got != 15*time.Minute {
		t.Fatalf("RetentionCleanupHoldReviewMaxAge() override = %v, want 15m", got)
	}
	if !RetentionCleanupRequireBackupManifest() {
		t.Fatal("RetentionCleanupRequireBackupManifest() override = false, want true")
	}
	if !RetentionCleanupRequireAuditExport() {
		t.Fatal("RetentionCleanupRequireAuditExport() override = false, want true")
	}
	if !RetentionCleanupRequireHoldReview() {
		t.Fatal("RetentionCleanupRequireHoldReview() override = false, want true")
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
		{name: "orchestrator", host: d.Orchestrator.MetricsHost, addr: OrchestratorMetricsListenAddr()},
		{name: "log", host: d.Log.MetricsHost, addr: LogMetricsListenAddr()},
		{name: "artifact", host: d.Artifact.MetricsHost, addr: ArtifactMetricsListenAddr()},
		{name: "worker", host: d.Worker.MetricsHost, addr: WorkerMetricsListenAddr()},
		{name: "worker-core", host: d.WorkerCore.MetricsHost, addr: WorkerCoreMetricsListenAddr()},
	}

	for _, tc := range cases {
		if tc.host != "localhost" {
			t.Fatalf("%s metrics_host default = %q, want localhost", tc.name, tc.host)
		}
	}

	wantAddr := map[string]string{
		"queue":        "localhost:9081",
		"orchestrator": "localhost:9090",
		"log":          "localhost:9083",
		"artifact":     "localhost:9089",
		"worker":       "localhost:9082",
		"worker-core":  "localhost:9092",
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

func TestArtifactConfig_DefaultAndOverride(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	if got := ArtifactGRPCPort(); got != 8086 {
		t.Fatalf("ArtifactGRPCPort default: got %d", got)
	}

	if got := ArtifactGRPCListenAddr(); got != ":8086" {
		t.Fatalf("ArtifactGRPCListenAddr default: got %q", got)
	}

	if got := ArtifactMetricsHost(); got != "localhost" {
		t.Fatalf("ArtifactMetricsHost default: got %q", got)
	}

	if got := ArtifactMetricsPort(); got != 9089 {
		t.Fatalf("ArtifactMetricsPort default: got %d", got)
	}

	if got := ArtifactMetricsListenAddr(); got != "localhost:9089" {
		t.Fatalf("ArtifactMetricsListenAddr default: got %q", got)
	}

	if got := ArtifactStorageReadOnlyMinFreeBytes(); got != 1<<30 {
		t.Fatalf("ArtifactStorageReadOnlyMinFreeBytes default: got %d", got)
	}

	if !ArtifactRegisterWithRegistry() {
		t.Fatal("ArtifactRegisterWithRegistry default: got false, want true")
	}

	viper.Set("grpc_port", 18086)
	viper.Set("metrics_host", "127.0.0.1")
	viper.Set("metrics_port", 19089)
	viper.Set("storage_read_only_min_free_bytes", uint64(2048))
	viper.Set("artifact.grpc.register_with_registry", false)
	viper.Set("artifact.grpc.advertise_address", "artifact.local:8086")
	viper.Set("artifact.registry.address", "registry.local:8082")
	viper.Set("artifact.grpc.resolver.address", "artifact-pinned:8086")

	if got := ArtifactGRPCPort(); got != 18086 {
		t.Fatalf("ArtifactGRPCPort override: got %d", got)
	}

	if got := ArtifactMetricsListenAddr(); got != "127.0.0.1:19089" {
		t.Fatalf("ArtifactMetricsListenAddr override: got %q", got)
	}

	if got := ArtifactStorageReadOnlyMinFreeBytes(); got != 2048 {
		t.Fatalf("ArtifactStorageReadOnlyMinFreeBytes override: got %d", got)
	}

	if ArtifactRegisterWithRegistry() {
		t.Fatal("ArtifactRegisterWithRegistry override: got true, want false")
	}

	if got := ArtifactGRPCAdvertiseAddress(); got != "artifact.local:8086" {
		t.Fatalf("ArtifactGRPCAdvertiseAddress override: got %q", got)
	}

	if got := ArtifactRegistryAddress(); got != "registry.local:8082" {
		t.Fatalf("ArtifactRegistryAddress override: got %q", got)
	}

	if got := PinnedArtifactAddress(); got != "artifact-pinned:8086" {
		t.Fatalf("PinnedArtifactAddress override: got %q", got)
	}

	viper.Set("artifact.grpc.resolver.address", "")
	viper.Set("discovery.artifact.address", "artifact-discovery:8086")
	if got := PinnedArtifactAddress(); got != "artifact-discovery:8086" {
		t.Fatalf("PinnedArtifactAddress discovery fallback: got %q", got)
	}
}

func TestSourceCheckoutRoot_DefaultAndOverride(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	dataHome := t.TempDir()
	if got, want := SourceCheckoutRoot(dataHome), filepath.Join(dataHome, "vectis", "source-checkouts"); got != want {
		t.Fatalf("SourceCheckoutRoot default: got %q, want %q", got, want)
	}

	viper.Set("source.checkout_root", "{{data_home}}/custom-source")
	if got, want := SourceCheckoutRoot(dataHome), filepath.Join(dataHome, "custom-source"); got != want {
		t.Fatalf("SourceCheckoutRoot viper override: got %q, want %q", got, want)
	}

	t.Setenv(envSourceCheckoutRoot, "{{data_home}}/env-source")
	if got, want := SourceCheckoutRoot(dataHome), filepath.Join(dataHome, "env-source"); got != want {
		t.Fatalf("SourceCheckoutRoot env override: got %q, want %q", got, want)
	}
}

func TestSourceSyncRunningTimeout_DefaultAndOverride(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv(envSourceSyncRunningTimeout, "")
	t.Setenv(envAPIServerSourceSyncRunningTimeout, "")

	if got := SourceSyncRunningTimeout(); got != 15*time.Minute {
		t.Fatalf("SourceSyncRunningTimeout default: got %v", got)
	}

	viper.Set("source.sync_running_timeout", 2*time.Minute)
	if got := SourceSyncRunningTimeout(); got != 2*time.Minute {
		t.Fatalf("SourceSyncRunningTimeout viper override: got %v", got)
	}

	t.Setenv(envSourceSyncRunningTimeout, "30s")
	if got := SourceSyncRunningTimeout(); got != 30*time.Second {
		t.Fatalf("SourceSyncRunningTimeout env override: got %v", got)
	}
}

func TestSourceSyncConfiguredRepositoriesOnStartup_DefaultAndOverride(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv(envSourceSyncConfiguredRepositoriesOnStartup, "")
	t.Setenv(envAPIServerSourceSyncConfiguredRepositoriesOnStartup, "")

	if SourceSyncConfiguredRepositoriesOnStartup() {
		t.Fatal("SourceSyncConfiguredRepositoriesOnStartup default: got true, want false")
	}

	viper.Set("source.sync_configured_repositories_on_startup", true)
	if !SourceSyncConfiguredRepositoriesOnStartup() {
		t.Fatal("SourceSyncConfiguredRepositoriesOnStartup viper override: got false, want true")
	}

	t.Setenv(envSourceSyncConfiguredRepositoriesOnStartup, "false")
	if SourceSyncConfiguredRepositoriesOnStartup() {
		t.Fatal("SourceSyncConfiguredRepositoriesOnStartup env override: got true, want false")
	}

	t.Setenv(envSourceSyncConfiguredRepositoriesOnStartup, "")
	t.Setenv(envAPIServerSourceSyncConfiguredRepositoriesOnStartup, "true")
	if !SourceSyncConfiguredRepositoriesOnStartup() {
		t.Fatal("SourceSyncConfiguredRepositoriesOnStartup API env override: got false, want true")
	}
}

func TestSourceSyncConfiguredRepositoriesInterval_DefaultAndOverride(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv(envSourceSyncConfiguredRepositoriesInterval, "")
	t.Setenv(envAPIServerSourceSyncConfiguredRepositoriesInterval, "")

	if got := SourceSyncConfiguredRepositoriesInterval(); got != 0 {
		t.Fatalf("SourceSyncConfiguredRepositoriesInterval default: got %v, want 0", got)
	}

	viper.Set("source.sync_configured_repositories_interval", 2*time.Minute)
	if got := SourceSyncConfiguredRepositoriesInterval(); got != 2*time.Minute {
		t.Fatalf("SourceSyncConfiguredRepositoriesInterval viper override: got %v", got)
	}

	t.Setenv(envSourceSyncConfiguredRepositoriesInterval, "30s")
	if got := SourceSyncConfiguredRepositoriesInterval(); got != 30*time.Second {
		t.Fatalf("SourceSyncConfiguredRepositoriesInterval env override: got %v", got)
	}

	t.Setenv(envSourceSyncConfiguredRepositoriesInterval, "")
	t.Setenv(envAPIServerSourceSyncConfiguredRepositoriesInterval, "45s")
	if got := SourceSyncConfiguredRepositoriesInterval(); got != 45*time.Second {
		t.Fatalf("SourceSyncConfiguredRepositoriesInterval API env override: got %v", got)
	}
}

func TestSourceSyncConfiguredRepositoriesMaxConcurrency_DefaultAndOverride(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv(envSourceSyncConfiguredRepositoriesMaxConcurrency, "")
	t.Setenv(envAPIServerSourceSyncConfiguredRepositoriesMaxConcurrency, "")

	if got := SourceSyncConfiguredRepositoriesMaxConcurrency(); got != 1 {
		t.Fatalf("SourceSyncConfiguredRepositoriesMaxConcurrency default: got %d, want 1", got)
	}

	viper.Set("source.sync_configured_repositories_max_concurrency", 3)
	if got := SourceSyncConfiguredRepositoriesMaxConcurrency(); got != 3 {
		t.Fatalf("SourceSyncConfiguredRepositoriesMaxConcurrency viper override: got %d", got)
	}

	t.Setenv(envSourceSyncConfiguredRepositoriesMaxConcurrency, "4")
	if got := SourceSyncConfiguredRepositoriesMaxConcurrency(); got != 4 {
		t.Fatalf("SourceSyncConfiguredRepositoriesMaxConcurrency env override: got %d", got)
	}

	t.Setenv(envSourceSyncConfiguredRepositoriesMaxConcurrency, "")
	t.Setenv(envAPIServerSourceSyncConfiguredRepositoriesMaxConcurrency, "5")
	if got := SourceSyncConfiguredRepositoriesMaxConcurrency(); got != 5 {
		t.Fatalf("SourceSyncConfiguredRepositoriesMaxConcurrency API env override: got %d", got)
	}
}

func TestSourceSyncConfiguredRepositoriesFailureBackoff_DefaultAndOverride(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv(envSourceSyncConfiguredRepositoriesFailureBackoff, "")
	t.Setenv(envAPIServerSourceSyncConfiguredRepositoriesFailureBackoff, "")

	if got := SourceSyncConfiguredRepositoriesFailureBackoff(); got != 5*time.Minute {
		t.Fatalf("SourceSyncConfiguredRepositoriesFailureBackoff default: got %v, want 5m", got)
	}

	viper.Set("source.sync_configured_repositories_failure_backoff", 2*time.Minute)
	if got := SourceSyncConfiguredRepositoriesFailureBackoff(); got != 2*time.Minute {
		t.Fatalf("SourceSyncConfiguredRepositoriesFailureBackoff viper override: got %v", got)
	}

	t.Setenv(envSourceSyncConfiguredRepositoriesFailureBackoff, "30s")
	if got := SourceSyncConfiguredRepositoriesFailureBackoff(); got != 30*time.Second {
		t.Fatalf("SourceSyncConfiguredRepositoriesFailureBackoff env override: got %v", got)
	}

	t.Setenv(envSourceSyncConfiguredRepositoriesFailureBackoff, "")
	t.Setenv(envAPIServerSourceSyncConfiguredRepositoriesFailureBackoff, "45s")
	if got := SourceSyncConfiguredRepositoriesFailureBackoff(); got != 45*time.Second {
		t.Fatalf("SourceSyncConfiguredRepositoriesFailureBackoff API env override: got %v", got)
	}
}

func TestSourceRepositoryDeclarations_Viper(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv(envSourceRepositories, "")
	t.Setenv(envAPIServerSourceRepositories, "")

	viper.Set("source.repositories", []map[string]any{
		{
			"repository_id":        " vectis-local ",
			"namespace":            "/team-a",
			"source_kind":          "local_checkout",
			"checkout_path":        " /work/vectis ",
			"checkout_mode":        "external",
			"authoring_mode":       "read_only",
			"worker_cache_mode":    " persistent ",
			"canonical_url":        " https://mirror.invalid/vectis.git ",
			"fallback_remote_urls": []string{" https://tier1.invalid/vectis.git ", "https://tier2.invalid/vectis.git"},
			"default_ref":          "main",
			"enabled":              false,
		},
	})

	repos, err := SourceRepositoryDeclarations()
	if err != nil {
		t.Fatal(err)
	}

	if len(repos) != 1 {
		t.Fatalf("len=%d, want 1", len(repos))
	}

	if repos[0].RepositoryID != "vectis-local" ||
		repos[0].SourceKind != "local_checkout" ||
		repos[0].CheckoutPath != "/work/vectis" ||
		repos[0].CheckoutMode != "external" ||
		repos[0].AuthoringMode != "read_only" ||
		repos[0].WorkerCacheMode != "persistent" ||
		repos[0].Namespace != "/team-a" ||
		repos[0].CanonicalURL != "https://mirror.invalid/vectis.git" ||
		len(repos[0].FallbackRemoteURLs) != 2 ||
		repos[0].FallbackRemoteURLs[0] != "https://tier1.invalid/vectis.git" ||
		repos[0].FallbackRemoteURLs[1] != "https://tier2.invalid/vectis.git" ||
		repos[0].DefaultRef != "main" ||
		repos[0].Enabled == nil ||
		*repos[0].Enabled {
		t.Fatalf("repository declaration mismatch: %+v", repos[0])
	}
}

func TestSourceRepositoryDeclarations_EnvJSON(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv(envAPIServerSourceRepositories, "")
	t.Setenv(envWorkerSourceRepositories, "")
	t.Setenv(envWorkerCoreSourceRepositories, "")
	t.Setenv(envSourceRepositories, `[{"repository_id":"vectis","checkout_mode":"managed","canonical_url":"https://example.invalid/vectis.git","fallback_remote_urls":["https://tier1.invalid/vectis.git"],"enabled":true}]`)

	repos, err := SourceRepositoryDeclarations()
	if err != nil {
		t.Fatal(err)
	}

	if len(repos) != 1 ||
		repos[0].RepositoryID != "vectis" ||
		repos[0].CheckoutMode != "managed" ||
		repos[0].WorkerCacheMode != "ephemeral" ||
		len(repos[0].FallbackRemoteURLs) != 1 ||
		repos[0].FallbackRemoteURLs[0] != "https://tier1.invalid/vectis.git" ||
		repos[0].Enabled == nil ||
		!*repos[0].Enabled {
		t.Fatalf("repository declarations mismatch: %+v", repos)
	}
}

func TestSourceRepositoryDeclarations_WorkerCoreEnvJSON(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv(envSourceRepositories, "")
	t.Setenv(envAPIServerSourceRepositories, "")
	t.Setenv(envWorkerSourceRepositories, "")
	t.Setenv(envWorkerCoreSourceRepositories, `[{"repository_id":"vectis","checkout_mode":"managed","worker_cache_mode":"persistent","canonical_url":"https://example.invalid/vectis.git"}]`)

	repos, err := SourceRepositoryDeclarations()
	if err != nil {
		t.Fatal(err)
	}

	if len(repos) != 1 ||
		repos[0].RepositoryID != "vectis" ||
		repos[0].WorkerCacheMode != "persistent" ||
		repos[0].CanonicalURL != "https://example.invalid/vectis.git" {
		t.Fatalf("repository declarations mismatch: %+v", repos)
	}
}

func TestSourceRepositoryDeclarations_RejectsInvalid(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv(envSourceRepositories, `[{"repository_id":"vectis","checkout_path":"/work/vectis"},{"repository_id":"vectis","checkout_path":"/work/vectis-2"}]`)
	t.Setenv(envAPIServerSourceRepositories, "")

	if _, err := SourceRepositoryDeclarations(); err == nil {
		t.Fatal("expected duplicate repository error")
	}

	t.Setenv(envSourceRepositories, `[{"repository_id":"vectis","unexpected":true}]`)
	if _, err := SourceRepositoryDeclarations(); err == nil {
		t.Fatal("expected unknown JSON field error")
	}

	t.Setenv(envSourceRepositories, `[{"repository_id":"vectis","checkout_path":"/work/vectis","default_ref":"HEAD~1"}]`)
	if _, err := SourceRepositoryDeclarations(); err == nil {
		t.Fatal("expected invalid default_ref error")
	}

	t.Setenv(envSourceRepositories, `[{"repository_id":"vectis","checkout_path":"/work/vectis","fallback_remote_urls":["-config"]}]`)
	if _, err := SourceRepositoryDeclarations(); err == nil || !strings.Contains(err.Error(), "fallback_remote_urls") {
		t.Fatalf("expected invalid fallback_remote_urls error, got %v", err)
	}

	t.Setenv(envSourceRepositories, `[{"repository_id":"vectis","source_kind":"archive","checkout_path":"/work/vectis"}]`)
	if _, err := SourceRepositoryDeclarations(); err == nil {
		t.Fatal("expected unsupported source_kind error")
	}

	t.Setenv(envSourceRepositories, `[{"repository_id":"vectis","checkout_mode":"magic","checkout_path":"/work/vectis"}]`)
	if _, err := SourceRepositoryDeclarations(); err == nil {
		t.Fatal("expected unsupported checkout_mode error")
	}

	t.Setenv(envSourceRepositories, `[{"repository_id":"vectis","authoring_mode":"magic","checkout_path":"/work/vectis"}]`)
	if _, err := SourceRepositoryDeclarations(); err == nil {
		t.Fatal("expected unsupported authoring_mode error")
	}

	t.Setenv(envSourceRepositories, `[{"repository_id":"vectis","worker_cache_mode":"magic","checkout_path":"/work/vectis"}]`)
	if _, err := SourceRepositoryDeclarations(); err == nil {
		t.Fatal("expected unsupported worker_cache_mode error")
	}

	t.Setenv(envSourceRepositories, `[{"repository_id":"vectis","checkout_mode":"external","authoring_mode":"local_commit","checkout_path":"/work/vectis"}]`)
	if _, err := SourceRepositoryDeclarations(); err == nil {
		t.Fatal("expected incompatible source authoring mode error")
	}

	t.Setenv(envSourceRepositories, `[{"repository_id":"vectis"}]`)
	if _, err := SourceRepositoryDeclarations(); err == nil {
		t.Fatal("expected missing checkout_path error")
	}
}

func TestSourceScheduleDeclarations_Viper(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv(envSourceSchedules, "")
	t.Setenv(envAPIServerSourceSchedules, "")

	viper.Set("source.schedules", []map[string]any{
		{
			"schedule_id":   " nightly-build ",
			"repository_id": " vectis ",
			"job_id":        " build ",
			"cron_spec":     " 0 * * * * ",
			"ref":           " main ",
			"path":          " .vectis/jobs/build.json ",
			"enabled":       false,
		},
	})

	schedules, err := SourceScheduleDeclarations()
	if err != nil {
		t.Fatal(err)
	}

	if len(schedules) != 1 {
		t.Fatalf("len=%d, want 1", len(schedules))
	}

	if schedules[0].ScheduleID != "nightly-build" ||
		schedules[0].RepositoryID != "vectis" ||
		schedules[0].JobID != "build" ||
		schedules[0].CronSpec != "0 * * * *" ||
		schedules[0].Ref != "main" ||
		schedules[0].Path != ".vectis/jobs/build.json" ||
		schedules[0].Enabled == nil ||
		*schedules[0].Enabled {
		t.Fatalf("schedule declaration mismatch: %+v", schedules[0])
	}
}

func TestSourceScheduleDeclarations_EnvJSON(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv(envAPIServerSourceSchedules, "")
	t.Setenv(envSourceSchedules, `[{"schedule_id":"nightly","repository_id":"vectis","job_id":"build","cron_spec":"0 * * * *","enabled":true}]`)

	schedules, err := SourceScheduleDeclarations()
	if err != nil {
		t.Fatal(err)
	}

	if len(schedules) != 1 ||
		schedules[0].ScheduleID != "nightly" ||
		schedules[0].RepositoryID != "vectis" ||
		schedules[0].JobID != "build" ||
		schedules[0].Enabled == nil ||
		!*schedules[0].Enabled {
		t.Fatalf("source schedule declarations mismatch: %+v", schedules)
	}
}

func TestSourceScheduleDeclarations_RejectsInvalid(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv(envSourceSchedules, `[{"schedule_id":"nightly","repository_id":"vectis","job_id":"build","cron_spec":"0 * * * *"},{"schedule_id":"nightly","repository_id":"vectis","job_id":"deploy","cron_spec":"0 * * * *"}]`)
	t.Setenv(envAPIServerSourceSchedules, "")

	if _, err := SourceScheduleDeclarations(); err == nil {
		t.Fatal("expected duplicate source schedule error")
	}

	t.Setenv(envSourceSchedules, `[{"schedule_id":"nightly","repository_id":"vectis","job_id":"build","cron_spec":"0 * * * *","unexpected":true}]`)
	if _, err := SourceScheduleDeclarations(); err == nil {
		t.Fatal("expected unknown JSON field error")
	}

	t.Setenv(envSourceSchedules, `[{"schedule_id":"nightly","repository_id":"vectis","job_id":"build"}]`)
	if _, err := SourceScheduleDeclarations(); err == nil {
		t.Fatal("expected missing cron_spec error")
	}

	t.Setenv(envSourceSchedules, `[{"schedule_id":"nightly","repository_id":"vectis","job_id":"build","cron_spec":"0 * * * *","ref":"HEAD~1"}]`)
	if _, err := SourceScheduleDeclarations(); err == nil {
		t.Fatal("expected invalid ref error")
	}

	t.Setenv(envSourceSchedules, `[{"schedule_id":"nightly","repository_id":"vectis","job_id":"build","cron_spec":"0 * * * *","path":"../build.json"}]`)
	if _, err := SourceScheduleDeclarations(); err == nil {
		t.Fatal("expected invalid path error")
	}

	t.Setenv(envSourceSchedules, `[{"schedule_id":"nightly","repository_id":"vectis","job_id":"team/build","cron_spec":"0 * * * *"}]`)
	if _, err := SourceScheduleDeclarations(); err == nil {
		t.Fatal("expected invalid derived job path error")
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

func TestPublicAPIBaseURLUsesEffectiveAPIAddress(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	if got := PublicAPIBaseURL(); got != "http://localhost:8080" {
		t.Fatalf("PublicAPIBaseURL default: got %q", got)
	}

	viper.Set("api.host", "127.0.0.1")
	viper.Set("api.port", 18080)
	if got := PublicAPIBaseURL(); got != "http://127.0.0.1:18080" {
		t.Fatalf("PublicAPIBaseURL api override: got %q", got)
	}

	viper.Set("host", "0.0.0.0")
	if got := PublicAPIBaseURL(); got != "http://localhost:18080" {
		t.Fatalf("PublicAPIBaseURL unspecified bind host: got %q", got)
	}

	viper.Set("port", 19080)
	if got := PublicAPIBaseURL(); got != "http://localhost:19080" {
		t.Fatalf("PublicAPIBaseURL flat port override: got %q", got)
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
	if got := WorkerExecutionCheckoutCacheRoot(); got != "" {
		t.Fatalf("default checkout cache root = %q, want empty", got)
	}
	if got := WorkerExecutionCheckoutCacheGenerationsToKeep(); got != 2 {
		t.Fatalf("default checkout cache generations to keep = %d, want 2", got)
	}
	if got := WorkerExecutionCheckoutCacheLeaseTTL(); got != time.Hour {
		t.Fatalf("default checkout cache lease TTL = %v, want 1h", got)
	}
	if got := WorkerExecutionCheckoutCacheMaxBytes(); got != 0 {
		t.Fatalf("default checkout cache max bytes = %d, want 0", got)
	}
	if got := WorkerExecutionCheckoutCacheWarmInterval(); got != 5*time.Minute {
		t.Fatalf("default checkout cache warm interval = %v, want 5m", got)
	}
	if got := WorkerExecutionCheckoutCacheWarmTimeout(); got != 30*time.Minute {
		t.Fatalf("default checkout cache warm timeout = %v, want 30m", got)
	}
	if got := WorkerExecutionCheckoutCacheWarmJitterRatio(); got != 0.2 {
		t.Fatalf("default checkout cache warm jitter ratio = %v, want 0.2", got)
	}
	if got := WorkerExecutionCheckoutCacheWarmParallelism(); got != 1 {
		t.Fatalf("default checkout cache warm parallelism = %d, want 1", got)
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
	viper.Set("worker.execution.checkout_cache_root", "/Users/me/vectis-cache")
	viper.Set("worker.execution.checkout_cache_generations_to_keep", 5)
	viper.Set("worker.execution.checkout_cache_lease_ttl", 90*time.Minute)
	viper.Set("worker.execution.checkout_cache_max_bytes", int64(1<<30))
	viper.Set("worker.execution.checkout_cache_warm_interval", 10*time.Minute)
	viper.Set("worker.execution.checkout_cache_warm_timeout", 45*time.Minute)
	viper.Set("worker.execution.checkout_cache_warm_jitter_ratio", 0.5)
	viper.Set("worker.execution.checkout_cache_warm_parallelism", 4)
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
	if got := WorkerExecutionCheckoutCacheRoot(); got != "/Users/me/vectis-cache" {
		t.Fatalf("override checkout cache root = %q", got)
	}
	if got := WorkerExecutionCheckoutCacheGenerationsToKeep(); got != 5 {
		t.Fatalf("override checkout cache generations to keep = %d", got)
	}
	if got := WorkerExecutionCheckoutCacheLeaseTTL(); got != 90*time.Minute {
		t.Fatalf("override checkout cache lease TTL = %v", got)
	}
	if got := WorkerExecutionCheckoutCacheMaxBytes(); got != 1<<30 {
		t.Fatalf("override checkout cache max bytes = %d", got)
	}
	if got := WorkerExecutionCheckoutCacheWarmInterval(); got != 10*time.Minute {
		t.Fatalf("override checkout cache warm interval = %v", got)
	}
	if got := WorkerExecutionCheckoutCacheWarmTimeout(); got != 45*time.Minute {
		t.Fatalf("override checkout cache warm timeout = %v", got)
	}
	if got := WorkerExecutionCheckoutCacheWarmJitterRatio(); got != 0.5 {
		t.Fatalf("override checkout cache warm jitter ratio = %v", got)
	}
	if got := WorkerExecutionCheckoutCacheWarmParallelism(); got != 4 {
		t.Fatalf("override checkout cache warm parallelism = %d, want 4", got)
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

	viper.Set("worker.execution.checkout_cache_generations_to_keep", 0)
	if got := WorkerExecutionCheckoutCacheGenerationsToKeep(); got != 2 {
		t.Fatalf("invalid checkout cache generations to keep should use default: got %d", got)
	}
	viper.Set("worker.execution.checkout_cache_lease_ttl", time.Duration(0))
	if got := WorkerExecutionCheckoutCacheLeaseTTL(); got != time.Hour {
		t.Fatalf("invalid checkout cache lease TTL should use default: got %v", got)
	}
	viper.Set("worker.execution.checkout_cache_max_bytes", int64(-1))
	if got := WorkerExecutionCheckoutCacheMaxBytes(); got != 0 {
		t.Fatalf("invalid checkout cache max bytes should use default: got %d", got)
	}
	viper.Set("worker.execution.checkout_cache_warm_interval", time.Duration(0))
	if got := WorkerExecutionCheckoutCacheWarmInterval(); got != 5*time.Minute {
		t.Fatalf("invalid checkout cache warm interval should use default: got %v", got)
	}
	viper.Set("worker.execution.checkout_cache_warm_timeout", time.Duration(0))
	if got := WorkerExecutionCheckoutCacheWarmTimeout(); got != 30*time.Minute {
		t.Fatalf("invalid checkout cache warm timeout should use default: got %v", got)
	}
	viper.Set("worker.execution.checkout_cache_warm_jitter_ratio", 2.0)
	if got := WorkerExecutionCheckoutCacheWarmJitterRatio(); got != 0.2 {
		t.Fatalf("invalid checkout cache warm jitter ratio should use default: got %v", got)
	}
	viper.Set("worker.execution.checkout_cache_warm_parallelism", 0)
	if got := WorkerExecutionCheckoutCacheWarmParallelism(); got != 1 {
		t.Fatalf("invalid checkout cache warm parallelism should use default: got %d", got)
	}
}

func TestWorkerArtifactMaxBytes_DefaultAndOverride(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	if got := WorkerArtifactMaxBytes(); got != 1<<30 {
		t.Fatalf("WorkerArtifactMaxBytes default: got %d", got)
	}

	if got := WorkerArtifactMaxRunBytes(); got != 10<<30 {
		t.Fatalf("WorkerArtifactMaxRunBytes default: got %d", got)
	}

	if got := WorkerArtifactMaxCount(); got != 1000 {
		t.Fatalf("WorkerArtifactMaxCount default: got %d", got)
	}

	if got := WorkerQueueDequeueStickySuccessBudget(); got != 64 {
		t.Fatalf("WorkerQueueDequeueStickySuccessBudget default: got %d", got)
	}

	if got := WorkerQueueDequeuePollBaseInterval(); got != 250*time.Millisecond {
		t.Fatalf("WorkerQueueDequeuePollBaseInterval default: got %v", got)
	}

	if got := WorkerQueueDequeuePollMaxInterval(); got != time.Second {
		t.Fatalf("WorkerQueueDequeuePollMaxInterval default: got %v", got)
	}

	if got := WorkerQueueDequeuePollJitterRatio(); got != 0.2 {
		t.Fatalf("WorkerQueueDequeuePollJitterRatio default: got %v", got)
	}

	if got := WorkerQueueContinuationInlineJobMaxBytes(); got != 65536 {
		t.Fatalf("WorkerQueueContinuationInlineJobMaxBytes default: got %d", got)
	}

	viper.Set("worker.artifact_max_bytes", int64(4096))
	if got := WorkerArtifactMaxBytes(); got != 4096 {
		t.Fatalf("WorkerArtifactMaxBytes namespaced override: got %d", got)
	}

	viper.Set("worker.artifact_max_run_bytes", int64(8192))
	if got := WorkerArtifactMaxRunBytes(); got != 8192 {
		t.Fatalf("WorkerArtifactMaxRunBytes namespaced override: got %d", got)
	}

	viper.Set("worker.artifact_max_count", int64(12))
	if got := WorkerArtifactMaxCount(); got != 12 {
		t.Fatalf("WorkerArtifactMaxCount namespaced override: got %d", got)
	}

	viper.Set("worker.queue.dequeue_sticky_success_budget", 7)
	if got := WorkerQueueDequeueStickySuccessBudget(); got != 7 {
		t.Fatalf("WorkerQueueDequeueStickySuccessBudget namespaced override: got %d", got)
	}

	viper.Set("worker.queue.dequeue_poll_base_interval", 100*time.Millisecond)
	if got := WorkerQueueDequeuePollBaseInterval(); got != 100*time.Millisecond {
		t.Fatalf("WorkerQueueDequeuePollBaseInterval namespaced override: got %v", got)
	}

	viper.Set("worker.queue.dequeue_poll_max_interval", 2*time.Second)
	if got := WorkerQueueDequeuePollMaxInterval(); got != 2*time.Second {
		t.Fatalf("WorkerQueueDequeuePollMaxInterval namespaced override: got %v", got)
	}

	viper.Set("worker.queue.dequeue_poll_jitter_ratio", 0)
	if got := WorkerQueueDequeuePollJitterRatio(); got != 0 {
		t.Fatalf("WorkerQueueDequeuePollJitterRatio zero override: got %v", got)
	}

	viper.Set("worker.queue.dequeue_poll_jitter_ratio", 0.5)
	if got := WorkerQueueDequeuePollJitterRatio(); got != 0.5 {
		t.Fatalf("WorkerQueueDequeuePollJitterRatio namespaced override: got %v", got)
	}

	viper.Set("worker.queue.continuation_inline_job_max_bytes", int64(32768))
	if got := WorkerQueueContinuationInlineJobMaxBytes(); got != 32768 {
		t.Fatalf("WorkerQueueContinuationInlineJobMaxBytes namespaced override: got %d", got)
	}

	viper.Set("worker.queue.continuation_inline_job_max_bytes", int64(0))
	if got := WorkerQueueContinuationInlineJobMaxBytes(); got != 0 {
		t.Fatalf("WorkerQueueContinuationInlineJobMaxBytes zero override should disable inline deliveries: got %d", got)
	}

	viper.Set("worker.queue.continuation_inline_job_max_bytes", int64(-1))
	if got := WorkerQueueContinuationInlineJobMaxBytes(); got != 0 {
		t.Fatalf("WorkerQueueContinuationInlineJobMaxBytes negative override should clamp to zero: got %d", got)
	}

	viper.Set("worker.queue.dequeue_sticky_success_budget", 0)
	if got := WorkerQueueDequeueStickySuccessBudget(); got != 64 {
		t.Fatalf("WorkerQueueDequeueStickySuccessBudget invalid override should use default: got %d", got)
	}

	viper.Set("worker.queue.dequeue_poll_base_interval", time.Duration(0))
	if got := WorkerQueueDequeuePollBaseInterval(); got != 250*time.Millisecond {
		t.Fatalf("WorkerQueueDequeuePollBaseInterval invalid override should use default: got %v", got)
	}

	viper.Set("worker.queue.dequeue_poll_max_interval", time.Duration(0))
	if got := WorkerQueueDequeuePollMaxInterval(); got != time.Second {
		t.Fatalf("WorkerQueueDequeuePollMaxInterval invalid override should use default: got %v", got)
	}

	viper.Set("worker.queue.dequeue_poll_jitter_ratio", 1.5)
	if got := WorkerQueueDequeuePollJitterRatio(); got != 0.2 {
		t.Fatalf("WorkerQueueDequeuePollJitterRatio invalid override should use default: got %v", got)
	}

	viper.Set("worker.queue.dequeue_poll_base_interval", 2*time.Second)
	viper.Set("worker.queue.dequeue_poll_max_interval", time.Second)
	if got := WorkerQueueDequeuePollMaxInterval(); got != 2*time.Second {
		t.Fatalf("WorkerQueueDequeuePollMaxInterval below base should clamp to base: got %v", got)
	}

	viper.Set("worker.artifact_max_bytes", int64(0))
	if got := WorkerArtifactMaxBytes(); got != 0 {
		t.Fatalf("WorkerArtifactMaxBytes should allow disabling limit: got %d", got)
	}

	viper.Set("worker.artifact_max_run_bytes", int64(0))
	if got := WorkerArtifactMaxRunBytes(); got != 0 {
		t.Fatalf("WorkerArtifactMaxRunBytes should allow disabling limit: got %d", got)
	}

	viper.Set("worker.artifact_max_count", int64(0))
	if got := WorkerArtifactMaxCount(); got != 0 {
		t.Fatalf("WorkerArtifactMaxCount should allow disabling limit: got %d", got)
	}

	viper.Set("worker.artifact_max_bytes", int64(-1))
	if got := WorkerArtifactMaxBytes(); got != 0 {
		t.Fatalf("WorkerArtifactMaxBytes negative override should disable limit: got %d", got)
	}

	viper.Set("worker.artifact_max_run_bytes", int64(-1))
	if got := WorkerArtifactMaxRunBytes(); got != 0 {
		t.Fatalf("WorkerArtifactMaxRunBytes negative override should disable limit: got %d", got)
	}

	viper.Set("worker.artifact_max_count", int64(-1))
	if got := WorkerArtifactMaxCount(); got != 0 {
		t.Fatalf("WorkerArtifactMaxCount negative override should disable limit: got %d", got)
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
