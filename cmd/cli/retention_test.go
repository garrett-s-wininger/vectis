package main

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"vectis/internal/retention"
)

func TestRetentionCleanupRequiresBackupManifestForBackupExpect(t *testing.T) {
	err := retentionCleanup(context.Background(), io.Discard, retention.Policy{}, true, false, "", "", retentionBackupCheckOptions{ExpectPath: "expected-topology.json"})
	if err == nil {
		t.Fatalf("retention cleanup succeeded unexpectedly")
	}
	if !strings.Contains(err.Error(), "--backup-manifest") {
		t.Fatalf("retention cleanup error = %v, want --backup-manifest", err)
	}
}

func TestCheckRetentionBackupManifestRejectsVerificationFailures(t *testing.T) {
	now := time.Date(2026, 6, 28, 16, 0, 0, 0, time.UTC)
	manifest := retentionBackupManifestForTest(now.Add(-30 * time.Minute).Format(time.RFC3339))
	manifest.RequiredPaths[1].Exists = false
	manifest.RequiredPaths[1].Readable = false

	root := t.TempDir()
	manifestPath := writeBackupJSONFile(t, root, "backup-manifest.json", manifest)

	evidence, err := checkRetentionBackupManifest(retentionBackupCheckOptions{ManifestPath: manifestPath}, now)
	if err == nil {
		t.Fatalf("backup manifest check succeeded unexpectedly")
	}
	if evidence == nil || evidence.Verified {
		t.Fatalf("backup manifest evidence = %+v, want unverified evidence", evidence)
	}
	if !strings.Contains(err.Error(), "verification failed") {
		t.Fatalf("backup manifest check error = %v, want verification failure", err)
	}
}

func TestCheckRetentionBackupManifestRejectsStaleManifest(t *testing.T) {
	now := time.Date(2026, 6, 28, 16, 0, 0, 0, time.UTC)
	manifest := retentionBackupManifestForTest(now.Add(-2 * time.Hour).Format(time.RFC3339))

	root := t.TempDir()
	manifestPath := writeBackupJSONFile(t, root, "backup-manifest.json", manifest)

	evidence, err := checkRetentionBackupManifest(retentionBackupCheckOptions{ManifestPath: manifestPath, MaxAge: time.Hour}, now)
	if err == nil {
		t.Fatalf("backup manifest check succeeded unexpectedly")
	}
	if evidence == nil || evidence.Age != "2h0m0s" {
		t.Fatalf("backup manifest evidence = %+v, want 2h age", evidence)
	}
	if !strings.Contains(err.Error(), "stale") {
		t.Fatalf("backup manifest check error = %v, want stale", err)
	}
}

func TestCheckRetentionBackupManifestAcceptsFreshExpectedTopology(t *testing.T) {
	now := time.Date(2026, 6, 28, 16, 0, 0, 0, time.UTC)
	manifest := retentionBackupManifestForTest(now.Add(-30 * time.Minute).Format(time.RFC3339))
	expected := backupExpectedTopology{
		SchemaVersion:    backupExpectedTopologySchemaVersion,
		InventorySources: []string{"host-a.inventory.json"},
		DatabaseRoles: []backupExpectedDatabaseRole{
			{InventorySource: "host-a.inventory.json", Role: "default", Driver: "sqlite3"},
		},
		Paths: []backupExpectedPath{
			{InventorySource: "host-a.inventory.json", Category: "local_state", ID: "queue.persistence", Path: "/var/lib/vectis/queue"},
		},
		RequireCategories: []string{"secret_stores", "tls_files", "config_paths"},
	}

	root := t.TempDir()
	manifestPath := writeBackupJSONFile(t, root, "backup-manifest.json", manifest)
	expectPath := writeBackupJSONFile(t, root, "expected-topology.json", expected)

	evidence, err := checkRetentionBackupManifest(retentionBackupCheckOptions{ManifestPath: manifestPath, ExpectPath: expectPath, MaxAge: 24 * time.Hour}, now)
	if err != nil {
		t.Fatalf("backup manifest check: %v", err)
	}
	if evidence == nil || !evidence.Verified {
		t.Fatalf("backup manifest evidence = %+v, want verified evidence", evidence)
	}
	if evidence.ExpectationSource != expectPath {
		t.Fatalf("expectation source = %q, want %q", evidence.ExpectationSource, expectPath)
	}
	if evidence.Age != "30m0s" || evidence.MaxAge != "24h0m0s" {
		t.Fatalf("freshness evidence = age %q max %q", evidence.Age, evidence.MaxAge)
	}
}

func retentionBackupManifestForTest(generatedAt string) backupManifest {
	version := 42
	dirty := false
	return backupManifest{
		SchemaVersion: backupManifestSchemaVersion,
		GeneratedAt:   generatedAt,
		Inventories: []backupManifestInventory{
			{Source: "host-a.inventory.json", GeneratedAt: generatedAt, Version: "test", DatabaseDriver: "sqlite3"},
		},
		DatabaseRoles: []backupManifestDatabaseRole{
			{
				InventorySource: "host-a.inventory.json",
				Role:            "default",
				Driver:          "sqlite3",
				DSN:             "sqlite.db",
				DSNSource:       "VECTIS_DATABASE_DSN",
				LocalPath:       "sqlite.db",
				Schema:          backupSchemaInventory{Inspectable: true, CurrentVersion: &version, Dirty: &dirty},
			},
		},
		RequiredPaths: []backupManifestPath{
			{InventorySource: "host-a.inventory.json", Category: "database", ID: "database.default", Kind: "file", Path: "sqlite.db", Enabled: true, Exists: true, Readable: true},
			{InventorySource: "host-a.inventory.json", Category: "local_state", ID: "queue.persistence", Kind: "directory", Path: "/var/lib/vectis/queue", Enabled: true, Exists: true, Readable: true},
			{InventorySource: "host-a.inventory.json", Category: "local_state", ID: "log.storage", Kind: "directory", Path: "/var/lib/vectis/log", Enabled: true, Exists: true, Readable: true},
			{InventorySource: "host-a.inventory.json", Category: "local_state", ID: "artifact.storage", Kind: "directory", Path: "/var/lib/vectis/artifact", Enabled: true, Exists: true, Readable: true},
			{InventorySource: "host-a.inventory.json", Category: "secret_stores", ID: "secrets.encryptedfs.root", Kind: "directory", Path: "/var/lib/vectis/secrets", Enabled: true, Exists: true, Readable: true},
			{InventorySource: "host-a.inventory.json", Category: "tls_files", ID: "grpc.cert_file", Kind: "file", Path: "/etc/vectis/tls/grpc.crt", Enabled: true, Exists: true, Readable: true},
			{InventorySource: "host-a.inventory.json", Category: "config_paths", ID: "deploy.config_dir", Kind: "directory", Path: "/etc/vectis/deploy", Enabled: true, Exists: true, Readable: true},
		},
	}
}
