package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"vectis/internal/database"
	"vectis/internal/storageverify"
)

func TestBackupInventoryJSONIncludesRedactedLocalEvidence(t *testing.T) {
	withOutputFormat(t, outputJSON)

	root := t.TempDir()
	dataHome := filepath.Join(root, "data")
	tmpDir := filepath.Join(root, "tmp")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		t.Fatalf("mkdir tmp: %v", err)
	}

	dbPath := filepath.Join(root, "db", "vectis.db")
	t.Setenv("XDG_DATA_HOME", dataHome)
	t.Setenv("TMPDIR", tmpDir)
	t.Setenv(database.EnvDatabaseDriver, "sqlite3")
	t.Setenv(database.EnvDatabaseDSN, dbPath)

	if err := database.Migrate(dbPath); err != nil {
		t.Fatalf("migrate test db: %v", err)
	}

	queueDir := filepath.Join(root, "queue")
	logDir := filepath.Join(root, "log")
	artifactDir := filepath.Join(root, "artifact")
	spoolDir := filepath.Join(root, "spool")
	sourceRoot := filepath.Join(root, "sources")
	secretRoot := filepath.Join(root, "secrets")
	keyFile := filepath.Join(root, "keys", "encryptedfs.key")
	tlsCert := filepath.Join(root, "tls", "grpc.crt")
	for _, dir := range []string{queueDir, logDir, artifactDir, spoolDir, sourceRoot, secretRoot, filepath.Dir(keyFile), filepath.Dir(tlsCert)} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}

	if err := os.WriteFile(keyFile, []byte("0123456789abcdef0123456789abcdef"), 0o600); err != nil {
		t.Fatalf("write key file: %v", err)
	}

	if err := os.WriteFile(tlsCert, []byte("cert"), 0o644); err != nil {
		t.Fatalf("write cert file: %v", err)
	}

	t.Setenv("VECTIS_QUEUE_INSTANCE_ID", "queue-a")
	t.Setenv("VECTIS_QUEUE_PERSISTENCE_DIR", queueDir)
	t.Setenv("VECTIS_LOG_INSTANCE_ID", "log-a")
	t.Setenv("VECTIS_LOG_STORAGE_DIR", logDir)
	t.Setenv("VECTIS_ARTIFACT_INSTANCE_ID", "artifact-a")
	t.Setenv("VECTIS_ARTIFACT_STORAGE_DIR", artifactDir)
	t.Setenv("VECTIS_LOG_FORWARDER_SPOOL_DIR", spoolDir)
	t.Setenv("VECTIS_SOURCE_CHECKOUT_ROOT", sourceRoot)
	t.Setenv("VECTIS_SECRETS_ENCRYPTEDFS_ROOT", secretRoot)
	t.Setenv("VECTIS_SECRETS_ENCRYPTEDFS_KEY_FILE", keyFile)
	t.Setenv("VECTIS_GRPC_TLS_CERT_FILE", tlsCert)

	var buf bytes.Buffer
	when := time.Date(2026, 6, 28, 12, 0, 0, 0, time.UTC)
	if err := writeBackupInventory(&buf, when); err != nil {
		t.Fatalf("write backup inventory: %v", err)
	}

	var inventory backupInventory
	if err := json.Unmarshal(buf.Bytes(), &inventory); err != nil {
		t.Fatalf("inventory JSON: %v\n%s", err, buf.String())
	}

	if inventory.GeneratedAt != "2026-06-28T12:00:00Z" {
		t.Fatalf("generated_at = %q", inventory.GeneratedAt)
	}

	if inventory.Database.Driver != "sqlite3" || len(inventory.Database.Roles) != 3 {
		t.Fatalf("database inventory = %+v", inventory.Database)
	}

	if inventory.Database.Roles[0].LocalPath != dbPath {
		t.Fatalf("default database local path = %q, want %q", inventory.Database.Roles[0].LocalPath, dbPath)
	}

	if inventory.Database.Roles[0].Schema.CurrentVersion == nil || inventory.Database.Roles[0].Schema.Dirty == nil || *inventory.Database.Roles[0].Schema.Dirty {
		t.Fatalf("default database schema = %+v", inventory.Database.Roles[0].Schema)
	}

	local := backupPathsByID(inventory.LocalState)
	for id, wantPath := range map[string]string{
		"queue.persistence":    queueDir,
		"log.storage":          logDir,
		"artifact.storage":     artifactDir,
		"log_forwarder.spool":  spoolDir,
		"source.checkout_root": sourceRoot,
	} {
		got := local[id]
		if got.Path != wantPath || !got.Readable {
			t.Fatalf("%s = %+v, want path=%s readable", id, got, wantPath)
		}
	}

	secrets := backupPathsByID(inventory.SecretStores)
	if secrets["secrets.encryptedfs.root"].Path != secretRoot || secrets["secrets.encryptedfs.key_file"].Path != keyFile {
		t.Fatalf("secret stores = %+v", inventory.SecretStores)
	}

	tls := backupPathsByID(inventory.TLSFiles)
	if tls["grpc.cert_file"].Path != tlsCert {
		t.Fatalf("TLS files = %+v", inventory.TLSFiles)
	}
}

func TestRedactDSN(t *testing.T) {
	tests := []string{
		"postgres://vectis:hunter2@db.example/vectis?sslmode=require&sslpassword=secret&token=abc",
		"host=db.example user=vectis password=hunter2 sslkey=/private/key.pem dbname=vectis",
	}

	for _, raw := range tests {
		got := redactDSN(raw)
		for _, leak := range []string{"hunter2", "secret", "abc", "/private/key.pem"} {
			if strings.Contains(got, leak) {
				t.Fatalf("redactDSN(%q) leaked %q as %q", raw, leak, got)
			}
		}

		if !strings.Contains(got, "REDACTED") {
			t.Fatalf("redactDSN(%q) = %q, want REDACTED marker", raw, got)
		}
	}
}

func TestBackupManifestAggregatesInventoryFiles(t *testing.T) {
	withOutputFormat(t, outputJSON)

	root := t.TempDir()
	version := 42
	dirty := false
	inventory := backupInventory{
		GeneratedAt: "2026-06-28T12:00:00Z",
		Version:     "test-version",
		Database: backupDatabaseInventory{
			Driver: "sqlite3",
			Roles: []backupDatabaseRoleInventory{
				{
					Role:      "default",
					DSN:       filepath.Join(root, "vectis.db"),
					DSNSource: database.EnvDatabaseDSN,
					LocalPath: filepath.Join(root, "vectis.db"),
					Schema: backupSchemaInventory{
						Inspectable:    true,
						CurrentVersion: &version,
						Dirty:          &dirty,
					},
				},
			},
		},
		Instances: []backupInstanceInventory{
			{Service: "queue", InstanceID: "queue-a", Source: "test"},
			{Service: "log", InstanceID: "log-a", Source: "test"},
			{Service: "artifact", InstanceID: "artifact-a", Source: "test"},
		},
		LocalState: []backupPathInventory{
			{ID: "queue.persistence", Kind: "directory", Path: filepath.Join(root, "queue"), Source: "test", Enabled: true, Exists: true, IsDir: true, Readable: true},
			{ID: "log.storage", Kind: "directory", Path: filepath.Join(root, "log"), Source: "test", Enabled: true, Exists: true, IsDir: true, Readable: true},
			{ID: "artifact.storage", Kind: "directory", Path: filepath.Join(root, "artifact"), Source: "test", Enabled: true, Exists: true, IsDir: true, Readable: true},
		},
		SecretStores: []backupPathInventory{
			{ID: "secrets.encryptedfs.root", Kind: "directory", Path: filepath.Join(root, "secrets"), Source: "test", Enabled: true, Exists: true, IsDir: true, Readable: true},
		},
		TLSFiles: []backupPathInventory{
			{ID: "grpc.cert_file", Kind: "file", Path: filepath.Join(root, "tls", "grpc.crt"), Source: "test", Enabled: true, Exists: true, IsFile: true, Readable: true},
		},
		ConfigPaths: []backupPathInventory{
			{ID: "deploy.config_dir", Kind: "directory", Path: filepath.Join(root, "deploy"), Source: "test", Enabled: true, Exists: true, IsDir: true, Readable: true},
		},
	}

	inventoryPath := writeBackupJSONFile(t, root, "inventory.json", inventory)
	var buf bytes.Buffer
	when := time.Date(2026, 6, 28, 13, 0, 0, 0, time.UTC)
	if err := writeBackupManifest(&buf, []string{inventoryPath}, when); err != nil {
		t.Fatalf("write backup manifest: %v", err)
	}

	var manifest backupManifest
	if err := json.Unmarshal(buf.Bytes(), &manifest); err != nil {
		t.Fatalf("manifest JSON: %v\n%s", err, buf.String())
	}

	if manifest.SchemaVersion != backupManifestSchemaVersion || manifest.GeneratedAt != "2026-06-28T13:00:00Z" {
		t.Fatalf("manifest header = %+v", manifest)
	}

	if len(manifest.Inventories) != 1 || manifest.Inventories[0].Source != inventoryPath {
		t.Fatalf("manifest inventories = %+v", manifest.Inventories)
	}

	if len(manifest.DatabaseRoles) != 1 {
		t.Fatalf("manifest database roles = %+v", manifest.DatabaseRoles)
	}

	paths := backupManifestPathsByCategoryID(manifest.RequiredPaths)
	for key := range map[string]bool{
		"database/database.default":              true,
		"local_state/queue.persistence":          true,
		"local_state/log.storage":                true,
		"local_state/artifact.storage":           true,
		"secret_stores/secrets.encryptedfs.root": true,
		"tls_files/grpc.cert_file":               true,
		"config_paths/deploy.config_dir":         true,
	} {
		if !paths[key].Readable {
			t.Fatalf("manifest path %s = %+v, want readable", key, paths[key])
		}
	}

	expected := &backupExpectedTopologyInput{
		Source: "expected.json",
		Expectations: backupExpectedTopology{
			SchemaVersion:    backupExpectedTopologySchemaVersion,
			InventorySources: []string{inventoryPath},
			DatabaseRoles: []backupExpectedDatabaseRole{
				{InventorySource: inventoryPath, Role: "default", Driver: "sqlite3"},
			},
			Instances: []backupExpectedInstance{
				{InventorySource: inventoryPath, Service: "queue", InstanceID: "queue-a"},
				{InventorySource: inventoryPath, Service: "log", InstanceID: "log-a"},
				{InventorySource: inventoryPath, Service: "artifact", InstanceID: "artifact-a"},
			},
			Paths: []backupExpectedPath{
				{InventorySource: inventoryPath, Category: "local_state", ID: "queue.persistence"},
				{InventorySource: inventoryPath, Category: "local_state", ID: "log.storage"},
				{InventorySource: inventoryPath, Category: "local_state", ID: "artifact.storage"},
			},
			RequireCategories: []string{"database", "local_state", "secret_stores", "tls_files", "config_paths"},
		},
	}

	result := verifyBackupManifest(manifest, expected, when)
	if result.Status != backupManifestStatusOK || len(result.Errors) != 0 {
		t.Fatalf("manifest verification = %+v", result)
	}

	if result.ExpectationSource != "expected.json" || result.Summary.ExpectedInventorySources != 1 || result.Summary.ExpectedInstances != 3 {
		t.Fatalf("expected topology summary = %+v", result)
	}
}

func TestBackupManifestVerificationReportsMissingRequiredPath(t *testing.T) {
	withOutputFormat(t, outputJSON)

	version := 42
	dirty := false
	manifest := backupManifest{
		SchemaVersion: backupManifestSchemaVersion,
		GeneratedAt:   "2026-06-28T13:00:00Z",
		Inventories: []backupManifestInventory{
			{Source: "host-a.json", GeneratedAt: "2026-06-28T12:00:00Z", Version: "test", DatabaseDriver: "sqlite3"},
		},
		DatabaseRoles: []backupManifestDatabaseRole{
			{InventorySource: "host-a.json", Role: "default", Driver: "sqlite3", DSN: "sqlite.db", LocalPath: "sqlite.db", Schema: backupSchemaInventory{Inspectable: true, CurrentVersion: &version, Dirty: &dirty}},
		},
		RequiredPaths: []backupManifestPath{
			{InventorySource: "host-a.json", Category: "database", ID: "database.default", Kind: "file", Path: "sqlite.db", Enabled: true, Exists: true, Readable: true},
			{InventorySource: "host-a.json", Category: "local_state", ID: "queue.persistence", Kind: "directory", Path: "/missing/queue", Enabled: true, Exists: false, Readable: false},
			{InventorySource: "host-a.json", Category: "local_state", ID: "log.storage", Kind: "directory", Path: "/var/lib/vectis/log", Enabled: true, Exists: true, Readable: true},
			{InventorySource: "host-a.json", Category: "local_state", ID: "artifact.storage", Kind: "directory", Path: "/var/lib/vectis/artifact", Enabled: true, Exists: true, Readable: true},
		},
	}

	root := t.TempDir()
	manifestPath := writeBackupJSONFile(t, root, "manifest.json", manifest)
	var buf bytes.Buffer
	when := time.Date(2026, 6, 28, 14, 0, 0, 0, time.UTC)
	err := writeBackupManifestVerification(&buf, manifestPath, "", nil, 0, when)
	if err == nil {
		t.Fatalf("verify backup manifest succeeded unexpectedly")
	}

	var result backupManifestVerification
	if decodeErr := json.Unmarshal(buf.Bytes(), &result); decodeErr != nil {
		t.Fatalf("verification JSON: %v\n%s", decodeErr, buf.String())
	}

	if result.Status != backupManifestStatusFailed {
		t.Fatalf("verification status = %q", result.Status)
	}

	if !backupFindingsContain(result.Errors, "path.missing") {
		t.Fatalf("verification errors = %+v, want path.missing", result.Errors)
	}
}

func TestBackupManifestVerificationReportsMissingExpectedTopology(t *testing.T) {
	withOutputFormat(t, outputJSON)

	version := 42
	dirty := false
	manifest := backupManifest{
		SchemaVersion: backupManifestSchemaVersion,
		GeneratedAt:   "2026-06-28T13:00:00Z",
		Inventories: []backupManifestInventory{
			{Source: "host-a.json", GeneratedAt: "2026-06-28T12:00:00Z", Version: "test", DatabaseDriver: "sqlite3"},
		},
		DatabaseRoles: []backupManifestDatabaseRole{
			{InventorySource: "host-a.json", Role: "default", Driver: "sqlite3", DSN: "sqlite.db", LocalPath: "sqlite.db", Schema: backupSchemaInventory{Inspectable: true, CurrentVersion: &version, Dirty: &dirty}},
		},
		Instances: []backupManifestInstance{
			{InventorySource: "host-a.json", Service: "queue", InstanceID: "queue-a"},
			{InventorySource: "host-a.json", Service: "log", InstanceID: "log-a"},
			{InventorySource: "host-a.json", Service: "artifact", InstanceID: "artifact-a"},
		},
		RequiredPaths: []backupManifestPath{
			{InventorySource: "host-a.json", Category: "database", ID: "database.default", Kind: "file", Path: "sqlite.db", Enabled: true, Exists: true, Readable: true},
			{InventorySource: "host-a.json", Category: "local_state", ID: "queue.persistence", Kind: "directory", Path: "/var/lib/vectis/queue", Enabled: true, Exists: true, Readable: true},
			{InventorySource: "host-a.json", Category: "local_state", ID: "log.storage", Kind: "directory", Path: "/var/lib/vectis/log", Enabled: true, Exists: true, Readable: true},
			{InventorySource: "host-a.json", Category: "local_state", ID: "artifact.storage", Kind: "directory", Path: "/var/lib/vectis/artifact", Enabled: true, Exists: true, Readable: true},
			{InventorySource: "host-a.json", Category: "secret_stores", ID: "secrets.encryptedfs.root", Kind: "directory", Path: "/var/lib/vectis/secrets", Enabled: true, Exists: true, Readable: true},
			{InventorySource: "host-a.json", Category: "tls_files", ID: "grpc.cert_file", Kind: "file", Path: "/etc/vectis/tls/grpc.crt", Enabled: true, Exists: true, Readable: true},
			{InventorySource: "host-a.json", Category: "config_paths", ID: "deploy.config_dir", Kind: "directory", Path: "/etc/vectis/deploy", Enabled: true, Exists: true, Readable: true},
		},
	}
	expected := backupExpectedTopology{
		SchemaVersion:    backupExpectedTopologySchemaVersion,
		InventorySources: []string{"host-a.json", "host-b.json"},
		Instances: []backupExpectedInstance{
			{Service: "queue", InstanceID: "queue-a"},
			{Service: "log", InstanceID: "log-b"},
		},
		Paths: []backupExpectedPath{
			{InventorySource: "host-b.json", Category: "local_state", ID: "queue.persistence"},
		},
		RequireCategories: []string{"secret_stores", "tls_files", "config_paths"},
	}

	root := t.TempDir()
	manifestPath := writeBackupJSONFile(t, root, "manifest.json", manifest)
	expectPath := writeBackupJSONFile(t, root, "expected.json", expected)
	var buf bytes.Buffer
	when := time.Date(2026, 6, 28, 15, 0, 0, 0, time.UTC)
	err := writeBackupManifestVerification(&buf, manifestPath, expectPath, nil, 0, when)
	if err == nil {
		t.Fatalf("verify backup manifest succeeded unexpectedly")
	}

	var result backupManifestVerification
	if decodeErr := json.Unmarshal(buf.Bytes(), &result); decodeErr != nil {
		t.Fatalf("verification JSON: %v\n%s", decodeErr, buf.String())
	}

	if result.Status != backupManifestStatusFailed || result.ExpectationSource != expectPath {
		t.Fatalf("verification result = %+v", result)
	}
	for _, id := range []string{"expectation.inventory_missing", "expectation.instance_missing", "expectation.path_missing"} {
		if !backupFindingsContain(result.Errors, id) {
			t.Fatalf("verification errors = %+v, want %s", result.Errors, id)
		}
	}
}

func TestBackupManifestVerificationAcceptsMatchingStorageReports(t *testing.T) {
	withOutputFormat(t, outputJSON)

	now := time.Date(2026, 6, 28, 16, 0, 0, 0, time.UTC)
	manifest := retentionBackupManifestForTest(now.Add(-30 * time.Minute).Format(time.RFC3339))

	root := t.TempDir()
	manifestPath := writeBackupJSONFile(t, root, "manifest.json", manifest)
	reportPaths := []string{
		writeBackupJSONFile(t, root, "queue.report.json", backupStorageReportForTest(storageverify.SurfaceQueue, "/var/lib/vectis/queue", now.Add(-5*time.Minute))),
		writeBackupJSONFile(t, root, "logs.report.json", backupStorageReportForTest(storageverify.SurfaceLogs, "/var/lib/vectis/log", now.Add(-5*time.Minute))),
		writeBackupJSONFile(t, root, "artifact.report.json", backupStorageReportForTest(storageverify.SurfaceArtifact, "/var/lib/vectis/artifact", now.Add(-5*time.Minute))),
	}

	var buf bytes.Buffer
	if err := writeBackupManifestVerification(&buf, manifestPath, "", reportPaths, time.Hour, now); err != nil {
		t.Fatalf("verify backup manifest with storage reports: %v\n%s", err, buf.String())
	}

	var result backupManifestVerification
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("verification JSON: %v\n%s", err, buf.String())
	}

	if result.Status != backupManifestStatusOK {
		t.Fatalf("verification result = %+v", result)
	}

	if result.Summary.StorageReports != 3 || result.Summary.StorageReportsVerified != 3 || result.Summary.StoragePathsRequired != 3 {
		t.Fatalf("storage summary = %+v", result.Summary)
	}

	if result.StorageReportMaxAge != "1h0m0s" || len(result.StorageReports) != 3 {
		t.Fatalf("storage evidence = max_age %q reports %+v", result.StorageReportMaxAge, result.StorageReports)
	}
}

func TestBackupManifestVerificationRequiresStorageReportsForAllStoragePaths(t *testing.T) {
	withOutputFormat(t, outputJSON)

	now := time.Date(2026, 6, 28, 16, 0, 0, 0, time.UTC)
	manifest := retentionBackupManifestForTest(now.Add(-30 * time.Minute).Format(time.RFC3339))

	root := t.TempDir()
	manifestPath := writeBackupJSONFile(t, root, "manifest.json", manifest)
	reportPath := writeBackupJSONFile(t, root, "queue.report.json", backupStorageReportForTest(storageverify.SurfaceQueue, "/var/lib/vectis/queue", now.Add(-5*time.Minute)))

	var buf bytes.Buffer
	err := writeBackupManifestVerification(&buf, manifestPath, "", []string{reportPath}, time.Hour, now)
	if err == nil {
		t.Fatalf("verify backup manifest succeeded unexpectedly")
	}

	var result backupManifestVerification
	if decodeErr := json.Unmarshal(buf.Bytes(), &result); decodeErr != nil {
		t.Fatalf("verification JSON: %v\n%s", decodeErr, buf.String())
	}

	if result.Status != backupManifestStatusFailed {
		t.Fatalf("verification status = %q", result.Status)
	}

	if !backupFindingsContain(result.Errors, "storage_report.missing") {
		t.Fatalf("verification errors = %+v, want storage_report.missing", result.Errors)
	}

	if result.Summary.StorageReportsVerified != 1 || result.Summary.StoragePathsRequired != 3 {
		t.Fatalf("storage summary = %+v", result.Summary)
	}
}

func TestBackupRestoreValidationIncludesVerifiedManifestAndSmokeRun(t *testing.T) {
	withOutputFormat(t, outputJSON)

	now := time.Date(2026, 6, 28, 17, 0, 0, 0, time.UTC)
	manifest := retentionBackupManifestForTest(now.Add(-30 * time.Minute).Format(time.RFC3339))

	root := t.TempDir()
	manifestPath := writeBackupJSONFile(t, root, "manifest.json", manifest)
	reportPaths := []string{
		writeBackupJSONFile(t, root, "queue.report.json", backupStorageReportForTest(storageverify.SurfaceQueue, "/var/lib/vectis/queue", now.Add(-5*time.Minute))),
		writeBackupJSONFile(t, root, "logs.report.json", backupStorageReportForTest(storageverify.SurfaceLogs, "/var/lib/vectis/log", now.Add(-5*time.Minute))),
		writeBackupJSONFile(t, root, "artifact.report.json", backupStorageReportForTest(storageverify.SurfaceArtifact, "/var/lib/vectis/artifact", now.Add(-5*time.Minute))),
	}

	opts := backupRestoreValidationOptions{
		ManifestPath:       manifestPath,
		StorageReportPaths: reportPaths,
		StorageMaxAge:      time.Hour,
		SmokeRunID:         "run-restore",
		Deployment:         "local",
		Profile:            "simple",
	}

	var buf bytes.Buffer
	if err := writeBackupRestoreValidation(&buf, opts, now, func(runID string) (runDetail, error) {
		if runID != "run-restore" {
			t.Fatalf("fetch run id = %q", runID)
		}
		return runDetail{RunID: runID, RunIndex: 7, Status: "succeeded"}, nil
	}); err != nil {
		t.Fatalf("write restore validation: %v\n%s", err, buf.String())
	}

	var validation backupRestoreValidation
	if err := json.Unmarshal(buf.Bytes(), &validation); err != nil {
		t.Fatalf("restore validation JSON: %v\n%s", err, buf.String())
	}

	if validation.Status != backupManifestStatusOK || validation.SchemaVersion != backupRestoreValidationSchemaVersion {
		t.Fatalf("validation header = %+v", validation)
	}

	if validation.Deployment != "local" || validation.Profile != "simple" || validation.Manifest != manifestPath {
		t.Fatalf("validation metadata = %+v", validation)
	}

	if validation.Verification.Status != backupManifestStatusOK || validation.Verification.Summary.StorageReportsVerified != 3 {
		t.Fatalf("validation verification = %+v", validation.Verification)
	}

	if validation.SmokeRun.RunID != "run-restore" || validation.SmokeRun.RunIndex != 7 || validation.SmokeRun.Status != "succeeded" || !validation.SmokeRun.Passed {
		t.Fatalf("validation smoke run = %+v", validation.SmokeRun)
	}
}

func TestBackupRestoreValidationRejectsFailedSmokeRun(t *testing.T) {
	withOutputFormat(t, outputJSON)

	now := time.Date(2026, 6, 28, 17, 30, 0, 0, time.UTC)
	root := t.TempDir()
	manifestPath := writeBackupJSONFile(t, root, "manifest.json", retentionBackupManifestForTest(now.Add(-30*time.Minute).Format(time.RFC3339)))

	var buf bytes.Buffer
	err := writeBackupRestoreValidation(&buf, backupRestoreValidationOptions{
		ManifestPath: manifestPath,
		SmokeRunID:   "run-failed",
	}, now, func(runID string) (runDetail, error) {
		return runDetail{RunID: runID, RunIndex: 8, Status: "failed"}, nil
	})

	if err == nil {
		t.Fatalf("restore validation succeeded unexpectedly")
	}

	var validation backupRestoreValidation
	if decodeErr := json.Unmarshal(buf.Bytes(), &validation); decodeErr != nil {
		t.Fatalf("restore validation JSON: %v\n%s", decodeErr, buf.String())
	}

	if validation.Status != backupManifestStatusFailed {
		t.Fatalf("validation status = %q", validation.Status)
	}

	if validation.Verification.Status != backupManifestStatusOK {
		t.Fatalf("validation verification = %+v", validation.Verification)
	}

	if validation.SmokeRun.RunID != "run-failed" || validation.SmokeRun.Status != "failed" || validation.SmokeRun.Passed {
		t.Fatalf("validation smoke run = %+v", validation.SmokeRun)
	}

	if !strings.Contains(err.Error(), "finished with status") {
		t.Fatalf("restore validation error = %v", err)
	}
}

func TestBackupRestoreValidationRequiresSmokeRun(t *testing.T) {
	withOutputFormat(t, outputJSON)

	root := t.TempDir()
	manifestPath := writeBackupJSONFile(t, root, "manifest.json", retentionBackupManifestForTest(time.Date(2026, 6, 28, 17, 0, 0, 0, time.UTC).Format(time.RFC3339)))

	var buf bytes.Buffer
	err := writeBackupRestoreValidation(&buf, backupRestoreValidationOptions{ManifestPath: manifestPath}, time.Now().UTC(), func(runID string) (runDetail, error) {
		t.Fatalf("fetch run should not be called")
		return runDetail{}, nil
	})

	if err == nil || !strings.Contains(err.Error(), "--smoke-run is required") {
		t.Fatalf("restore validation error = %v", err)
	}

	if buf.Len() != 0 {
		t.Fatalf("restore validation wrote output unexpectedly: %s", buf.String())
	}
}

func TestBackupPodmanExpectedTopologyHA(t *testing.T) {
	withOutputFormat(t, outputJSON)

	var buf bytes.Buffer
	if err := writeBackupPodmanExpectedTopology(&buf, podmanProfileHA); err != nil {
		t.Fatalf("write podman expected topology: %v", err)
	}

	var expected backupExpectedTopology
	if err := json.Unmarshal(buf.Bytes(), &expected); err != nil {
		t.Fatalf("expected topology JSON: %v\n%s", err, buf.String())
	}

	if expected.SchemaVersion != backupExpectedTopologySchemaVersion {
		t.Fatalf("schema version = %d", expected.SchemaVersion)
	}

	if len(expected.DatabaseRoles) != 3 {
		t.Fatalf("database roles = %+v", expected.DatabaseRoles)
	}

	if len(expected.Instances) != 6 {
		t.Fatalf("instances = %+v", expected.Instances)
	}

	if len(expected.Paths) != 8 {
		t.Fatalf("paths = %+v", expected.Paths)
	}

	pathSet := backupExpectedPathSet(expected.Paths)
	for _, key := range []string{
		"local_state/queue.persistence//data/vectis/queue/local-ha/queue-1",
		"local_state/queue.persistence//data/vectis/queue/local-ha/queue-2",
		"local_state/log.storage//data/vectis/jobs/log-1",
		"local_state/log.storage//data/vectis/jobs/log-2",
		"local_state/artifact.storage//data/vectis/artifact/artifact-1",
		"local_state/artifact.storage//data/vectis/artifact/artifact-2",
		"secret_stores/secrets.encryptedfs.root//data/vectis/secrets/encryptedfs",
		"secret_stores/secrets.encryptedfs.key_file//run/vectis/secrets/encryptedfs.key",
	} {
		if !pathSet[key] {
			t.Fatalf("expected paths missing %s: %+v", key, expected.Paths)
		}
	}

	manifest := backupManifest{
		SchemaVersion: backupManifestSchemaVersion,
		GeneratedAt:   "2026-06-28T13:00:00Z",
		Inventories: []backupManifestInventory{
			{Source: "podman.inventory.json", GeneratedAt: "2026-06-28T12:00:00Z", Version: "test", DatabaseDriver: "pgx"},
		},
	}

	version := 42
	dirty := false
	for _, role := range expected.DatabaseRoles {
		manifest.DatabaseRoles = append(manifest.DatabaseRoles, backupManifestDatabaseRole{
			InventorySource: "podman.inventory.json",
			Role:            role.Role,
			Driver:          role.Driver,
			DSN:             "postgres://vectis:REDACTED@127.0.0.1:5432/vectis",
			Schema:          backupSchemaInventory{Inspectable: true, CurrentVersion: &version, Dirty: &dirty},
		})
	}

	for _, instance := range expected.Instances {
		manifest.Instances = append(manifest.Instances, backupManifestInstance{
			InventorySource: "podman.inventory.json",
			Service:         instance.Service,
			InstanceID:      instance.InstanceID,
		})
	}

	for _, path := range expected.Paths {
		manifest.RequiredPaths = append(manifest.RequiredPaths, backupManifestPath{
			InventorySource: "podman.inventory.json",
			Category:        path.Category,
			ID:              path.ID,
			Path:            path.Path,
			Enabled:         true,
			Exists:          true,
			Readable:        true,
		})
	}

	manifest.RequiredPaths = append(manifest.RequiredPaths, backupManifestPath{
		InventorySource: "podman.inventory.json",
		Category:        "config_paths",
		ID:              "deploy.config_dir",
		Path:            "/etc/vectis/deploy",
		Enabled:         true,
		Exists:          true,
		Readable:        true,
	})

	result := verifyBackupManifest(manifest, &backupExpectedTopologyInput{Source: "podman-expected.json", Expectations: expected}, time.Date(2026, 6, 28, 15, 0, 0, 0, time.UTC))
	if result.Status != backupManifestStatusOK || len(result.Errors) != 0 {
		t.Fatalf("podman expected topology verification = %+v", result)
	}
}

func TestBackupPodmanExpectedTopologySimple(t *testing.T) {
	withOutputFormat(t, outputJSON)

	var buf bytes.Buffer
	if err := writeBackupPodmanExpectedTopology(&buf, podmanProfileSimple); err != nil {
		t.Fatalf("write podman expected topology: %v", err)
	}

	var expected backupExpectedTopology
	if err := json.Unmarshal(buf.Bytes(), &expected); err != nil {
		t.Fatalf("expected topology JSON: %v\n%s", err, buf.String())
	}

	instanceSet := map[string]bool{}
	for _, instance := range expected.Instances {
		instanceSet[instance.Service+"/"+instance.InstanceID] = true
	}

	for _, key := range []string{"queue/queue-1", "log/log-1", "artifact/artifact-1"} {
		if !instanceSet[key] {
			t.Fatalf("expected instances missing %s: %+v", key, expected.Instances)
		}
	}

	pathSet := backupExpectedPathSet(expected.Paths)
	for _, key := range []string{
		"local_state/queue.persistence//data/vectis/queue",
		"local_state/log.storage//data/vectis/jobs",
		"local_state/artifact.storage//data/vectis/artifact",
		"secret_stores/secrets.encryptedfs.root//data/vectis/secrets/encryptedfs",
		"secret_stores/secrets.encryptedfs.key_file//run/vectis/secrets/encryptedfs.key",
	} {
		if !pathSet[key] {
			t.Fatalf("expected paths missing %s: %+v", key, expected.Paths)
		}
	}
}

func TestBackupLinuxExpectedTopology(t *testing.T) {
	withOutputFormat(t, outputJSON)

	var buf bytes.Buffer
	if err := writeBackupLinuxExpectedTopology(&buf, backupExpectLinuxManifestPath); err != nil {
		t.Fatalf("write linux expected topology: %v", err)
	}

	var expected backupExpectedTopology
	if err := json.Unmarshal(buf.Bytes(), &expected); err != nil {
		t.Fatalf("expected topology JSON: %v\n%s", err, buf.String())
	}

	if expected.SchemaVersion != backupExpectedTopologySchemaVersion {
		t.Fatalf("schema version = %d", expected.SchemaVersion)
	}

	if len(expected.DatabaseRoles) != 3 {
		t.Fatalf("database roles = %+v", expected.DatabaseRoles)
	}

	if len(expected.Instances) != 4 {
		t.Fatalf("instances = %+v", expected.Instances)
	}

	if len(expected.Paths) != 7 {
		t.Fatalf("paths = %+v", expected.Paths)
	}

	pathSet := backupExpectedPathSet(expected.Paths)
	for _, key := range []string{
		"local_state/queue.persistence//var/lib/vectis/queue/default/queue-1",
		"local_state/log.storage//var/lib/vectis/log/log-1",
		"local_state/artifact.storage//var/lib/vectis/artifact/artifact-1",
		"local_state/log_forwarder.spool//var/lib/vectis/log-forwarder/spool",
		"secret_stores/secrets.encryptedfs.root//var/lib/vectis/secrets/envelopes",
		"secret_stores/secrets.encryptedfs.key_file//etc/vectis/secrets/encryptedfs.key",
		"config_paths/linux.config_dir//etc/vectis",
	} {
		if !pathSet[key] {
			t.Fatalf("expected paths missing %s: %+v", key, expected.Paths)
		}
	}

	manifest := backupManifest{
		SchemaVersion: backupManifestSchemaVersion,
		GeneratedAt:   "2026-06-28T13:00:00Z",
		Inventories: []backupManifestInventory{
			{Source: "linux.inventory.json", GeneratedAt: "2026-06-28T12:00:00Z", Version: "test", DatabaseDriver: "pgx"},
		},
	}

	version := 42
	dirty := false
	for _, role := range expected.DatabaseRoles {
		manifest.DatabaseRoles = append(manifest.DatabaseRoles, backupManifestDatabaseRole{
			InventorySource: "linux.inventory.json",
			Role:            role.Role,
			Driver:          role.Driver,
			DSN:             "postgres://vectis:REDACTED@127.0.0.1:5432/vectis",
			Schema:          backupSchemaInventory{Inspectable: true, CurrentVersion: &version, Dirty: &dirty},
		})
	}

	for _, instance := range expected.Instances {
		manifest.Instances = append(manifest.Instances, backupManifestInstance{
			InventorySource: "linux.inventory.json",
			Service:         instance.Service,
			InstanceID:      instance.InstanceID,
		})
	}

	for _, path := range expected.Paths {
		manifest.RequiredPaths = append(manifest.RequiredPaths, backupManifestPath{
			InventorySource: "linux.inventory.json",
			Category:        path.Category,
			ID:              path.ID,
			Path:            path.Path,
			Enabled:         true,
			Exists:          true,
			Readable:        true,
		})
	}

	result := verifyBackupManifest(manifest, &backupExpectedTopologyInput{Source: "linux-expected.json", Expectations: expected}, time.Date(2026, 6, 28, 15, 0, 0, 0, time.UTC))
	if result.Status != backupManifestStatusOK || len(result.Errors) != 0 {
		t.Fatalf("linux expected topology verification = %+v", result)
	}
}

func backupPathsByID(paths []backupPathInventory) map[string]backupPathInventory {
	out := map[string]backupPathInventory{}
	for _, path := range paths {
		out[path.ID] = path
	}

	return out
}

func backupManifestPathsByCategoryID(paths []backupManifestPath) map[string]backupManifestPath {
	out := map[string]backupManifestPath{}
	for _, path := range paths {
		out[path.Category+"/"+path.ID] = path
	}

	return out
}

func backupExpectedPathSet(paths []backupExpectedPath) map[string]bool {
	out := map[string]bool{}
	for _, path := range paths {
		out[path.Category+"/"+path.ID+"/"+path.Path] = true
	}

	return out
}

func backupFindingsContain(findings []backupManifestFinding, id string) bool {
	for _, finding := range findings {
		if finding.ID == id {
			return true
		}
	}

	return false
}

func backupStorageReportForTest(surface, path string, checkedAt time.Time) storageverify.Report {
	return storageverify.Report{
		Surface:      surface,
		Path:         path,
		Status:       storageverify.StatusOK,
		CheckedFiles: 1,
		CheckedBytes: 2,
		Records:      3,
		CheckedAt:    checkedAt,
	}
}

func writeBackupJSONFile(t *testing.T, dir, name string, v any) string {
	t.Helper()

	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal %s: %v", name, err)
	}

	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}

	return path
}
