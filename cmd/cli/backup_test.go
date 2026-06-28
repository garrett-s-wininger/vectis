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

func backupPathsByID(paths []backupPathInventory) map[string]backupPathInventory {
	out := map[string]backupPathInventory{}
	for _, path := range paths {
		out[path.ID] = path
	}

	return out
}
