//go:build !nosqlite

package database

import (
	"database/sql"
	"strings"
	"testing"

	_ "vectis/internal/dbdrivers"
)

func TestSchemaWaitConnectFailure_sqliteMissingTable(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open memory sqlite: %v", err)
	}
	defer db.Close()

	var one int
	err = db.QueryRow("SELECT 1 FROM schema_migrations LIMIT 1").Scan(&one)
	if err == nil {
		t.Fatal("expected error querying missing table")
	}

	if schemaWaitConnectFailure(err) {
		t.Fatalf("missing schema_migrations should not be classified as connect failure, got err %v", err)
	}
}

func TestSchemaMigrationsReadyRejectsDirtyVersion(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open memory sqlite: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE schema_migrations (version INTEGER NOT NULL PRIMARY KEY, dirty INTEGER NOT NULL)"); err != nil {
		t.Fatalf("create schema_migrations: %v", err)
	}

	if _, err := db.Exec("INSERT INTO schema_migrations (version, dirty) VALUES (1, 1)"); err != nil {
		t.Fatalf("insert dirty migration row: %v", err)
	}

	err = schemaMigrationsReady(db)
	if err == nil || !strings.Contains(err.Error(), "dirty") {
		t.Fatalf("schemaMigrationsReady dirty err = %v, want dirty error", err)
	}

	if _, err := db.Exec("UPDATE schema_migrations SET dirty = 0 WHERE version = 1"); err != nil {
		t.Fatalf("clear dirty migration row: %v", err)
	}

	if err := schemaMigrationsReady(db); err != nil {
		t.Fatalf("schemaMigrationsReady clean row: %v", err)
	}
}
