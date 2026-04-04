//go:build !nosqlite

package database

import (
	"database/sql"
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
