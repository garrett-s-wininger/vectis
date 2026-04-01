package dbtest

import (
	"database/sql"
	"testing"

	"vectis/internal/migrations"

	_ "github.com/mattn/go-sqlite3"
)

func NewTestDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open test db: %v", err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := migrations.Run(db, "sqlite3"); err != nil {
		db.Close()
		t.Fatalf("failed to run migrations: %v", err)
	}

	t.Cleanup(func() { db.Close() })
	return db
}
