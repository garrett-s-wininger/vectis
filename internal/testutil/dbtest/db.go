package dbtest

import (
	"context"
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

	if _, err := db.ExecContext(context.Background(), "PRAGMA foreign_keys = ON"); err != nil {
		_ = db.Close()
		t.Fatalf("failed to enable sqlite foreign keys: %v", err)
	}

	if err := migrations.Run(db, "sqlite3"); err != nil {
		_ = db.Close()
		t.Fatalf("failed to run migrations: %v", err)
	}

	t.Cleanup(func() { _ = db.Close() })
	return db
}
