package migrations_test

import (
	"database/sql"
	"testing"

	"vectis/internal/migrations"

	_ "github.com/mattn/go-sqlite3"
)

func TestSQLiteMigrations_UpDownRoundTrip(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := migrations.Run(db, "sqlite3"); err != nil {
		t.Fatalf("run up migrations: %v", err)
	}

	assertTableExists(t, db, "job_runs")
	assertTableExists(t, db, "run_dispatch_events")

	if err := migrations.Down(db, "sqlite3"); err != nil {
		t.Fatalf("run down migrations: %v", err)
	}

	assertTableMissing(t, db, "job_runs")
	assertTableMissing(t, db, "run_dispatch_events")
}

func assertTableExists(t *testing.T, db *sql.DB, table string) {
	t.Helper()

	var name string
	err := db.QueryRow(`SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`, table).Scan(&name)
	if err != nil {
		t.Fatalf("expected table %s to exist: %v", table, err)
	}
}

func assertTableMissing(t *testing.T, db *sql.DB, table string) {
	t.Helper()

	var name string
	err := db.QueryRow(`SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`, table).Scan(&name)
	if err == nil {
		t.Fatalf("expected table %s to be dropped, found %s", table, name)
	}

	if err != sql.ErrNoRows {
		t.Fatalf("query table %s: %v", table, err)
	}
}
