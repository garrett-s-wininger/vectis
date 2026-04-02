package database

import (
	"testing"

	_ "vectis/internal/dbdrivers"
)

func TestOpenDB_Pgx_AppliesPoolFromDefaults(t *testing.T) {
	t.Setenv(EnvDatabaseDriver, "pgx")
	t.Setenv(EnvPgxMaxOpenConns, "")
	t.Setenv(EnvPgxMaxIdleConns, "")

	db, err := OpenDB("postgres://user:pass@127.0.0.1:9/nope?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if got := db.Stats().MaxOpenConnections; got != 25 {
		t.Fatalf("MaxOpenConnections: want 25, got %d", got)
	}
}

func TestOpenDB_Pgx_EnvOverridesPool(t *testing.T) {
	t.Setenv(EnvDatabaseDriver, "pgx")
	t.Setenv(EnvPgxMaxOpenConns, "11")
	t.Setenv(EnvPgxMaxIdleConns, "4")
	t.Setenv(EnvPgxConnMaxLifetime, "3m")
	t.Setenv(EnvPgxConnMaxIdleTime, "30s")

	db, err := OpenDB("postgres://user:pass@127.0.0.1:9/nope?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if got := db.Stats().MaxOpenConnections; got != 11 {
		t.Fatalf("MaxOpenConnections: want 11, got %d", got)
	}
}

func TestEffectivePgxPool_CapsMaxIdleToMaxOpen(t *testing.T) {
	t.Setenv(EnvPgxMaxOpenConns, "3")
	t.Setenv(EnvPgxMaxIdleConns, "100")

	cfg, err := effectivePgxPool()
	if err != nil {
		t.Fatal(err)
	}

	if cfg.maxOpen != 3 || cfg.maxIdle != 3 {
		t.Fatalf("want maxOpen=3 maxIdle=3, got %d %d", cfg.maxOpen, cfg.maxIdle)
	}
}

func TestOpenDB_Pgx_InvalidMaxOpenEnv(t *testing.T) {
	t.Setenv(EnvDatabaseDriver, "pgx")
	t.Setenv(EnvPgxMaxOpenConns, "not-a-number")

	_, err := OpenDB("postgres://user:pass@127.0.0.1:9/nope?sslmode=disable")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestOpenDB_Pgx_InvalidDurationEnv(t *testing.T) {
	t.Setenv(EnvDatabaseDriver, "pgx")
	t.Setenv(EnvPgxConnMaxLifetime, "forever")

	_, err := OpenDB("postgres://user:pass@127.0.0.1:9/nope?sslmode=disable")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestOpenDB_Sqlite_DoesNotApplyPgxPool(t *testing.T) {
	t.Setenv(EnvDatabaseDriver, "sqlite3")
	t.Setenv(EnvPgxMaxOpenConns, "7")

	db, err := OpenDB(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if got := db.Stats().MaxOpenConnections; got != 0 {
		t.Fatalf("sqlite MaxOpenConnections should stay default 0, got %d", got)
	}
}

func TestEffectivePgxPool_FromConfigDefaults(t *testing.T) {
	t.Setenv(EnvPgxMaxOpenConns, "")
	t.Setenv(EnvPgxMaxIdleConns, "")
	t.Setenv(EnvPgxConnMaxLifetime, "")
	t.Setenv(EnvPgxConnMaxIdleTime, "")

	cfg, err := effectivePgxPool()
	if err != nil {
		t.Fatal(err)
	}

	if cfg.maxOpen != 25 || cfg.maxIdle != 10 {
		t.Fatalf("want maxOpen=25 maxIdle=10, got %d %d", cfg.maxOpen, cfg.maxIdle)
	}
}
