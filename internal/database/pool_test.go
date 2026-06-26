package database

import (
	"context"
	"net/url"
	"path/filepath"
	"testing"
	"time"

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

func TestPgxDSNWithDefaults_PlanCacheModeURL(t *testing.T) {
	t.Setenv("VECTIS_DATABASE_PGX_PLAN_CACHE_MODE", "force_generic_plan")

	got, err := pgxDSNWithDefaults("postgres://user:pass@127.0.0.1:9/nope?sslmode=disable")
	if err != nil {
		t.Fatalf("pgxDSNWithDefaults: %v", err)
	}

	parsed, err := url.Parse(got)
	if err != nil {
		t.Fatalf("parse resulting dsn: %v", err)
	}

	if query := parsed.Query(); query.Get("plan_cache_mode") != "force_generic_plan" || query.Get("sslmode") != "disable" {
		t.Fatalf("query = %v, want plan_cache_mode=force_generic_plan and sslmode=disable", query)
	}
}

func TestPgxDSNWithDefaults_PreservesExplicitPlanCacheMode(t *testing.T) {
	t.Setenv("VECTIS_DATABASE_PGX_PLAN_CACHE_MODE", "force_generic_plan")

	got, err := pgxDSNWithDefaults("postgres://user:pass@127.0.0.1:9/nope?sslmode=disable&plan_cache_mode=force_custom_plan")
	if err != nil {
		t.Fatalf("pgxDSNWithDefaults: %v", err)
	}

	parsed, err := url.Parse(got)
	if err != nil {
		t.Fatalf("parse resulting dsn: %v", err)
	}

	if got := parsed.Query().Get("plan_cache_mode"); got != "force_custom_plan" {
		t.Fatalf("plan_cache_mode = %q, want force_custom_plan", got)
	}
}

func TestPgxDSNWithDefaults_PlanCacheModeKeywordDSN(t *testing.T) {
	t.Setenv("VECTIS_DATABASE_PGX_PLAN_CACHE_MODE", "force_generic_plan")

	got, err := pgxDSNWithDefaults("host=127.0.0.1 port=5432 dbname=vectis sslmode=disable")
	if err != nil {
		t.Fatalf("pgxDSNWithDefaults: %v", err)
	}

	want := "host=127.0.0.1 port=5432 dbname=vectis sslmode=disable plan_cache_mode=force_generic_plan"
	if got != want {
		t.Fatalf("pgxDSNWithDefaults keyword DSN = %q, want %q", got, want)
	}
}

func TestPgxDSNWithDefaults_InvalidPlanCacheMode(t *testing.T) {
	t.Setenv("VECTIS_DATABASE_PGX_PLAN_CACHE_MODE", "always")

	if _, err := pgxDSNWithDefaults("postgres://user:pass@127.0.0.1:9/nope?sslmode=disable"); err == nil {
		t.Fatal("pgxDSNWithDefaults succeeded with invalid plan cache mode")
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

func TestSQLiteDSNWithDefaults(t *testing.T) {
	tests := []struct {
		name string
		dsn  string
		want string
	}{
		{
			name: "plain file path",
			dsn:  "/tmp/vectis.db",
			want: "/tmp/vectis.db?_busy_timeout=10000&_journal_mode=WAL&_txlock=immediate",
		},
		{
			name: "preserves existing query",
			dsn:  "file:/tmp/vectis.db?cache=shared",
			want: "file:/tmp/vectis.db?cache=shared&_busy_timeout=10000&_journal_mode=WAL&_txlock=immediate",
		},
		{
			name: "preserves configured busy timeout",
			dsn:  "/tmp/vectis.db?_busy_timeout=250",
			want: "/tmp/vectis.db?_busy_timeout=250&_journal_mode=WAL&_txlock=immediate",
		},
		{
			name: "preserves configured journal mode",
			dsn:  "/tmp/vectis.db?_journal_mode=DELETE",
			want: "/tmp/vectis.db?_journal_mode=DELETE&_busy_timeout=10000&_txlock=immediate",
		},
		{
			name: "preserves configured txlock",
			dsn:  "/tmp/vectis.db?_txlock=deferred",
			want: "/tmp/vectis.db?_txlock=deferred&_busy_timeout=10000&_journal_mode=WAL",
		},
		{
			name: "leaves memory database alone",
			dsn:  ":memory:",
			want: ":memory:",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sqliteDSNWithDefaults(tt.dsn); got != tt.want {
				t.Fatalf("sqliteDSNWithDefaults(%q): want %q, got %q", tt.dsn, tt.want, got)
			}
		})
	}
}

func TestOpenDB_SqliteQueuesConcurrentWriteTransactions(t *testing.T) {
	t.Setenv(EnvDatabaseDriver, "sqlite3")

	db, err := OpenDB(filepath.Join(t.TempDir(), "vectis.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(2)

	if _, err := db.Exec(`CREATE TABLE writes (id INTEGER PRIMARY KEY, value TEXT NOT NULL)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	tx1, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin tx1: %v", err)
	}
	defer tx1.Rollback()
	if _, err := tx1.Exec(`INSERT INTO writes (value) VALUES ('first')`); err != nil {
		t.Fatalf("insert tx1: %v", err)
	}

	errCh := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		tx2, err := db.BeginTx(ctx, nil)
		if err != nil {
			errCh <- err
			return
		}
		defer tx2.Rollback()

		if _, err := tx2.ExecContext(ctx, `INSERT INTO writes (value) VALUES ('second')`); err != nil {
			errCh <- err
			return
		}
		errCh <- tx2.Commit()
	}()

	<-started
	select {
	case err := <-errCh:
		t.Fatalf("second write transaction completed before first commit: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	if err := tx1.Commit(); err != nil {
		t.Fatalf("commit tx1: %v", err)
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("second write transaction: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second write transaction did not complete after first commit")
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
