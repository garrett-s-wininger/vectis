package observability

import (
	"testing"

	"vectis/internal/database"
)

func TestRegisterSQLDBPoolMetrics_nilDB(t *testing.T) {
	t.Parallel()

	err := RegisterSQLDBPoolMetrics(nil)
	if err == nil {
		t.Fatal("expected error for nil *sql.DB")
	}
}

func TestRegisterTaskDispatchBacklogMetrics_nilDB(t *testing.T) {
	t.Parallel()

	err := RegisterTaskDispatchBacklogMetrics(nil)
	if err == nil {
		t.Fatal("expected error for nil *sql.DB")
	}
}

func TestRebindMetricQueryForPgx(t *testing.T) {
	t.Setenv(database.EnvDatabaseDriver, "pgx")

	got := rebindMetricQueryForPgx("SELECT ? AS first, ? AS second")
	want := "SELECT $1 AS first, $2 AS second"
	if got != want {
		t.Fatalf("query = %q, want %q", got, want)
	}
}

func TestRebindMetricQueryForPgx_nonPgx(t *testing.T) {
	t.Setenv(database.EnvDatabaseDriver, "sqlite3")

	query := "SELECT ? AS first, ? AS second"
	if got := rebindMetricQueryForPgx(query); got != query {
		t.Fatalf("query = %q, want %q", got, query)
	}
}
