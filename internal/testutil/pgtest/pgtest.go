//go:build integration

package pgtest

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"vectis/internal/database"
	"vectis/internal/migrations"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

const defaultPostgresImage = "postgres:18-alpine"

func NewTestDB(t *testing.T) *sql.DB {
	t.Helper()

	db := NewUnmigratedTestDB(t)

	if err := migrations.Run(db, "pgx"); err != nil {
		t.Fatalf("run postgres migrations: %v", err)
	}

	return db
}

func NewUnmigratedTestDB(t *testing.T) *sql.DB {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	container, err := postgres.Run(ctx,
		defaultPostgresImage,
		postgres.WithDatabase("vectis"),
		postgres.WithUsername("vectis"),
		postgres.WithPassword("vectis"),
		postgres.BasicWaitStrategies(),
		postgres.WithSQLDriver("pgx"),
	)

	if err != nil {
		if requirePostgresTests() {
			t.Fatalf("start postgres testcontainer: %v", err)
		}
		t.Skipf("skipping Postgres integration test; container runtime unavailable or image missing: %v", err)
	}

	testcontainers.CleanupContainer(t, container)

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("get postgres connection string: %v", err)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open postgres test db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping postgres test db: %v", err)
	}

	t.Setenv(database.EnvDatabaseDriver, "pgx")
	return db
}

func requirePostgresTests() bool {
	v := strings.ToLower(os.Getenv("VECTIS_REQUIRE_POSTGRES_TESTS"))
	return v == "1" || v == "true" || v == "yes"
}
