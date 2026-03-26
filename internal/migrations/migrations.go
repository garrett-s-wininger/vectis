package migrations

import (
	"database/sql"
	"embed"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/database/sqlite3"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed sqlite/*.sql postgres/*.sql
var migrationFiles embed.FS

func Run(db *sql.DB, dbDriver string) error {
	var (
		driverInstance database.Driver
		sourceBase     string
		migrateDriver  string
	)

	switch dbDriver {
	case "sqlite3":
		d, err := sqlite3.WithInstance(db, &sqlite3.Config{})
		if err != nil {
			return fmt.Errorf("failed to create sqlite database driver: %w", err)
		}

		driverInstance = d
		sourceBase = "sqlite"
		migrateDriver = "sqlite3"
	case "pgx":
		d, err := postgres.WithInstance(db, &postgres.Config{})
		if err != nil {
			return fmt.Errorf("failed to create postgres database driver: %w", err)
		}

		driverInstance = d
		sourceBase = "postgres"
		migrateDriver = "postgres"
	default:
		return fmt.Errorf("unsupported database driver for migrations: %q", dbDriver)
	}

	source, err := iofs.New(migrationFiles, sourceBase)
	if err != nil {
		return fmt.Errorf("failed to create migration source: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", source, migrateDriver, driverInstance)
	if err != nil {
		return fmt.Errorf("failed to create migrate instance: %w", err)
	}

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("failed to run migrations: %w", err)
	}

	return nil
}
