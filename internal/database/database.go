package database

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"vectis/internal/config"
	"vectis/internal/migrations"
)

const (
	EnvDatabaseDriver = "VECTIS_DATABASE_DRIVER"
	EnvDatabaseDSN    = "VECTIS_DATABASE_DSN"

	postgresMigrationAdvisoryLockKey int64 = 987654321

	postgresMigrationRetryDeadline    = 60 * time.Second
	postgresMigrationLockTimeout      = 10 * time.Second
	postgresMigrationRetrySleep       = 750 * time.Millisecond

	schemaWaitDeadline       = 5 * time.Minute
	schemaWaitPollInterval   = 750 * time.Millisecond
)

func GetDBPath() string {
	if dsn := os.Getenv(EnvDatabaseDSN); dsn != "" {
		if strings.Contains(dsn, "{{data_home}}") {
			return strings.NewReplacer("{{data_home}}", DataHome()).Replace(dsn)
		}

		return dsn
	}

	dataHome := DataHome()
	return config.DBDSN(dataHome)
}

func DataHome() string {
	dataHome := os.Getenv("XDG_DATA_HOME")
	if dataHome == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			panic(fmt.Sprintf("cannot determine home directory: %v", err))
		}

		dataHome = filepath.Join(home, ".local", "share")
	}

	return dataHome
}

func OpenDB(dbPath string) (*sql.DB, error) {
	driver := EffectiveDBDriver()
	if driver == "sqlite3" {
		dir := filepath.Dir(dbPath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create data directory: %w", err)
		}
	}

	db, err := sql.Open(driver, dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	return db, nil
}

func EffectiveDBDriver() string {
	if driver := os.Getenv(EnvDatabaseDriver); driver != "" {
		return driver
	}

	return config.DBDriver()
}

func Migrate(dbPath string) error {
	db, err := OpenDB(dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	return MigrateDB(db)
}

func MigrateDB(db *sql.DB) error {
	driver := EffectiveDBDriver()

	if driver == "pgx" {
		deadline := time.Now().Add(postgresMigrationRetryDeadline)
		var lastErr error

		for time.Now().Before(deadline) {
			ctx, cancel := context.WithTimeout(context.Background(), postgresMigrationLockTimeout)
			lastErr = migrateWithLock(ctx, db, postgresMigrationAdvisoryLockKey, driver)
			cancel()

			if lastErr == nil {
				return nil
			}

			time.Sleep(postgresMigrationRetrySleep)
		}

		return fmt.Errorf("failed to run migrations (postgres): %w", lastErr)
	}

	if err := migrations.Run(db, driver); err != nil {
		return fmt.Errorf("failed to run migrations: %w", err)
	}

	return nil
}

func migrateWithLock(ctx context.Context, db *sql.DB, advisoryLockKey int64, driver string) error {
	if _, err := db.ExecContext(ctx, "SELECT pg_advisory_lock($1)", advisoryLockKey); err != nil {
		return err
	}
	defer func() { _, _ = db.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", advisoryLockKey) }()

	return migrations.Run(db, driver)
}

func WaitForMigrations(db *sql.DB) error {
	driver := EffectiveDBDriver()
	switch driver {
	case "pgx", "sqlite3":
	default:
		return nil
	}

	deadline := time.Now().Add(schemaWaitDeadline)
	for time.Now().Before(deadline) {
		var one int
		err := db.QueryRow("SELECT 1 FROM stored_jobs LIMIT 1").Scan(&one)
		if err == nil {
			return nil
		}

		time.Sleep(schemaWaitPollInterval)
	}

	return fmt.Errorf("timed out waiting for database schema; apply migrations with vectis-cli migrate (same VECTIS_DATABASE_DRIVER / VECTIS_DATABASE_DSN)")
}
