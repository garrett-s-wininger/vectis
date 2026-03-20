package database

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	"vectis/internal/config"
	"vectis/internal/migrations"
)

func GetDBPath() string {
	dataHome := os.Getenv("XDG_DATA_HOME")
	if dataHome == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			panic(fmt.Sprintf("cannot determine home directory: %v", err))
		}
		dataHome = filepath.Join(home, ".local", "share")
	}
	return config.DBDSN(dataHome)
}

func OpenDB(dbPath string) (*sql.DB, error) {
	driver := config.DBDriver()
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

func Migrate(dbPath string) error {
	db, err := OpenDB(dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	if err := migrations.Run(db); err != nil {
		return fmt.Errorf("failed to run migrations: %w", err)
	}

	return nil
}
