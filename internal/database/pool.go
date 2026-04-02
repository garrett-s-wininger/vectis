package database

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"vectis/internal/config"
)

const (
	EnvPgxMaxOpenConns    = "VECTIS_DATABASE_PGX_MAX_OPEN_CONNS"
	EnvPgxMaxIdleConns    = "VECTIS_DATABASE_PGX_MAX_IDLE_CONNS"
	EnvPgxConnMaxLifetime = "VECTIS_DATABASE_PGX_CONN_MAX_LIFETIME"
	EnvPgxConnMaxIdleTime = "VECTIS_DATABASE_PGX_CONN_MAX_IDLE_TIME"
)

type pgxPoolConfig struct {
	maxOpen     int
	maxIdle     int
	maxLifetime time.Duration
	maxIdleTime time.Duration
}

func effectivePgxPool() (pgxPoolConfig, error) {
	maxOpen := config.DatabasePgxPoolMaxOpenConns()
	if v := strings.TrimSpace(os.Getenv(EnvPgxMaxOpenConns)); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return pgxPoolConfig{}, fmt.Errorf("%s: %w", EnvPgxMaxOpenConns, err)
		}

		if n <= 0 {
			return pgxPoolConfig{}, fmt.Errorf("%s must be > 0 (got %d)", EnvPgxMaxOpenConns, n)
		}

		maxOpen = n
	}

	maxIdle := config.DatabasePgxPoolMaxIdleConns()
	if v := strings.TrimSpace(os.Getenv(EnvPgxMaxIdleConns)); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return pgxPoolConfig{}, fmt.Errorf("%s: %w", EnvPgxMaxIdleConns, err)
		}
		if n < 0 {
			return pgxPoolConfig{}, fmt.Errorf("%s must be >= 0 (got %d)", EnvPgxMaxIdleConns, n)
		}
		maxIdle = n
	}

	if maxIdle > maxOpen {
		maxIdle = maxOpen
	}

	maxLifetime := config.DatabasePgxConnMaxLifetime()
	if v := strings.TrimSpace(os.Getenv(EnvPgxConnMaxLifetime)); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return pgxPoolConfig{}, fmt.Errorf("%s: %w", EnvPgxConnMaxLifetime, err)
		}
		maxLifetime = d
	}

	maxIdleTime := config.DatabasePgxConnMaxIdleTime()
	if v := strings.TrimSpace(os.Getenv(EnvPgxConnMaxIdleTime)); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return pgxPoolConfig{}, fmt.Errorf("%s: %w", EnvPgxConnMaxIdleTime, err)
		}
		maxIdleTime = d
	}

	return pgxPoolConfig{
		maxOpen:     maxOpen,
		maxIdle:     maxIdle,
		maxLifetime: maxLifetime,
		maxIdleTime: maxIdleTime,
	}, nil
}

func applyPgxPoolSettings(db *sql.DB) error {
	cfg, err := effectivePgxPool()
	if err != nil {
		return err
	}

	db.SetMaxOpenConns(cfg.maxOpen)
	db.SetMaxIdleConns(cfg.maxIdle)
	db.SetConnMaxLifetime(cfg.maxLifetime)
	db.SetConnMaxIdleTime(cfg.maxIdleTime)
	return nil
}
