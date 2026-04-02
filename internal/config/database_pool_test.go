package config

import (
	"testing"
	"time"

	"github.com/spf13/viper"
)

func TestDatabasePgxPool_FromEmbeddedDefaults(t *testing.T) {
	if got := DatabasePgxPoolMaxOpenConns(); got != 25 {
		t.Fatalf("DatabasePgxPoolMaxOpenConns: want 25, got %d", got)
	}

	if got := DatabasePgxPoolMaxIdleConns(); got != 10 {
		t.Fatalf("DatabasePgxPoolMaxIdleConns: want 10, got %d", got)
	}

	if got := DatabasePgxConnMaxLifetime(); got != time.Hour {
		t.Fatalf("DatabasePgxConnMaxLifetime: want 1h, got %v", got)
	}

	if got := DatabasePgxConnMaxIdleTime(); got != 15*time.Minute {
		t.Fatalf("DatabasePgxConnMaxIdleTime: want 15m, got %v", got)
	}
}

func TestDatabasePgxPool_ViperOverrides(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("database.pgx_pool.max_open_conns", 40)
	viper.Set("database.pgx_pool.max_idle_conns", 8)
	viper.Set("database.pgx_pool.conn_max_lifetime", 90*time.Minute)
	viper.Set("database.pgx_pool.conn_max_idle_time", 5*time.Minute)

	if got := DatabasePgxPoolMaxOpenConns(); got != 40 {
		t.Fatalf("max open: got %d", got)
	}

	if got := DatabasePgxPoolMaxIdleConns(); got != 8 {
		t.Fatalf("max idle: got %d", got)
	}

	if got := DatabasePgxConnMaxLifetime(); got != 90*time.Minute {
		t.Fatalf("conn max lifetime: got %v", got)
	}

	if got := DatabasePgxConnMaxIdleTime(); got != 5*time.Minute {
		t.Fatalf("conn max idle: got %v", got)
	}
}
