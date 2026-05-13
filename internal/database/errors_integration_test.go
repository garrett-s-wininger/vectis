//go:build integration

package database

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestIsUnavailableError_pgconnConnectErrorChain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := pgconn.Connect(ctx, "postgres://127.0.0.1:1/nope?sslmode=disable")
	if err == nil {
		t.Fatal("expected dial error")
	}

	if errors.Is(err, syscall.EPERM) {
		t.Skipf("local TCP dial blocked by environment: %v", err)
	}

	if !IsUnavailableError(err) {
		t.Fatalf("expected pgconn dial failure to be unavailable: %v (%T)", err, err)
	}
}
