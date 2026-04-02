package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestIsUnavailableError_sqlErrConnDone(t *testing.T) {
	t.Parallel()
	err := fmt.Errorf("wrapped: %w", sql.ErrConnDone)
	if !IsUnavailableError(err) {
		t.Fatal("expected ErrConnDone chain to be unavailable")
	}
}

func TestIsUnavailableError_deadlineExceeded(t *testing.T) {
	t.Parallel()
	err := fmt.Errorf("op: %w", context.DeadlineExceeded)
	if !IsUnavailableError(err) {
		t.Fatal("expected DeadlineExceeded to be unavailable")
	}
}

func TestIsUnavailableError_syscallErrno(t *testing.T) {
	t.Parallel()
	err := fmt.Errorf("dial tcp: %w", syscall.ECONNREFUSED)
	if !IsUnavailableError(err) {
		t.Fatal("expected ECONNREFUSED to be unavailable")
	}
}

func TestIsUnavailableError_netOpError(t *testing.T) {
	t.Parallel()
	op := &net.OpError{
		Op:  "dial",
		Net: "tcp",
		Err: syscall.ECONNREFUSED,
	}

	if !IsUnavailableError(op) {
		t.Fatal("expected net.OpError with ECONNREFUSED to be unavailable")
	}
}

func TestIsUnavailableError_netOpErrorTimeout(t *testing.T) {
	t.Parallel()
	op := &net.OpError{
		Op:  "read",
		Net: "tcp",
		Err: syscall.ETIMEDOUT,
	}

	if !IsUnavailableError(op) {
		t.Fatal("expected timeout OpError to be unavailable")
	}
}

func TestIsUnavailableError_dns(t *testing.T) {
	t.Parallel()
	dns := &net.DNSError{Err: "no such host", Name: "x", IsNotFound: true}
	if !IsUnavailableError(dns) {
		t.Fatal("expected DNSError to be unavailable")
	}
}

func TestIsUnavailableError_pgxSQLStateConnectionClass(t *testing.T) {
	t.Parallel()
	err := &pgconn.PgError{Code: "08006", Message: "connection failure"}
	if !IsUnavailableError(err) {
		t.Fatal("expected SQLSTATE 08xxx to be unavailable")
	}
}

func TestIsUnavailableError_pgxSQLStateTooManyConnections(t *testing.T) {
	t.Parallel()
	err := &pgconn.PgError{Code: "53300", Message: "too many connections"}
	if !IsUnavailableError(err) {
		t.Fatal("expected 53300 to be unavailable")
	}
}

func TestIsUnavailableError_pgxSQLStateNotUnavailable(t *testing.T) {
	t.Parallel()
	err := &pgconn.PgError{Code: "23505", Message: "unique_violation"}
	if IsUnavailableError(err) {
		t.Fatal("expected unique violation not to be unavailable")
	}
}

func TestIsUnavailableError_wrappedNetOpError(t *testing.T) {
	t.Parallel()
	inner := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	err := fmt.Errorf("failed to connect: %w", inner)
	if !IsUnavailableError(err) {
		t.Fatal("expected wrapped net.OpError to be unavailable")
	}
}

func TestIsUnavailableError_nil(t *testing.T) {
	t.Parallel()
	if IsUnavailableError(nil) {
		t.Fatal("nil should not be unavailable")
	}
}

func TestIsUnavailableError_plainLogicError(t *testing.T) {
	t.Parallel()
	if IsUnavailableError(errors.New("syntax error at or near")) {
		t.Fatal("plain message should not be unavailable")
	}
}

func TestIsUnavailableError_sqliteFallback(t *testing.T) {
	t.Parallel()
	if !IsUnavailableError(errors.New("database is closed")) {
		t.Fatal("sqlite-style message should still match fallback")
	}

	if !IsUnavailableError(errors.New("driver: bad connection")) {
		t.Fatal("sqlite driver-style message should still match fallback")
	}
}

func TestIsUnavailableError_netErrClosed(t *testing.T) {
	t.Parallel()
	err := fmt.Errorf("read: %w", net.ErrClosed)
	if !IsUnavailableError(err) {
		t.Fatal("expected net.ErrClosed to be unavailable")
	}
}

func TestIsUnavailableError_pgconnConnectErrorChain(t *testing.T) {
	if testing.Short() {
		t.Skip("uses local network dial")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := pgconn.Connect(ctx, "postgres://127.0.0.1:1/nope?sslmode=disable")
	if err == nil {
		t.Fatal("expected dial error")
	}

	if !IsUnavailableError(err) {
		t.Fatalf("expected pgconn dial failure to be unavailable: %v (%T)", err, err)
	}
}
