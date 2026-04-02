package database

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"strings"
	"syscall"

	"github.com/jackc/pgx/v5/pgconn"
)

func IsUnavailableError(err error) bool {
	if err == nil {
		return false
	}

	if isUnavailableTyped(err) {
		return true
	}

	return isUnavailableStringFallback(err)
}

func isUnavailableTyped(err error) bool {
	if errors.Is(err, sql.ErrConnDone) {
		return true
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	if errors.Is(err, net.ErrClosed) {
		return true
	}

	for _, e := range unavailableErrnos {
		if errors.Is(err, e) {
			return true
		}
	}

	var pe *pgconn.PgError
	if errors.As(err, &pe) && pgSQLStateUnavailable(pe.Code) {
		return true
	}

	var ce *pgconn.ConnectError
	if errors.As(err, &ce) {
		return isUnavailableTyped(errors.Unwrap(ce))
	}

	var op *net.OpError
	if errors.As(err, &op) {
		if op.Timeout() {
			return true
		}
		if op.Err != nil && isUnavailableTyped(op.Err) {
			return true
		}
	}

	var dns *net.DNSError
	if errors.As(err, &dns) {
		return true
	}

	return false
}

// PostgreSQL class 08 = connection exception; a few single codes for shutdown / resource pressure.
func pgSQLStateUnavailable(code string) bool {
	if len(code) < 2 {
		return false
	}

	switch code[:2] {
	case "08":
		return true
	default:
	}

	switch code {
	case "53300", "57P01", "57P03":
		return true
	default:
		return false
	}
}

var unavailableErrnos = []error{
	syscall.ECONNREFUSED,
	syscall.ECONNRESET,
	syscall.EPIPE,
	syscall.ETIMEDOUT,
	syscall.EHOSTUNREACH,
	syscall.ENETUNREACH,
	syscall.ECONNABORTED,
	syscall.ENETDOWN,
}

func isUnavailableStringFallback(err error) bool {
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "database is closed"),
		strings.Contains(msg, "driver: bad connection"):
		return true
	default:
		return false
	}
}
