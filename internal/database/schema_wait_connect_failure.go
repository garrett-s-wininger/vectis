package database

import (
	"context"
	"database/sql"
	"errors"
	"net"

	"github.com/jackc/pgx/v5/pgconn"
)

func schemaWaitConnectFailure(err error) bool {
	if err == nil || errors.Is(err, sql.ErrNoRows) {
		return false
	}

	var connErr *pgconn.ConnectError
	if errors.As(err, &connErr) {
		return true
	}

	var parseErr *pgconn.ParseConfigError
	if errors.As(err, &parseErr) {
		return true
	}

	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgWaitIsConnectFailure(pgErr)
	}

	if cf, matched := sqliteWaitIsConnectFailure(err); matched {
		return cf
	}

	return false
}

func pgWaitIsConnectFailure(e *pgconn.PgError) bool {
	switch e.Code {
	case "28P01", "28000", "3D000":
		return true
	default:
		return false
	}
}
