//go:build !nosqlite

package database

import (
	"errors"

	sqlite3 "github.com/mattn/go-sqlite3"
)

func sqliteWaitIsConnectFailure(err error) (connectFailure bool, matched bool) {
	var e sqlite3.Error
	if !errors.As(err, &e) {
		return false, false
	}

	switch e.Code {
	case sqlite3.ErrCantOpen, sqlite3.ErrNotADB, sqlite3.ErrAuth, sqlite3.ErrCorrupt, sqlite3.ErrIoErr, sqlite3.ErrNomem:
		return true, true
	default:
		return false, true
	}
}
