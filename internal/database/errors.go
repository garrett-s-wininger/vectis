package database

import (
	"database/sql"
	"errors"
	"strings"
)

func IsUnavailableError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, sql.ErrConnDone) {
		return true
	}

	// TODO(garrett): Do something better than a string compare, if possible.
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "database is closed"),
		strings.Contains(msg, "driver: bad connection"),
		strings.Contains(msg, "connection refused"),
		strings.Contains(msg, "broken pipe"),
		strings.Contains(msg, "connection reset"),
		strings.Contains(msg, "network is unreachable"),
		strings.Contains(msg, "no such host"),
		strings.Contains(msg, "i/o timeout"),
		strings.Contains(msg, "timeout"):
		return true
	default:
		return false
	}
}
