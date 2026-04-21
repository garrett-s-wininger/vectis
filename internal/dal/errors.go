package dal

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrNotFound = errors.New("not found")
	ErrConflict = errors.New("conflict")
)

func IsNotFound(err error) bool {
	return errors.Is(err, ErrNotFound)
}

func IsConflict(err error) bool {
	return errors.Is(err, ErrConflict)
}

func normalizeSQLError(err error) error {
	if err == nil {
		return nil
	}

	lower := strings.ToLower(err.Error())
	if strings.Contains(lower, "unique constraint failed") ||
		strings.Contains(lower, "duplicate key value violates unique constraint") {
		return fmt.Errorf("%w: %v", ErrConflict, err)
	}

	return err
}
