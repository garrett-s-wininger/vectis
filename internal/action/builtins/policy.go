package builtins

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"vectis/internal/action"
)

func positiveIntField(with map[string]string, name string, defaultValue int) (int, error) {
	raw := strings.TrimSpace(with[name])
	if raw == "" {
		return defaultValue, nil
	}

	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}

	return value, nil
}

func validatePositiveIntField(with map[string]string, name string) []action.FieldError {
	raw := strings.TrimSpace(with[name])
	if raw == "" {
		return nil
	}

	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return []action.FieldError{{Field: name, Message: "must be a positive integer"}}
	}

	return nil
}

func durationField(with map[string]string, name string) (time.Duration, error) {
	raw := strings.TrimSpace(with[name])
	if raw == "" {
		return 0, fmt.Errorf("%s is required", name)
	}

	duration, err := time.ParseDuration(raw)
	if err != nil || duration <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", name)
	}

	return duration, nil
}

func validateDurationField(with map[string]string, name string) []action.FieldError {
	raw := strings.TrimSpace(with[name])
	if raw == "" {
		return nil
	}

	duration, err := time.ParseDuration(raw)
	if err != nil || duration <= 0 {
		return []action.FieldError{{Field: name, Message: "must be a positive duration like \"30s\", \"5m\", or \"1h\""}}
	}

	return nil
}
