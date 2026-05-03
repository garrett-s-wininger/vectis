package cli

import (
	"context"
	"time"

	"vectis/internal/interfaces"
)

const defaultShutdownCallbackTimeout = 5 * time.Second

func DeferShutdown(logger interfaces.Logger, name string, shutdown func(context.Context) error) func() {
	return DeferShutdownWithTimeout(logger, name, shutdown, defaultShutdownCallbackTimeout)
}

func DeferShutdownWithTimeout(
	logger interfaces.Logger,
	name string,
	shutdown func(context.Context) error,
	timeout time.Duration,
) func() {
	return func() {
		if shutdown == nil {
			return
		}

		shutCtx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		if err := shutdown(shutCtx); err != nil && logger != nil {
			logger.Warn("%s shutdown: %v", name, err)
		}
	}
}
