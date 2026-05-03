package cli

import (
	"context"
	"errors"
	"net/http"
	"time"

	"vectis/internal/interfaces"
)

func ServeHTTP(
	ctx context.Context,
	srv *http.Server,
	serve func() error,
	shutdownTimeout time.Duration,
	shutdownName string,
	logger interfaces.Logger,
) error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- serve()
	}()

	select {
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutCtx); err != nil && logger != nil {
			logger.Warn("%s shutdown: %v", shutdownName, err)
		}

		err := <-errCh
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
