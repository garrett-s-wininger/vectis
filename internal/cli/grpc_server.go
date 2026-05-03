package cli

import (
	"context"
	"errors"
	"net"

	"vectis/internal/interfaces"

	"google.golang.org/grpc"
)

func ServeGRPC(ctx context.Context, srv *grpc.Server, ln net.Listener, serviceName string, logger interfaces.Logger) error {
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.Serve(ln)
	}()

	select {
	case <-ctx.Done():
		if logger != nil {
			logger.Info("Shutting down %s gRPC server...", serviceName)
		}

		srv.GracefulStop()
		if logger != nil {
			logger.Info("%s gRPC server stopped", serviceName)
		}

		return nil
	case err := <-serveErr:
		if errors.Is(err, grpc.ErrServerStopped) {
			return nil
		}

		return err
	}
}
