package cli

import (
	"context"
	"errors"
	"net"
	"time"

	"vectis/internal/interfaces"

	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

const defaultGRPCShutdownTimeout = 5 * time.Second

type grpcHealthSetter interface {
	SetServingStatus(string, healthpb.HealthCheckResponse_ServingStatus)
}

type grpcServeOptions struct {
	shutdownTimeout time.Duration
	health          grpcHealthSetter
	healthService   string
}

type GRPCServeOption func(*grpcServeOptions)

func WithGRPCShutdownTimeout(timeout time.Duration) GRPCServeOption {
	return func(opts *grpcServeOptions) {
		if timeout > 0 {
			opts.shutdownTimeout = timeout
		}
	}
}

func WithGRPCHealthServer(health grpcHealthSetter, service string) GRPCServeOption {
	return func(opts *grpcServeOptions) {
		opts.health = health
		opts.healthService = service
	}
}

func ServeGRPC(ctx context.Context, srv *grpc.Server, ln net.Listener, serviceName string, logger interfaces.Logger, options ...GRPCServeOption) error {
	opts := grpcServeOptions{shutdownTimeout: defaultGRPCShutdownTimeout}
	for _, option := range options {
		if option != nil {
			option(&opts)
		}
	}

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.Serve(ln)
	}()

	select {
	case <-ctx.Done():
		if logger != nil {
			logger.Info("Shutting down %s gRPC server...", serviceName)
		}

		markGRPCNotServing(opts)
		stopGRPCWithTimeout(srv, serviceName, opts.shutdownTimeout, logger)
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

func markGRPCNotServing(opts grpcServeOptions) {
	if opts.health == nil {
		return
	}

	opts.health.SetServingStatus(opts.healthService, healthpb.HealthCheckResponse_NOT_SERVING)
}

func stopGRPCWithTimeout(srv *grpc.Server, serviceName string, timeout time.Duration, logger interfaces.Logger) {
	if timeout <= 0 {
		timeout = defaultGRPCShutdownTimeout
	}

	stopped := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(stopped)
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-stopped:
	case <-timer.C:
		if logger != nil {
			logger.Warn("%s gRPC graceful shutdown exceeded %v; forcing stop", serviceName, timeout)
		}
		srv.Stop()
		<-stopped
	}
}
