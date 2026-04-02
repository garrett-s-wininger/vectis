package resolver

import (
	"context"
	"fmt"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/backoff"
	"vectis/internal/interfaces"
	"vectis/internal/registry"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"

	_ "google.golang.org/grpc/health"
)

const (
	pinnedDialMaxTries  = 5
	pinnedDialBaseDelay = 500 * time.Millisecond
)

func NewClientWithPinnedAddress(ctx context.Context, comp api.Component, addr string, logger interfaces.Logger, clock interfaces.Clock) (*grpc.ClientConn, func(), error) {
	if clock == nil {
		clock = interfaces.SystemClock{}
	}

	serviceName := comp.String()
	target := fmt.Sprintf("static:///%s", addr)
	logger.Debug("resolver: connecting to %s at %s (pinned)", serviceName, target)

	staticB := &staticBuilder{addr: addr, logger: logger}

	opts := []grpc.DialOption{
		grpc.WithResolvers(staticB),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"pick_first"}`),
	}

	var conn *grpc.ClientConn
	retryer := backoff.NewRetryer(backoff.RetryConfig{
		MaxTries:  pinnedDialMaxTries,
		BaseDelay: pinnedDialBaseDelay,
		Clock:     clock,
	})

	err := retryer.Do(ctx, func() error {
		if conn != nil {
			_ = conn.Close()
			conn = nil
		}

		var e error
		conn, e = grpc.NewClient(target, opts...)
		if e != nil {
			return e
		}

		if e = waitForConnReady(ctx, conn); e != nil {
			_ = conn.Close()
			conn = nil
			return e
		}

		return nil
	}, func(attempt int, nextDelay time.Duration, err error) {
		if attempt == 1 {
			logger.Warn("resolver: could not reach pinned %s at %s: %v (retries at debug)", serviceName, addr, err)
		} else {
			logger.Debug("resolver: pinned connect to %s at %s (attempt %d/%d): %v; retrying in %v",
				serviceName, addr, attempt, pinnedDialMaxTries, err, nextDelay)
		}
	})

	if err != nil {
		return nil, nil, fmt.Errorf("resolver: failed to connect to %s at %s: %w", serviceName, addr, err)
	}

	return conn, func() { conn.Close() }, nil
}

func NewRegistryClient(ctx context.Context, addr string, logger interfaces.Logger, clock interfaces.Clock) (*registry.Registry, error) {
	return registry.New(ctx, addr, logger, clock)
}

func NewClientWithRegistry(ctx context.Context, comp api.Component, logger interfaces.Logger, regClient *registry.Registry) (*grpc.ClientConn, func(), error) {
	if addr := pinnedAddress(comp); addr != "" {
		return NewClientWithPinnedAddress(ctx, comp, addr, logger, nil)
	}

	builder := BuildResolver(comp, regClient, logger)
	target := BuildTarget(comp)

	conn, cleanup, err := dialWithResolver(ctx, comp, target, builder, logger)
	if err != nil {
		return nil, nil, err
	}

	return conn, cleanup, nil
}

func grpcHealthServiceName(comp api.Component) string {
	switch comp {
	case api.Component_COMPONENT_QUEUE:
		return "queue"
	case api.Component_COMPONENT_LOG:
		return "log"
	default:
		return comp.String()
	}
}

func grpcDefaultServiceConfigJSON(comp api.Component) string {
	return fmt.Sprintf(
		`{"loadBalancingPolicy": "round_robin", "healthCheckConfig": {"serviceName": "%s"}}`,
		grpcHealthServiceName(comp),
	)
}

func dialWithResolver(ctx context.Context, comp api.Component, target string, builder resolver.Builder, logger interfaces.Logger) (*grpc.ClientConn, func(), error) {
	logger.Debug("resolver: connecting to %s at %s", comp.String(), target)

	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithResolvers(builder),
		grpc.WithDefaultServiceConfig(grpcDefaultServiceConfigJSON(comp)),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("resolver: failed to connect to %s: %w", comp.String(), err)
	}

	if err := waitForConnReady(ctx, conn); err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("resolver: %s not ready: %w", comp.String(), err)
	}

	return conn, func() { conn.Close() }, nil
}
