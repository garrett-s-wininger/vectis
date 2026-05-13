package multidial

import (
	"context"
	"fmt"

	api "vectis/api/gen/go"
	"vectis/internal/backoff"
	"vectis/internal/config"
	"vectis/internal/interfaces"
	"vectis/internal/registry"
	"vectis/internal/resolver"

	"google.golang.org/grpc"
)

func DialQueueAndLog(ctx context.Context, logger interfaces.Logger, retryMetrics backoff.RetryMetrics) (interfaces.QueueClient, interfaces.LogClient, func(), error) {
	qPin := config.PinnedQueueAddress()
	lPin := config.PinnedLogAddress()

	var regClient *registry.Registry
	if qPin == "" || lPin == "" {
		var err error
		regClient, err = resolver.NewRegistryClient(ctx, config.WorkerRegistryDialAddress(), logger, interfaces.SystemClock{}, retryMetrics)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("registry client: %w", err)
		}
	}

	queueConn, queueCleanup, err := dialComponent(ctx, logger, regClient, qPin, api.Component_COMPONENT_QUEUE, retryMetrics)
	if err != nil {
		if regClient != nil {
			_ = regClient.Close()
		}

		return nil, nil, nil, fmt.Errorf("queue client: %w", err)
	}

	logConn, logCleanup, err := dialComponent(ctx, logger, regClient, lPin, api.Component_COMPONENT_LOG, retryMetrics)
	if err != nil {
		queueCleanup()
		if regClient != nil {
			_ = regClient.Close()
		}

		return nil, nil, nil, fmt.Errorf("log client: %w", err)
	}

	return interfaces.NewGRPCQueueClient(queueConn), interfaces.NewGRPCLogClient(logConn), func() {
		queueCleanup()
		logCleanup()
		if regClient != nil {
			_ = regClient.Close()
		}
	}, nil
}

func dialComponent(ctx context.Context, logger interfaces.Logger, reg *registry.Registry, pinned string, comp api.Component, retryMetrics backoff.RetryMetrics) (*grpc.ClientConn, func(), error) {
	if pinned != "" {
		return resolver.NewClientWithPinnedAddress(ctx, comp, pinned, logger, nil, retryMetrics)
	}

	if reg == nil {
		return nil, nil, fmt.Errorf("registry client required for %s discovery", comp.String())
	}

	return resolver.NewClientWithRegistry(ctx, comp, logger, reg, retryMetrics)
}
