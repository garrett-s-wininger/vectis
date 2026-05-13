package resolver

import (
	"context"

	api "vectis/api/gen/go"
	"vectis/internal/backoff"
	"vectis/internal/interfaces"

	"google.golang.org/grpc"
)

func DialQueue(ctx context.Context, logger interfaces.Logger, pinnedQueueAddr, registryDialAddr string, retryMetrics backoff.RetryMetrics) (*grpc.ClientConn, func(), error) {
	if pinnedQueueAddr != "" {
		logger.Info("Using pinned queue address: %s", pinnedQueueAddr)
		return NewClientWithPinnedAddress(ctx, api.Component_COMPONENT_QUEUE, pinnedQueueAddr, logger, nil, retryMetrics)
	}

	regClient, err := NewRegistryClient(ctx, registryDialAddr, logger, interfaces.SystemClock{}, retryMetrics)
	if err != nil {
		return nil, nil, err
	}

	conn, cleanup, err := NewClientWithRegistry(ctx, api.Component_COMPONENT_QUEUE, logger, regClient, retryMetrics)
	if err != nil {
		_ = regClient.Close()
		return nil, nil, err
	}

	return conn, func() { cleanup(); _ = regClient.Close() }, nil
}
