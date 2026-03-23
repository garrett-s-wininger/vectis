package resolver

import (
	"context"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"

	"google.golang.org/grpc"
)

func DialQueue(ctx context.Context, logger interfaces.Logger, pinnedQueueAddr, registryDialAddr string) (*grpc.ClientConn, func(), error) {
	if pinnedQueueAddr != "" {
		logger.Info("Using pinned queue address: %s", pinnedQueueAddr)
		return NewClientWithPinnedAddress(ctx, api.Component_COMPONENT_QUEUE, pinnedQueueAddr, logger, nil)
	}

	regClient, err := NewRegistryClient(ctx, registryDialAddr, logger, interfaces.SystemClock{})
	if err != nil {
		return nil, nil, err
	}

	conn, cleanup, err := NewClientWithRegistry(ctx, api.Component_COMPONENT_QUEUE, logger, regClient)
	if err != nil {
		regClient.Close()
		return nil, nil, err
	}

	return conn, func() { cleanup(); regClient.Close() }, nil
}
