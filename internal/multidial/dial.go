package multidial

import (
	"context"
	"fmt"

	"vectis/internal/backoff"
	"vectis/internal/config"
	"vectis/internal/interfaces"
	"vectis/internal/logclient"
	"vectis/internal/queueclient"
)

func DialQueueAndLog(ctx context.Context, logger interfaces.Logger, retryMetrics backoff.RetryMetrics) (interfaces.QueueClient, interfaces.LogClient, func(), error) {
	qPin := config.PinnedQueueAddress()
	lPin := config.PinnedLogAddress()

	queuePool, err := queueclient.NewManagingQueuePoolClient(ctx, logger, queueclient.QueuePoolOptions{
		PinnedAddress:   qPin,
		RegistryAddress: config.WorkerRegistryDialAddress(),
		RetryMetrics:    retryMetrics,
	})

	if err != nil {
		return nil, nil, nil, fmt.Errorf("queue client: %w", err)
	}

	logPool, err := logclient.NewManagingLogClient(ctx, logger, logclient.PoolOptions{
		PinnedAddress:   lPin,
		RegistryAddress: config.WorkerRegistryDialAddress(),
		RetryMetrics:    retryMetrics,
	})

	if err != nil {
		_ = queuePool.Close()
		return nil, nil, nil, fmt.Errorf("log client: %w", err)
	}

	return queuePool, logPool, func() {
		_ = queuePool.Close()
		_ = logPool.Close()
	}, nil
}
