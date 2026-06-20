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

type DialOptions struct {
	QueueDequeueSupportedIsolation  []string
	QueueDequeueStickySuccessBudget int
}

func DialQueueAndLog(ctx context.Context, logger interfaces.Logger, retryMetrics backoff.RetryMetrics, assignmentStore logclient.AssignmentStore, routingMetrics logclient.RoutingMetrics) (interfaces.QueueClient, interfaces.LogClient, func(), error) {
	return DialQueueAndLogWithOptions(ctx, logger, retryMetrics, assignmentStore, routingMetrics, DialOptions{})
}

func DialQueueAndLogWithOptions(ctx context.Context, logger interfaces.Logger, retryMetrics backoff.RetryMetrics, assignmentStore logclient.AssignmentStore, routingMetrics logclient.RoutingMetrics, opts DialOptions) (interfaces.QueueClient, interfaces.LogClient, func(), error) {
	qPin := config.PinnedQueueAddress()
	lPin := config.PinnedLogAddress()

	queuePool, err := queueclient.NewManagingQueuePoolClient(ctx, logger, queueclient.QueuePoolOptions{
		PinnedAddress:              qPin,
		RegistryAddress:            config.WorkerRegistryDialAddress(),
		RetryMetrics:               retryMetrics,
		DequeueSupportedIsolation:  opts.QueueDequeueSupportedIsolation,
		DequeueStickySuccessBudget: opts.QueueDequeueStickySuccessBudget,
	})

	if err != nil {
		return nil, nil, nil, fmt.Errorf("queue client: %w", err)
	}

	logPool, err := logclient.NewManagingLogClient(ctx, logger, logclient.PoolOptions{
		PinnedAddress:   lPin,
		RegistryAddress: config.WorkerRegistryDialAddress(),
		RetryMetrics:    retryMetrics,
		AssignmentStore: assignmentStore,
		Metrics:         routingMetrics,
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
