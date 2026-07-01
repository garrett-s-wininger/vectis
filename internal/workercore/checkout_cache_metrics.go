package workercore

import (
	"context"
	"sync"

	"vectis/internal/observability"
	"vectis/internal/source"
)

var (
	checkoutCacheMetricsOnce sync.Once
	checkoutCacheMetrics     *observability.CheckoutCacheMetrics
	checkoutCacheMetricsErr  error
)

func recordCheckoutCacheStats(ctx context.Context, stats source.WorkerCheckoutCacheStats) {
	metrics := workerCheckoutCacheMetrics()
	if metrics == nil {
		return
	}

	metrics.RecordCheckoutCacheStats(ctx, observability.CheckoutCacheStats{
		Repositories: stats.Repositories,
		Generations:  stats.Generations,
		PackFiles:    stats.PackFiles,
		PackBytes:    stats.PackBytes,
		ActiveLeases: stats.ActiveLeases,
	})
}

func workerCheckoutCacheMetrics() *observability.CheckoutCacheMetrics {
	checkoutCacheMetricsOnce.Do(func() {
		checkoutCacheMetrics, checkoutCacheMetricsErr = observability.NewCheckoutCacheMetrics()
	})

	if checkoutCacheMetricsErr != nil {
		return nil
	}

	return checkoutCacheMetrics
}
