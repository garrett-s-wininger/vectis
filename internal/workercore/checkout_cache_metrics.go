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

func recordCheckoutCacheClone(ctx context.Context, mode, reason string) {
	metrics := workerCheckoutCacheMetrics()
	if metrics == nil {
		return
	}

	metrics.RecordCheckoutCacheClone(ctx, mode, reason)
}

func recordCheckoutCacheDemandHydration(ctx context.Context, outcome string) {
	metrics := workerCheckoutCacheMetrics()
	if metrics == nil {
		return
	}

	metrics.RecordCheckoutCacheDemandHydration(ctx, outcome)
}

func recordCheckoutCacheGenerationEviction(ctx context.Context, reason string) {
	metrics := workerCheckoutCacheMetrics()
	if metrics == nil {
		return
	}

	metrics.RecordCheckoutCacheGenerationEviction(ctx, reason)
}

func recordCheckoutCacheSelfHeal(ctx context.Context, operation, outcome string) {
	metrics := workerCheckoutCacheMetrics()
	if metrics == nil {
		return
	}

	metrics.RecordCheckoutCacheSelfHeal(ctx, operation, outcome)
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
