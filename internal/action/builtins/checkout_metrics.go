package builtins

import (
	"context"
	"sync"
	"time"

	"vectis/internal/observability"
)

var (
	checkoutActionMetricsOnce sync.Once
	checkoutActionMetrics     *observability.CheckoutActionMetrics
	checkoutActionMetricsErr  error
)

func recordCheckoutActionResult(ctx context.Context, strategy, outcome, reason string, d time.Duration) {
	metrics := checkoutMetrics()
	if metrics == nil {
		return
	}

	metrics.RecordCheckoutActionResult(ctx, strategy, outcome, reason, d)
}

func recordCheckoutActionCacheCheck(ctx context.Context, outcome, reason string, d time.Duration) {
	metrics := checkoutMetrics()
	if metrics == nil {
		return
	}

	metrics.RecordCheckoutActionCacheCheck(ctx, outcome, reason, d)
}

func checkoutMetrics() *observability.CheckoutActionMetrics {
	checkoutActionMetricsOnce.Do(func() {
		checkoutActionMetrics, checkoutActionMetricsErr = observability.NewCheckoutActionMetrics()
	})

	if checkoutActionMetricsErr != nil {
		return nil
	}

	return checkoutActionMetrics
}
