package observability

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type RetryMetrics struct {
	attempts  metric.Int64Counter
	exhausted metric.Int64Counter
	delayHist metric.Float64Histogram
}

func NewRetryMetrics() (*RetryMetrics, error) {
	m := otel.Meter("vectis/retry")

	attempts, err := m.Int64Counter("vectis_retries_total",
		metric.WithDescription("Total retry attempts"),
		metric.WithUnit("{attempt}"))

	if err != nil {
		return nil, fmt.Errorf("vectis_retries_total: %w", err)
	}

	exhausted, err := m.Int64Counter("vectis_retries_exhausted_total",
		metric.WithDescription("Retry loops that exhausted all attempts without success"),
		metric.WithUnit("{loop}"))

	if err != nil {
		return nil, fmt.Errorf("vectis_retries_exhausted_total: %w", err)
	}

	delayHist, err := m.Float64Histogram("vectis_retry_delay_seconds",
		metric.WithDescription("Observed backoff delay between retry attempts"),
		metric.WithUnit("s"))

	if err != nil {
		return nil, fmt.Errorf("vectis_retry_delay_seconds: %w", err)
	}

	return &RetryMetrics{
		attempts:  attempts,
		exhausted: exhausted,
		delayHist: delayHist,
	}, nil
}

func (rm *RetryMetrics) RecordAttempt(ctx context.Context, component string) {
	if rm == nil {
		return
	}

	rm.attempts.Add(ctx, 1, metric.WithAttributes(retryMetricAttributes(component)...))
}

func (rm *RetryMetrics) RecordExhausted(ctx context.Context, component string) {
	if rm == nil {
		return
	}

	rm.exhausted.Add(ctx, 1, metric.WithAttributes(retryMetricAttributes(component)...))
}

func (rm *RetryMetrics) RecordDelay(ctx context.Context, component string, delay time.Duration) {
	if rm == nil {
		return
	}

	rm.delayHist.Record(ctx, delay.Seconds(), metric.WithAttributes(retryMetricAttributes(component)...))
}

func retryMetricAttributes(component string) []attribute.KeyValue {
	service, operation := splitRetryComponent(component)
	return []attribute.KeyValue{
		attribute.String("component", component),
		attribute.String("service", service),
		attribute.String("operation", operation),
	}
}

func splitRetryComponent(component string) (service, operation string) {
	component = strings.TrimSpace(component)
	if component == "" {
		return "unknown", "retry"
	}

	for _, sep := range []string{".", "/", ":"} {
		if service, operation, ok := strings.Cut(component, sep); ok && service != "" && operation != "" {
			return service, operation
		}
	}

	return component, "retry"
}
