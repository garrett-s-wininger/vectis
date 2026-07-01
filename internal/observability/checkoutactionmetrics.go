package observability

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	CheckoutActionStrategyCache      = "cache"
	CheckoutActionStrategyDirect     = "direct"
	CheckoutActionStrategyValidation = "validation"

	CheckoutActionOutcomeSuccess = "success"
	CheckoutActionOutcomeFailed  = "failed"

	CheckoutActionCacheOutcomeHit     = "hit"
	CheckoutActionCacheOutcomeMiss    = "miss"
	CheckoutActionCacheOutcomeFailed  = "failed"
	CheckoutActionCacheOutcomeSkipped = "skipped"
	CheckoutActionCacheOutcomeUnknown = "unknown"

	CheckoutActionReasonOK                   = "ok"
	CheckoutActionReasonMissingURL           = "missing_url"
	CheckoutActionReasonCredentialedURL      = "credentialed_url"
	CheckoutActionReasonInvalidFetchRefspecs = "invalid_fetch_refspecs"
	CheckoutActionReasonNoCache              = "no_cache"
	CheckoutActionReasonCacheError           = "cache_error"
	CheckoutActionReasonStartFailed          = "start_failed"
	CheckoutActionReasonGitCloneFailed       = "git_clone_failed"
	CheckoutActionReasonGitFetchFailed       = "git_fetch_failed"
	CheckoutActionReasonUnknown              = "unknown"
)

type CheckoutActionMetrics struct {
	results            metric.Int64Counter
	duration           metric.Float64Histogram
	cacheChecks        metric.Int64Counter
	cacheCheckDuration metric.Float64Histogram
	directCloneTime    metric.Float64Histogram
}

func NewCheckoutActionMetrics() (*CheckoutActionMetrics, error) {
	m := otel.Meter("vectis/checkout")

	results, err := m.Int64Counter("vectis_checkout_action_results_total",
		metric.WithDescription("Checkout action terminal results by execution strategy, outcome, and reason"),
		metric.WithUnit("{checkout}"))
	if err != nil {
		return nil, fmt.Errorf("vectis_checkout_action_results_total: %w", err)
	}

	duration, err := m.Float64Histogram("vectis_checkout_action_duration_seconds",
		metric.WithDescription("Wall time spent executing checkout actions"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1200, 1800, 3600))
	if err != nil {
		return nil, fmt.Errorf("vectis_checkout_action_duration_seconds: %w", err)
	}

	cacheChecks, err := m.Int64Counter("vectis_checkout_action_cache_checks_total",
		metric.WithDescription("Checkout action worker-core cache checks by outcome and reason"),
		metric.WithUnit("{check}"))
	if err != nil {
		return nil, fmt.Errorf("vectis_checkout_action_cache_checks_total: %w", err)
	}

	cacheCheckDuration, err := m.Float64Histogram("vectis_checkout_action_cache_check_duration_seconds",
		metric.WithDescription("Wall time spent checking the worker-core checkout cache"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1200, 1800))
	if err != nil {
		return nil, fmt.Errorf("vectis_checkout_action_cache_check_duration_seconds: %w", err)
	}

	directCloneTime, err := m.Float64Histogram("vectis_checkout_action_direct_clone_duration_seconds",
		metric.WithDescription("Wall time spent in direct git clone execution after cache miss or cache bypass"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1200, 1800, 3600))
	if err != nil {
		return nil, fmt.Errorf("vectis_checkout_action_direct_clone_duration_seconds: %w", err)
	}

	return &CheckoutActionMetrics{
		results:            results,
		duration:           duration,
		cacheChecks:        cacheChecks,
		cacheCheckDuration: cacheCheckDuration,
		directCloneTime:    directCloneTime,
	}, nil
}

func (m *CheckoutActionMetrics) RecordCheckoutActionResult(ctx context.Context, strategy, outcome, reason string, d time.Duration) {
	if m == nil {
		return
	}

	if strategy == "" {
		strategy = CheckoutActionStrategyDirect
	}
	if outcome == "" {
		outcome = CheckoutActionOutcomeFailed
	}
	if reason == "" {
		reason = CheckoutActionReasonUnknown
	}

	attrs := checkoutActionResultAttributes(strategy, outcome, reason)
	m.results.Add(ctx, 1, metric.WithAttributes(attrs...))
	m.duration.Record(ctx, max(d.Seconds(), 0), metric.WithAttributes(attrs...))
}

func (m *CheckoutActionMetrics) RecordCheckoutActionCacheCheck(ctx context.Context, outcome, reason string, d time.Duration) {
	if m == nil {
		return
	}

	if outcome == "" {
		outcome = CheckoutActionCacheOutcomeFailed
	}
	if reason == "" {
		reason = CheckoutActionReasonUnknown
	}

	attrs := checkoutActionCacheCheckAttributes(outcome, reason)
	m.cacheChecks.Add(ctx, 1, metric.WithAttributes(attrs...))
	m.cacheCheckDuration.Record(ctx, max(d.Seconds(), 0), metric.WithAttributes(attrs...))
}

func (m *CheckoutActionMetrics) RecordCheckoutActionDirectClone(ctx context.Context, cacheState, outcome, reason string, d time.Duration) {
	if m == nil {
		return
	}

	if cacheState == "" {
		cacheState = CheckoutActionCacheOutcomeUnknown
	}
	if outcome == "" {
		outcome = CheckoutActionOutcomeFailed
	}
	if reason == "" {
		reason = CheckoutActionReasonUnknown
	}

	attrs := checkoutActionDirectCloneAttributes(cacheState, outcome, reason)
	m.directCloneTime.Record(ctx, max(d.Seconds(), 0), metric.WithAttributes(attrs...))
}

func checkoutActionResultAttributes(strategy, outcome, reason string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("strategy", strategy),
		attribute.String("outcome", outcome),
		attribute.String("reason", reason),
	}
}

func checkoutActionDirectCloneAttributes(cacheState, outcome, reason string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("cache_state", cacheState),
		attribute.String("outcome", outcome),
		attribute.String("reason", reason),
	}
}

func checkoutActionCacheCheckAttributes(outcome, reason string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("outcome", outcome),
		attribute.String("reason", reason),
	}
}
