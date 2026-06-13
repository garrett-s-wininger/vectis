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
	SecretsResolveOutcomeSuccess  = "success"
	SecretsResolveOutcomeDenied   = "denied"
	SecretsResolveOutcomeNotFound = "not_found"
	SecretsResolveOutcomeFailed   = "failed"

	SecretsResolveReasonOK                  = "ok"
	SecretsResolveReasonUnknown             = "unknown"
	SecretsResolveReasonMissingProvider     = "missing_provider"
	SecretsResolveReasonMissingAuthorizer   = "missing_authorizer"
	SecretsResolveReasonAuthorizationDenied = "authorization_denied"
	SecretsResolveReasonProviderDenied      = "provider_denied"
	SecretsResolveReasonProviderNotFound    = "provider_not_found"
	SecretsResolveReasonProviderError       = "provider_error"
	SecretsResolveReasonInvalidBundle       = "invalid_bundle"
)

type SecretsMetrics struct {
	resolveRequests metric.Int64Counter
	resolveDuration metric.Float64Histogram
}

func NewSecretsMetrics() (*SecretsMetrics, error) {
	m := otel.Meter("vectis/secrets")

	resolveRequests, err := m.Int64Counter("vectis_secrets_resolve_requests_total",
		metric.WithDescription("Secret resolve RPCs by outcome, reason, and provider"),
		metric.WithUnit("{request}"))
	if err != nil {
		return nil, fmt.Errorf("vectis_secrets_resolve_requests_total: %w", err)
	}

	resolveDuration, err := m.Float64Histogram("vectis_secrets_resolve_duration_seconds",
		metric.WithDescription("Wall time for secret resolve RPCs by outcome, reason, and provider"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5))
	if err != nil {
		return nil, fmt.Errorf("vectis_secrets_resolve_duration_seconds: %w", err)
	}

	return &SecretsMetrics{
		resolveRequests: resolveRequests,
		resolveDuration: resolveDuration,
	}, nil
}

func (m *SecretsMetrics) RecordResolve(ctx context.Context, outcome, reason, provider string, duration time.Duration) {
	if m == nil {
		return
	}

	if outcome == "" {
		outcome = SecretsResolveOutcomeFailed
	}

	if reason == "" {
		reason = SecretsResolveReasonUnknown
	}

	if provider == "" {
		provider = "unknown"
	}

	attrs := metric.WithAttributes(
		attribute.String("outcome", outcome),
		attribute.String("reason", reason),
		attribute.String("provider", provider),
	)

	m.resolveRequests.Add(ctx, 1, attrs)
	m.resolveDuration.Record(ctx, duration.Seconds(), attrs)
}
