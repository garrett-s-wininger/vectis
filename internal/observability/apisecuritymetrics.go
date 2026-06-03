package observability

import (
	"context"
	"fmt"
	"strconv"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type APISecurityMetrics struct {
	rejections metric.Int64Counter
}

func NewAPISecurityMetrics() (*APISecurityMetrics, error) {
	m := otel.Meter("vectis/api/security")

	rejections, err := m.Int64Counter("vectis_api_security_rejections_total",
		metric.WithDescription("Total API requests rejected by web security controls"),
		metric.WithUnit("{request}"))
	if err != nil {
		return nil, fmt.Errorf("vectis_api_security_rejections_total: %w", err)
	}

	return &APISecurityMetrics{rejections: rejections}, nil
}

func (m *APISecurityMetrics) RecordSecurityRejection(ctx context.Context, reason, route string, status int) {
	if m == nil || reason == "" || route == "" || status <= 0 {
		return
	}

	m.rejections.Add(ctx, 1, metric.WithAttributes(
		attribute.String("reason", reason),
		attribute.String("route", route),
		attribute.String("status", strconv.Itoa(status)),
	))
}
