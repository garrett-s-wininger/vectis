package observability

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type LogRoutingMetrics struct {
	shardAssignments metric.Int64Counter
	routeFailures    metric.Int64Counter
}

func NewLogRoutingMetrics() (*LogRoutingMetrics, error) {
	m := otel.Meter("vectis/log-routing")

	assignments, err := m.Int64Counter("vectis_log_shard_assignments_total",
		metric.WithDescription("Log shard assignment decisions by outcome"),
		metric.WithUnit("{assignment}"))
	if err != nil {
		return nil, fmt.Errorf("vectis_log_shard_assignments_total: %w", err)
	}

	failures, err := m.Int64Counter("vectis_log_shard_route_failures_total",
		metric.WithDescription("Log shard routing failures by operation and reason"),
		metric.WithUnit("{failure}"))
	if err != nil {
		return nil, fmt.Errorf("vectis_log_shard_route_failures_total: %w", err)
	}

	return &LogRoutingMetrics{
		shardAssignments: assignments,
		routeFailures:    failures,
	}, nil
}

func (m *LogRoutingMetrics) RecordShardAssignment(ctx context.Context, outcome string) {
	if m == nil {
		return
	}

	if outcome == "" {
		outcome = "unknown"
	}

	m.shardAssignments.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcome)))
}

func (m *LogRoutingMetrics) RecordShardRouteFailure(ctx context.Context, operation, reason string) {
	if m == nil {
		return
	}

	if operation == "" {
		operation = "unknown"
	}

	if reason == "" {
		reason = "unknown"
	}

	m.routeFailures.Add(ctx, 1, metric.WithAttributes(
		attribute.String("operation", operation),
		attribute.String("reason", reason),
	))
}
