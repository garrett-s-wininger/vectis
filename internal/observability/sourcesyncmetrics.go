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

const (
	SourceSyncTriggerManual   = "manual"
	SourceSyncTriggerPeriodic = "periodic"
	SourceSyncTriggerStartup  = "startup"

	SourceSyncOutcomeAlreadyRunning = "already_running"
	SourceSyncOutcomeFailed         = "failed"
	SourceSyncOutcomeSucceeded      = "succeeded"

	SourceRefHydrationCacheHit  = "hit"
	SourceRefHydrationCacheMiss = "miss"

	SourceSyncReasonNone                  = "none"
	SourceSyncReasonContextCanceled       = "context_canceled"
	SourceSyncReasonContextDeadline       = "context_deadline_exceeded"
	SourceSyncReasonDatabaseBeginFailed   = "db_begin_failed"
	SourceSyncReasonDatabaseUpdateFailed  = "db_update_failed"
	SourceSyncReasonDatabaseLock          = "database_lock"
	SourceSyncReasonInMemoryLock          = "in_memory_lock"
	SourceSyncReasonUnsupportedSourceKind = "unsupported_source_kind"
	SourceSyncReasonUnknown               = "unknown"
)

type SourceSyncMetrics struct {
	syncs                metric.Int64Counter
	syncDuration         metric.Float64Histogram
	refHydrations        metric.Int64Counter
	refHydrationDuration metric.Float64Histogram
}

func NewSourceSyncMetrics() (*SourceSyncMetrics, error) {
	m := otel.Meter("vectis/source")

	syncs, err := m.Int64Counter("vectis_source_repository_syncs_total",
		metric.WithDescription("Total source repository sync attempts by trigger, repository kind, checkout mode, outcome, and reason"),
		metric.WithUnit("{sync}"))
	if err != nil {
		return nil, fmt.Errorf("vectis_source_repository_syncs_total: %w", err)
	}

	syncDuration, err := m.Float64Histogram("vectis_source_repository_sync_duration_seconds",
		metric.WithDescription("Wall time spent handling source repository sync attempts"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300))
	if err != nil {
		return nil, fmt.Errorf("vectis_source_repository_sync_duration_seconds: %w", err)
	}

	refHydrations, err := m.Int64Counter("vectis_source_ref_hydrations_total",
		metric.WithDescription("Total managed source ref hydration attempts by repository kind, checkout mode, outcome, reason, tier, and cache state"),
		metric.WithUnit("{hydration}"))
	if err != nil {
		return nil, fmt.Errorf("vectis_source_ref_hydrations_total: %w", err)
	}

	refHydrationDuration, err := m.Float64Histogram("vectis_source_ref_hydration_duration_seconds",
		metric.WithDescription("Wall time spent hydrating missing managed source refs"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300))
	if err != nil {
		return nil, fmt.Errorf("vectis_source_ref_hydration_duration_seconds: %w", err)
	}

	return &SourceSyncMetrics{
		syncs:                syncs,
		syncDuration:         syncDuration,
		refHydrations:        refHydrations,
		refHydrationDuration: refHydrationDuration,
	}, nil
}

func (m *SourceSyncMetrics) RecordSourceRepositorySync(ctx context.Context, trigger, sourceKind, checkoutMode, outcome, reason string, d time.Duration) {
	if m == nil {
		return
	}

	attrs := sourceSyncMetricAttributes(trigger, sourceKind, checkoutMode, outcome, reason)
	m.syncs.Add(ctx, 1, metric.WithAttributes(attrs...))
	m.syncDuration.Record(ctx, max(d.Seconds(), 0), metric.WithAttributes(attrs...))
}

func (m *SourceSyncMetrics) RecordSourceRefHydration(ctx context.Context, sourceKind, checkoutMode, outcome, reason, tier, cacheState string, d time.Duration) {
	if m == nil {
		return
	}

	attrs := sourceRefHydrationMetricAttributes(sourceKind, checkoutMode, outcome, reason, tier, cacheState)
	m.refHydrations.Add(ctx, 1, metric.WithAttributes(attrs...))
	m.refHydrationDuration.Record(ctx, max(d.Seconds(), 0), metric.WithAttributes(attrs...))
}

func SourceSyncReasonFromErrorCode(code string) string {
	code = strings.TrimSpace(code)
	if code == "" {
		return SourceSyncReasonNone
	}

	switch code {
	case "context canceled":
		return SourceSyncReasonContextCanceled
	case "context deadline exceeded":
		return SourceSyncReasonContextDeadline
	}

	var out strings.Builder
	for _, r := range strings.ToLower(code) {
		switch {
		case r >= 'a' && r <= 'z':
			out.WriteRune(r)
		case r >= '0' && r <= '9':
			out.WriteRune(r)
		case r == '_':
			out.WriteRune(r)
		case r == '-' || r == '.' || r == ' ' || r == ':':
			out.WriteByte('_')
		default:
			out.WriteByte('_')
		}
	}

	reason := strings.Trim(out.String(), "_")
	if reason == "" {
		return SourceSyncReasonUnknown
	}

	for strings.Contains(reason, "__") {
		reason = strings.ReplaceAll(reason, "__", "_")
	}

	return reason
}

func sourceSyncMetricAttributes(trigger, sourceKind, checkoutMode, outcome, reason string) []attribute.KeyValue {
	trigger = sourceSyncMetricLabel(trigger, "unknown")
	sourceKind = sourceSyncMetricLabel(sourceKind, "unknown")
	checkoutMode = sourceSyncMetricLabel(checkoutMode, "unknown")
	outcome = sourceSyncMetricLabel(outcome, SourceSyncOutcomeFailed)
	reason = sourceSyncMetricLabel(reason, SourceSyncReasonUnknown)

	return []attribute.KeyValue{
		attribute.String("trigger", trigger),
		attribute.String("source_kind", sourceKind),
		attribute.String("checkout_mode", checkoutMode),
		attribute.String("outcome", outcome),
		attribute.String("reason", reason),
	}
}

func sourceRefHydrationMetricAttributes(sourceKind, checkoutMode, outcome, reason, tier, cacheState string) []attribute.KeyValue {
	sourceKind = sourceSyncMetricLabel(sourceKind, "unknown")
	checkoutMode = sourceSyncMetricLabel(checkoutMode, "unknown")
	outcome = sourceSyncMetricLabel(outcome, SourceSyncOutcomeFailed)
	reason = sourceSyncMetricLabel(reason, SourceSyncReasonUnknown)
	tier = sourceSyncMetricLabel(tier, "unknown")
	cacheState = sourceSyncMetricLabel(cacheState, SourceRefHydrationCacheMiss)

	return []attribute.KeyValue{
		attribute.String("source_kind", sourceKind),
		attribute.String("checkout_mode", checkoutMode),
		attribute.String("outcome", outcome),
		attribute.String("reason", reason),
		attribute.String("tier", tier),
		attribute.String("cache", cacheState),
	}
}

func sourceSyncMetricLabel(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}

	return value
}
