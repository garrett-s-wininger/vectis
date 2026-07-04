package observability

import (
	"context"
	"fmt"
	"strings"
	"sync"
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
	objectStorePressure  metric.Int64ObservableGauge
	objectStorePackFiles metric.Int64ObservableGauge
	objectStorePackBytes metric.Int64ObservableGauge
	objectStoreLoose     metric.Int64ObservableGauge
	objectStoreHydrated  metric.Int64ObservableGauge
	objectStoreWarnings  metric.Int64ObservableGauge
	objectStoreMu        sync.RWMutex
	objectStore          map[string]sourceRepositoryObjectStoreMetricRecord
}

type SourceRepositoryObjectStoreWarning struct {
	Code     string
	Severity string
}

type sourceRepositoryObjectStoreMetricRecord struct {
	repositoryID string
	sourceKind   string
	checkoutMode string
	pressure     string
	packFiles    int
	packBytes    int64
	looseObjects int
	hydratedRefs int
	warnings     []SourceRepositoryObjectStoreWarning
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

	objectStorePressure, err := m.Int64ObservableGauge("vectis_source_repository_object_store_pressure",
		metric.WithDescription("Latest source repository Git object-store pressure classification as 0=ok, 1=warning, 2=critical"),
		metric.WithUnit("{level}"))
	if err != nil {
		return nil, fmt.Errorf("vectis_source_repository_object_store_pressure: %w", err)
	}

	objectStorePackFiles, err := m.Int64ObservableGauge("vectis_source_repository_object_store_pack_files",
		metric.WithDescription("Latest source repository Git object-store pack file count"),
		metric.WithUnit("{pack_file}"))
	if err != nil {
		return nil, fmt.Errorf("vectis_source_repository_object_store_pack_files: %w", err)
	}

	objectStorePackBytes, err := m.Int64ObservableGauge("vectis_source_repository_object_store_pack_bytes",
		metric.WithDescription("Latest source repository Git object-store pack bytes"),
		metric.WithUnit("By"))
	if err != nil {
		return nil, fmt.Errorf("vectis_source_repository_object_store_pack_bytes: %w", err)
	}

	objectStoreLoose, err := m.Int64ObservableGauge("vectis_source_repository_object_store_loose_objects",
		metric.WithDescription("Latest source repository Git object-store loose object count"),
		metric.WithUnit("{object}"))
	if err != nil {
		return nil, fmt.Errorf("vectis_source_repository_object_store_loose_objects: %w", err)
	}

	objectStoreHydrated, err := m.Int64ObservableGauge("vectis_source_repository_object_store_hydrated_refs",
		metric.WithDescription("Latest source repository Git Vectis-hydrated ref count"),
		metric.WithUnit("{ref}"))

	if err != nil {
		return nil, fmt.Errorf("vectis_source_repository_object_store_hydrated_refs: %w", err)
	}

	objectStoreWarnings, err := m.Int64ObservableGauge("vectis_source_repository_object_store_warnings",
		metric.WithDescription("Latest source repository Git object-store warning conditions; each active warning is observed as 1"),
		metric.WithUnit("{warning}"))

	if err != nil {
		return nil, fmt.Errorf("vectis_source_repository_object_store_warnings: %w", err)
	}

	metrics := &SourceSyncMetrics{
		syncs:                syncs,
		syncDuration:         syncDuration,
		refHydrations:        refHydrations,
		refHydrationDuration: refHydrationDuration,
		objectStorePressure:  objectStorePressure,
		objectStorePackFiles: objectStorePackFiles,
		objectStorePackBytes: objectStorePackBytes,
		objectStoreLoose:     objectStoreLoose,
		objectStoreHydrated:  objectStoreHydrated,
		objectStoreWarnings:  objectStoreWarnings,
		objectStore:          make(map[string]sourceRepositoryObjectStoreMetricRecord),
	}

	_, err = m.RegisterCallback(metrics.observeSourceRepositoryObjectStoreMetrics,
		objectStorePressure,
		objectStorePackFiles,
		objectStorePackBytes,
		objectStoreLoose,
		objectStoreHydrated,
		objectStoreWarnings,
	)

	if err != nil {
		return nil, fmt.Errorf("source repository object-store metrics callback: %w", err)
	}

	return metrics, nil
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

func (m *SourceSyncMetrics) RecordSourceRepositoryObjectStore(_ context.Context, repositoryID, sourceKind, checkoutMode, pressure string, packFiles int, packBytes int64, looseObjects, hydratedRefs int, warnings []SourceRepositoryObjectStoreWarning) {
	if m == nil {
		return
	}

	repositoryID = sourceSyncMetricLabel(repositoryID, "unknown")
	rec := sourceRepositoryObjectStoreMetricRecord{
		repositoryID: repositoryID,
		sourceKind:   sourceSyncMetricLabel(sourceKind, "unknown"),
		checkoutMode: sourceSyncMetricLabel(checkoutMode, "unknown"),
		pressure:     sourceSyncMetricLabel(pressure, "ok"),
		packFiles:    max(packFiles, 0),
		packBytes:    max(packBytes, 0),
		looseObjects: max(looseObjects, 0),
		hydratedRefs: max(hydratedRefs, 0),
		warnings:     normalizeSourceRepositoryObjectStoreMetricWarnings(warnings),
	}

	m.objectStoreMu.Lock()
	if m.objectStore == nil {
		m.objectStore = make(map[string]sourceRepositoryObjectStoreMetricRecord)
	}

	m.objectStore[repositoryID] = rec
	m.objectStoreMu.Unlock()
}

func (m *SourceSyncMetrics) observeSourceRepositoryObjectStoreMetrics(_ context.Context, o metric.Observer) error {
	m.objectStoreMu.RLock()
	records := make([]sourceRepositoryObjectStoreMetricRecord, 0, len(m.objectStore))
	for _, rec := range m.objectStore {
		records = append(records, rec)
	}
	m.objectStoreMu.RUnlock()

	for _, rec := range records {
		attrs := sourceRepositoryObjectStoreMetricAttributes(rec.repositoryID, rec.sourceKind, rec.checkoutMode)
		o.ObserveInt64(m.objectStorePressure, sourceRepositoryObjectStorePressureRank(rec.pressure),
			metric.WithAttributes(append(attrs, attribute.String("pressure", rec.pressure))...))
		o.ObserveInt64(m.objectStorePackFiles, int64(rec.packFiles), metric.WithAttributes(attrs...))
		o.ObserveInt64(m.objectStorePackBytes, rec.packBytes, metric.WithAttributes(attrs...))
		o.ObserveInt64(m.objectStoreLoose, int64(rec.looseObjects), metric.WithAttributes(attrs...))
		o.ObserveInt64(m.objectStoreHydrated, int64(rec.hydratedRefs), metric.WithAttributes(attrs...))

		for _, warning := range rec.warnings {
			warningAttrs := append(append([]attribute.KeyValue(nil), attrs...),
				attribute.String("code", warning.Code),
				attribute.String("severity", warning.Severity),
			)

			o.ObserveInt64(m.objectStoreWarnings, 1, metric.WithAttributes(warningAttrs...))
		}
	}

	return nil
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

func sourceRepositoryObjectStoreMetricAttributes(repositoryID, sourceKind, checkoutMode string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("repository_id", sourceSyncMetricLabel(repositoryID, "unknown")),
		attribute.String("source_kind", sourceSyncMetricLabel(sourceKind, "unknown")),
		attribute.String("checkout_mode", sourceSyncMetricLabel(checkoutMode, "unknown")),
	}
}

func normalizeSourceRepositoryObjectStoreMetricWarnings(in []SourceRepositoryObjectStoreWarning) []SourceRepositoryObjectStoreWarning {
	if len(in) == 0 {
		return nil
	}

	out := make([]SourceRepositoryObjectStoreWarning, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, warning := range in {
		code := sourceSyncMetricLabel(warning.Code, SourceSyncReasonUnknown)
		severity := sourceSyncMetricLabel(warning.Severity, "warning")
		key := code + "\x00" + severity
		if _, ok := seen[key]; ok {
			continue
		}

		seen[key] = struct{}{}
		out = append(out, SourceRepositoryObjectStoreWarning{
			Code:     code,
			Severity: severity,
		})
	}

	return out
}

func sourceRepositoryObjectStorePressureRank(pressure string) int64 {
	switch strings.TrimSpace(pressure) {
	case "critical":
		return 2
	case "warning":
		return 1
	default:
		return 0
	}
}

func sourceSyncMetricLabel(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}

	return value
}
