package observability

import (
	"context"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	CheckoutCacheCloneModeHardlink = "hardlink"
	CheckoutCacheCloneModeCopy     = "copy"
	CheckoutCacheCloneModeBorrowed = "borrowed"

	CheckoutCacheCloneReasonOK    = "ok"
	CheckoutCacheCloneReasonProbe = "probe"
	CheckoutCacheCloneReasonRetry = "retry"
)

type CheckoutCacheStats struct {
	Repositories int64
	Generations  int64
	PackFiles    int64
	PackBytes    int64
	ActiveLeases int64
}

type CheckoutCacheMetrics struct {
	repositories metric.Int64ObservableGauge
	generations  metric.Int64ObservableGauge
	packFiles    metric.Int64ObservableGauge
	packBytes    metric.Int64ObservableGauge
	activeLeases metric.Int64ObservableGauge
	clones       metric.Int64Counter
	mu           sync.RWMutex
	stats        CheckoutCacheStats
}

func NewCheckoutCacheMetrics() (*CheckoutCacheMetrics, error) {
	m := otel.Meter("vectis/checkout")

	repositories, err := m.Int64ObservableGauge("vectis_checkout_cache_repositories",
		metric.WithDescription("Worker-core checkout cache persistent repositories with at least one retained mirror generation"),
		metric.WithUnit("{repository}"))
	if err != nil {
		return nil, fmt.Errorf("vectis_checkout_cache_repositories: %w", err)
	}

	generations, err := m.Int64ObservableGauge("vectis_checkout_cache_generations",
		metric.WithDescription("Worker-core checkout cache immutable mirror generations retained across persistent repositories"),
		metric.WithUnit("{generation}"))
	if err != nil {
		return nil, fmt.Errorf("vectis_checkout_cache_generations: %w", err)
	}

	packFiles, err := m.Int64ObservableGauge("vectis_checkout_cache_pack_files",
		metric.WithDescription("Worker-core checkout cache Git pack files across retained mirror generations"),
		metric.WithUnit("{pack_file}"))
	if err != nil {
		return nil, fmt.Errorf("vectis_checkout_cache_pack_files: %w", err)
	}

	packBytes, err := m.Int64ObservableGauge("vectis_checkout_cache_pack_bytes",
		metric.WithDescription("Worker-core checkout cache Git pack bytes across retained mirror generations"),
		metric.WithUnit("By"))
	if err != nil {
		return nil, fmt.Errorf("vectis_checkout_cache_pack_bytes: %w", err)
	}

	activeLeases, err := m.Int64ObservableGauge("vectis_checkout_cache_active_leases",
		metric.WithDescription("Worker-core checkout cache active generation leases held by in-flight checkouts"),
		metric.WithUnit("{lease}"))
	if err != nil {
		return nil, fmt.Errorf("vectis_checkout_cache_active_leases: %w", err)
	}

	clones, err := m.Int64Counter("vectis_checkout_cache_clones_total",
		metric.WithDescription("Worker-core checkout cache workspace clones by local object transfer mode and selection reason"),
		metric.WithUnit("{clone}"))
	if err != nil {
		return nil, fmt.Errorf("vectis_checkout_cache_clones_total: %w", err)
	}

	metrics := &CheckoutCacheMetrics{
		repositories: repositories,
		generations:  generations,
		packFiles:    packFiles,
		packBytes:    packBytes,
		activeLeases: activeLeases,
		clones:       clones,
	}

	_, err = m.RegisterCallback(metrics.observeCheckoutCacheStats,
		repositories,
		generations,
		packFiles,
		packBytes,
		activeLeases,
	)
	if err != nil {
		return nil, fmt.Errorf("checkout cache metrics callback: %w", err)
	}

	return metrics, nil
}

func (m *CheckoutCacheMetrics) RecordCheckoutCacheClone(ctx context.Context, mode, reason string) {
	if m == nil {
		return
	}

	if mode == "" {
		mode = CheckoutCacheCloneModeHardlink
	}

	if reason == "" {
		reason = CheckoutCacheCloneReasonOK
	}

	m.clones.Add(ctx, 1, metric.WithAttributes(
		attribute.String("mode", mode),
		attribute.String("reason", reason),
	))
}

func (m *CheckoutCacheMetrics) RecordCheckoutCacheStats(_ context.Context, stats CheckoutCacheStats) {
	if m == nil {
		return
	}

	stats.Repositories = max(stats.Repositories, 0)
	stats.Generations = max(stats.Generations, 0)
	stats.PackFiles = max(stats.PackFiles, 0)
	stats.PackBytes = max(stats.PackBytes, 0)
	stats.ActiveLeases = max(stats.ActiveLeases, 0)

	m.mu.Lock()
	m.stats = stats
	m.mu.Unlock()
}

func (m *CheckoutCacheMetrics) observeCheckoutCacheStats(_ context.Context, o metric.Observer) error {
	m.mu.RLock()
	stats := m.stats
	m.mu.RUnlock()

	o.ObserveInt64(m.repositories, stats.Repositories)
	o.ObserveInt64(m.generations, stats.Generations)
	o.ObserveInt64(m.packFiles, stats.PackFiles)
	o.ObserveInt64(m.packBytes, stats.PackBytes)
	o.ObserveInt64(m.activeLeases, stats.ActiveLeases)
	return nil
}
