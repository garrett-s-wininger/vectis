package observability

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	WorkerOutcomeSuccess          = "success"
	WorkerOutcomeFailed           = "failed"
	WorkerOutcomeAborted          = "aborted"
	WorkerOutcomeSkippedUnclaimed = "skipped_unclaimed"
	WorkerOutcomeMalformed        = "malformed"

	WorkerPhaseIdle       = "idle"
	WorkerPhaseDequeuing  = "dequeuing"
	WorkerPhaseClaiming   = "claiming"
	WorkerPhaseAcking     = "acking"
	WorkerPhaseExecuting  = "executing"
	WorkerPhaseFinalizing = "finalizing"

	WorkerSPIRESVIDOutcomeSuccess = "success"
	WorkerSPIRESVIDOutcomeFailed  = "failed"

	WorkerSPIRESVIDReasonMatched           = "matched"
	WorkerSPIRESVIDReasonMissingIdentity   = "missing_identity"
	WorkerSPIRESVIDReasonMissingSource     = "missing_source"
	WorkerSPIRESVIDReasonInvalidExpectedID = "invalid_expected_id"
	WorkerSPIRESVIDReasonMismatch          = "mismatch"
	WorkerSPIRESVIDReasonSourceError       = "source_error"
	WorkerSPIRESVIDReasonUnknown           = "unknown"
)

var workerPhases = []string{
	WorkerPhaseIdle,
	WorkerPhaseDequeuing,
	WorkerPhaseClaiming,
	WorkerPhaseAcking,
	WorkerPhaseExecuting,
	WorkerPhaseFinalizing,
}

type WorkerMetrics struct {
	received        metric.Int64Counter
	duration        metric.Float64Histogram
	spireSVIDChecks metric.Int64Counter
	phaseGauge      metric.Int64ObservableGauge
	drainingGauge   metric.Int64ObservableGauge
	dbGauge         metric.Int64ObservableGauge
	callback        metric.Registration
	mu              sync.RWMutex
	phase           string
	draining        bool
	dbUnavailable   bool
}

func NewWorkerMetrics() (*WorkerMetrics, error) {
	m := otel.Meter("vectis/worker")

	received, err := m.Int64Counter("vectis_worker_jobs_received_total",
		metric.WithDescription("Jobs dequeued and passed to the worker handler"),
		metric.WithUnit("{job}"))

	if err != nil {
		return nil, fmt.Errorf("vectis_worker_jobs_received_total: %w", err)
	}

	duration, err := m.Float64Histogram("vectis_worker_job_duration_seconds",
		metric.WithDescription("Wall time from job handler entry to terminal outcome"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300))

	if err != nil {
		return nil, fmt.Errorf("vectis_worker_job_duration_seconds: %w", err)
	}

	spireSVIDChecks, err := m.Int64Counter("vectis_worker_spire_svid_checks_total",
		metric.WithDescription("Worker SPIRE execution X.509-SVID checks by outcome and reason"),
		metric.WithUnit("{check}"))

	if err != nil {
		return nil, fmt.Errorf("vectis_worker_spire_svid_checks_total: %w", err)
	}

	phaseGauge, err := m.Int64ObservableGauge("vectis_worker_lifecycle_state",
		metric.WithDescription("Current worker lifecycle phase. One labelled state is 1 and the remaining states are 0."))

	if err != nil {
		return nil, fmt.Errorf("vectis_worker_lifecycle_state: %w", err)
	}

	drainingGauge, err := m.Int64ObservableGauge("vectis_worker_draining",
		metric.WithDescription("Whether the worker has received shutdown and stopped accepting new dequeue work."))

	if err != nil {
		return nil, fmt.Errorf("vectis_worker_draining: %w", err)
	}

	dbGauge, err := m.Int64ObservableGauge("vectis_worker_db_unavailable",
		metric.WithDescription("Whether the worker has observed database unavailability on DB-backed transitions."))

	if err != nil {
		return nil, fmt.Errorf("vectis_worker_db_unavailable: %w", err)
	}

	wm := &WorkerMetrics{
		received:        received,
		duration:        duration,
		spireSVIDChecks: spireSVIDChecks,
		phaseGauge:      phaseGauge,
		drainingGauge:   drainingGauge,
		dbGauge:         dbGauge,
		phase:           WorkerPhaseIdle,
	}

	callback, err := m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		phase, draining, dbUnavailable := wm.snapshot()
		for _, known := range workerPhases {
			value := int64(0)
			if known == phase {
				value = 1
			}

			o.ObserveInt64(phaseGauge, value, metric.WithAttributes(attribute.String("state", known)))
		}

		o.ObserveInt64(drainingGauge, boolInt64(draining))
		o.ObserveInt64(dbGauge, boolInt64(dbUnavailable))
		return nil
	}, phaseGauge, drainingGauge, dbGauge)

	if err != nil {
		return nil, fmt.Errorf("worker lifecycle callback: %w", err)
	}

	wm.callback = callback
	return wm, nil
}

func (wm *WorkerMetrics) RecordJobReceived(ctx context.Context) {
	if wm == nil {
		return
	}

	wm.received.Add(ctx, 1)
}

func (wm *WorkerMetrics) RecordJobFinished(ctx context.Context, outcome string, d time.Duration) {
	if wm == nil || outcome == "" {
		return
	}

	attrs := metric.WithAttributes(attribute.String("outcome", outcome))
	wm.duration.Record(ctx, d.Seconds(), attrs)
}

func (wm *WorkerMetrics) RecordSPIRESVIDCheck(ctx context.Context, outcome, reason string) {
	if wm == nil {
		return
	}

	if outcome == "" {
		outcome = WorkerSPIRESVIDOutcomeFailed
	}

	if reason == "" {
		reason = WorkerSPIRESVIDReasonUnknown
	}

	wm.spireSVIDChecks.Add(ctx, 1, metric.WithAttributes(
		attribute.String("outcome", outcome),
		attribute.String("reason", reason),
	))
}

func (wm *WorkerMetrics) SetLifecyclePhase(phase string) {
	if wm == nil || phase == "" {
		return
	}

	wm.mu.Lock()
	wm.phase = phase
	wm.mu.Unlock()
}

func (wm *WorkerMetrics) LifecyclePhase() string {
	if wm == nil {
		return ""
	}

	wm.mu.RLock()
	defer wm.mu.RUnlock()
	return wm.phase
}

func (wm *WorkerMetrics) SetDraining(draining bool) {
	if wm == nil {
		return
	}

	wm.mu.Lock()
	wm.draining = draining
	wm.mu.Unlock()
}

func (wm *WorkerMetrics) Draining() bool {
	if wm == nil {
		return false
	}

	wm.mu.RLock()
	defer wm.mu.RUnlock()
	return wm.draining
}

func (wm *WorkerMetrics) SetDBUnavailable(unavailable bool) {
	if wm == nil {
		return
	}

	wm.mu.Lock()
	wm.dbUnavailable = unavailable
	wm.mu.Unlock()
}

func (wm *WorkerMetrics) DBUnavailable() bool {
	if wm == nil {
		return false
	}

	wm.mu.RLock()
	defer wm.mu.RUnlock()
	return wm.dbUnavailable
}

func (wm *WorkerMetrics) snapshot() (string, bool, bool) {
	wm.mu.RLock()
	defer wm.mu.RUnlock()
	return wm.phase, wm.draining, wm.dbUnavailable
}

func boolInt64(v bool) int64 {
	if v {
		return 1
	}

	return 0
}
