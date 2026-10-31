package observability

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

func RegisterOrchestratorRetentionMetrics(stats func() (int64, int64, int64, int64)) (func() error, error) {
	m := otel.Meter("vectis/orchestrator")
	var gauges []metric.Int64ObservableGauge

	for _, item := range []struct{ name, description string }{
		{"vectis_orchestrator_retained_runs", "Run graphs currently retained in orchestrator memory"},
		{"vectis_orchestrator_retained_tasks", "Tasks currently retained in orchestrator memory"},
		{"vectis_orchestrator_runs_awaiting_persistence", "Completed run graphs awaiting durable persistence acknowledgment"},
	} {
		gauge, err := m.Int64ObservableGauge(item.name, metric.WithDescription(item.description))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", item.name, err)
		}

		gauges = append(gauges, gauge)
	}

	reclaimed, err := m.Int64ObservableCounter("vectis_orchestrator_reclaimed_runs_total",
		metric.WithDescription("Run graphs reclaimed after durable state acknowledgment"))

	if err != nil {
		return nil, err
	}

	registration, err := m.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		runs, tasks, awaiting, released := stats()
		observer.ObserveInt64(gauges[0], runs)
		observer.ObserveInt64(gauges[1], tasks)
		observer.ObserveInt64(gauges[2], awaiting)
		observer.ObserveInt64(reclaimed, released)
		return nil
	}, gauges[0], gauges[1], gauges[2], reclaimed)

	if err != nil {
		return nil, err
	}

	return registration.Unregister, nil
}
