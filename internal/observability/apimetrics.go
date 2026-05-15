package observability

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"runtime/debug"
	"time"

	promclient "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
)

const (
	defaultDeploymentEnv = "development"
)

func InitServiceMetrics(ctx context.Context, serviceName string) (metrics http.Handler, shutdown func(context.Context) error, err error) {
	reg := promclient.NewRegistry()

	exp, err := prometheus.New(prometheus.WithRegisterer(reg))
	if err != nil {
		return nil, nil, fmt.Errorf("prometheus exporter: %w", err)
	}

	res, err := serviceResource(ctx, serviceName)
	if err != nil {
		return nil, nil, fmt.Errorf("resource: %w", err)
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(exp),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)

	h := promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})

	shutdown = mp.Shutdown
	return h, shutdown, nil
}

func InitAPIMetrics(ctx context.Context) (metrics http.Handler, shutdown func(context.Context) error, err error) {
	return InitServiceMetrics(ctx, "vectis-api")
}

func serviceResource(ctx context.Context, serviceName string) (*resource.Resource, error) {
	ver := "unknown"
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" && info.Main.Version != "(devel)" {
		ver = info.Main.Version
	}

	env := os.Getenv("VECTIS_DEPLOYMENT_ENV")
	if env == "" {
		env = defaultDeploymentEnv
	}

	return resource.New(ctx,
		resource.WithHost(),
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(ver),
			attribute.String("deployment.environment", env),
		),
	)
}

func RegisterSQLDBPoolMetrics(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("RegisterSQLDBPoolMetrics: db is nil")
	}

	m := otel.Meter("vectis/database/sql")

	openG, err := m.Int64ObservableGauge("db_client_connections_open",
		metric.WithDescription("Open connections in the database/sql pool"),
		metric.WithUnit("{connection}"))

	if err != nil {
		return fmt.Errorf("db_client_connections_open: %w", err)
	}

	inUseG, err := m.Int64ObservableGauge("db_client_connections_in_use",
		metric.WithDescription("Connections currently in use (executing a query)"),
		metric.WithUnit("{connection}"))
	if err != nil {
		return fmt.Errorf("db_client_connections_in_use: %w", err)
	}

	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		st := db.Stats()
		o.ObserveInt64(openG, int64(st.OpenConnections))
		o.ObserveInt64(inUseG, int64(st.InUse))
		return nil
	}, openG, inUseG)

	if err != nil {
		return fmt.Errorf("register db pool callback: %w", err)
	}

	return nil
}

func RegisterRetentionStorageMetrics(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("RegisterRetentionStorageMetrics: db is nil")
	}

	m := otel.Meter("vectis/storage")
	recordsG, err := m.Int64ObservableGauge("vectis_storage_records",
		metric.WithDescription("Durable SQL records by retention surface"),
		metric.WithUnit("{record}"))

	if err != nil {
		return fmt.Errorf("vectis_storage_records: %w", err)
	}

	oldestAgeG, err := m.Int64ObservableGauge("vectis_storage_oldest_record_age_seconds",
		metric.WithDescription("Age of the oldest durable SQL record by retention surface"),
		metric.WithUnit("s"))

	if err != nil {
		return fmt.Errorf("vectis_storage_oldest_record_age_seconds: %w", err)
	}

	countQueries := []struct {
		surface string
		query   string
	}{
		{"active_runs", `SELECT COUNT(*) FROM job_runs WHERE status NOT IN ('succeeded', 'failed')`},
		{"terminal_runs", `SELECT COUNT(*) FROM job_runs WHERE status IN ('succeeded', 'failed', 'aborted', 'cancelled', 'abandoned')`},
		{"run_dispatch_events", `SELECT COUNT(*) FROM run_dispatch_events`},
		{"job_definitions", `SELECT COUNT(*) FROM job_definitions`},
		{"idempotency_keys", `SELECT COUNT(*) FROM idempotency_keys`},
		{"audit_log", `SELECT COUNT(*) FROM audit_log`},
	}

	oldestQueries := []struct {
		surface string
		query   string
	}{
		{"terminal_runs", `SELECT CAST(MIN(finished_at) AS TEXT) FROM job_runs WHERE status IN ('succeeded', 'failed', 'aborted', 'cancelled', 'abandoned') AND finished_at IS NOT NULL`},
		{"job_definitions", `SELECT CAST(MIN(created_at) AS TEXT) FROM job_definitions`},
		{"idempotency_keys", `SELECT CAST(MIN(updated_at) AS TEXT) FROM idempotency_keys`},
		{"audit_log", `SELECT CAST(MIN(created_at) AS TEXT) FROM audit_log`},
	}

	_, err = m.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		for _, q := range countQueries {
			n, err := queryInt64(ctx, db, q.query)
			if err != nil {
				return err
			}
			o.ObserveInt64(recordsG, n, metric.WithAttributes(attribute.String("surface", q.surface)))
		}

		now := time.Now().UTC()
		for _, q := range oldestQueries {
			oldest, ok, err := queryTime(ctx, db, q.query)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}

			age := max(int64(now.Sub(oldest).Seconds()), 0)
			o.ObserveInt64(oldestAgeG, age, metric.WithAttributes(attribute.String("surface", q.surface)))
		}

		return nil
	}, recordsG, oldestAgeG)

	if err != nil {
		return fmt.Errorf("register retention storage callback: %w", err)
	}

	return nil
}

func queryInt64(ctx context.Context, db *sql.DB, query string) (int64, error) {
	var n int64
	if err := db.QueryRowContext(ctx, query).Scan(&n); err != nil {
		return 0, err
	}

	return n, nil
}

func queryTime(ctx context.Context, db *sql.DB, query string) (time.Time, bool, error) {
	var raw sql.NullString
	if err := db.QueryRowContext(ctx, query).Scan(&raw); err != nil {
		return time.Time{}, false, err
	}

	if !raw.Valid || raw.String == "" {
		return time.Time{}, false, nil
	}

	t, err := parseDBTime(raw.String)
	if err != nil {
		return time.Time{}, false, err
	}

	return t, true, nil
}

func parseDBTime(raw string) (time.Time, error) {
	layouts := []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999Z07:00",
		"2006-01-02 15:04:05Z07:00",
		"2006-01-02 15:04:05.999999-07",
		"2006-01-02 15:04:05-07",
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05",
	}

	for _, layout := range layouts {
		t, err := time.Parse(layout, raw)
		if err == nil {
			return t.UTC(), nil
		}
	}

	return time.Time{}, fmt.Errorf("parse database timestamp %q", raw)
}
