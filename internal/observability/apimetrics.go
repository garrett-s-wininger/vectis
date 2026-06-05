package observability

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"vectis/internal/database"
	"vectis/internal/version"

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
	env := os.Getenv("VECTIS_DEPLOYMENT_ENV")
	if env == "" {
		env = defaultDeploymentEnv
	}

	return resource.New(ctx,
		resource.WithHost(),
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(version.Version),
			attribute.String("service.commit", version.Commit),
			attribute.String("service.build_date", version.BuildDate),
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
		{"run_tasks", `SELECT COUNT(*) FROM run_tasks`},
		{"task_attempts", `SELECT COUNT(*) FROM task_attempts`},
		{"run_segments", `SELECT COUNT(*) FROM run_segments`},
		{"segment_executions", `SELECT COUNT(*) FROM segment_executions`},
		{"task_dispatch_intents", `SELECT COUNT(*) FROM task_dispatch_intents`},
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

func RegisterTaskDispatchBacklogMetrics(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("RegisterTaskDispatchBacklogMetrics: db is nil")
	}

	m := otel.Meter("vectis/task-dispatch")
	pendingG, err := m.Int64ObservableGauge("vectis_task_dispatch_pending_intents",
		metric.WithDescription("Pending task dispatch intents eligible for queue handoff by target cell"),
		metric.WithUnit("{intent}"))
	if err != nil {
		return fmt.Errorf("vectis_task_dispatch_pending_intents: %w", err)
	}

	_, err = m.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		counts, err := queryTaskDispatchPendingByCell(ctx, db, time.Now().UnixNano())
		if err != nil {
			return err
		}

		for _, count := range counts {
			o.ObserveInt64(pendingG, count.Count, metric.WithAttributes(attribute.String("cell_id", count.CellID)))
		}

		return nil
	}, pendingG)
	if err != nil {
		return fmt.Errorf("register task dispatch backlog callback: %w", err)
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

type metricCountByCell struct {
	CellID string
	Count  int64
}

func queryTaskDispatchPendingByCell(ctx context.Context, db *sql.DB, cutoffUnixNano int64) ([]metricCountByCell, error) {
	rows, err := db.QueryContext(ctx, rebindMetricQueryForPgx(`
		SELECT tdi.cell_id, COUNT(*)
		FROM task_dispatch_intents tdi
		JOIN job_runs jr ON jr.run_id = tdi.run_id
		JOIN segment_executions se ON se.execution_id = tdi.execution_id
		JOIN run_segments rs ON rs.segment_id = se.segment_id AND rs.run_id = tdi.run_id
		JOIN run_tasks rt ON rt.task_id = tdi.task_id AND rt.run_id = tdi.run_id
		JOIN task_attempts ta ON ta.attempt_id = tdi.task_attempt_id AND ta.task_id = rt.task_id AND ta.run_id = tdi.run_id
		WHERE tdi.enqueued_at IS NULL
			AND rs.status = 'pending'
			AND se.status = 'pending'
			AND rt.status = 'pending'
			AND ta.status = 'pending'
			AND jr.status = 'queued'
			AND (tdi.last_enqueue_attempt_at IS NULL OR tdi.last_enqueue_attempt_at <= ?)
		GROUP BY tdi.cell_id
		ORDER BY tdi.cell_id ASC
	`), cutoffUnixNano)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []metricCountByCell
	for rows.Next() {
		var count metricCountByCell
		if err := rows.Scan(&count.CellID, &count.Count); err != nil {
			return nil, err
		}

		out = append(out, count)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

func rebindMetricQueryForPgx(query string) string {
	if os.Getenv(database.EnvDatabaseDriver) != "pgx" {
		return query
	}

	var b strings.Builder
	b.Grow(len(query) + 8)
	argNum := 1
	for i := 0; i < len(query); i++ {
		if query[i] == '?' {
			b.WriteByte('$')
			b.WriteString(strconv.Itoa(argNum))
			argNum++
			continue
		}

		b.WriteByte(query[i])
	}

	return b.String()
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
