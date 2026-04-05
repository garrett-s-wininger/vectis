package observability

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"runtime/debug"

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
