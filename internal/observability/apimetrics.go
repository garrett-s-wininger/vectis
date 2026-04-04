package observability

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"runtime/debug"

	promclient "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
)

const (
	defaultDeploymentEnv = "development"
)

func InitAPIMetrics(ctx context.Context) (metrics http.Handler, shutdown func(context.Context) error, err error) {
	reg := promclient.NewRegistry()

	exp, err := prometheus.New(prometheus.WithRegisterer(reg))
	if err != nil {
		return nil, nil, fmt.Errorf("prometheus exporter: %w", err)
	}

	res, err := apiResource(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("resource: %w", err)
	}

	mp := metric.NewMeterProvider(
		metric.WithReader(exp),
		metric.WithResource(res),
	)
	otel.SetMeterProvider(mp)

	h := promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})

	shutdown = mp.Shutdown
	return h, shutdown, nil
}

func apiResource(ctx context.Context) (*resource.Resource, error) {
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
			semconv.ServiceName("vectis-api"),
			semconv.ServiceVersion(ver),
			attribute.String("deployment.environment", env),
		),
	)
}
