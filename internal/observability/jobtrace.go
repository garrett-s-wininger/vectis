package observability

import (
	"context"

	api "vectis/api/gen/go"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

const (
	jobTraceParentKey             = "traceparent"
	jobTraceStateKey              = "tracestate"
	JobEnqueuedAtUnixNanoKey      = "vectis_enqueue_unix_nano"
	JobEnqueueAcceptedUnixNanoKey = "vectis_enqueue_accepted_unix_nano"
)

func InjectJobTraceContext(ctx context.Context, req *api.JobRequest) {
	if req == nil {
		return
	}

	if req.Metadata == nil {
		req.Metadata = map[string]string{}
	}

	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	if tp := carrier.Get(jobTraceParentKey); tp != "" {
		req.Metadata[jobTraceParentKey] = tp
	}

	if ts := carrier.Get(jobTraceStateKey); ts != "" {
		req.Metadata[jobTraceStateKey] = ts
	}
}

func ExtractJobTraceContext(base context.Context, req *api.JobRequest) context.Context {
	if base == nil {
		base = context.Background()
	}

	if req == nil {
		return base
	}

	carrier := propagation.MapCarrier{}
	if tp := req.GetMetadata()[jobTraceParentKey]; tp != "" {
		carrier.Set(jobTraceParentKey, tp)
	}

	if ts := req.GetMetadata()[jobTraceStateKey]; ts != "" {
		carrier.Set(jobTraceStateKey, ts)
	}

	if len(carrier) == 0 {
		return base
	}

	return otel.GetTextMapPropagator().Extract(base, carrier)
}
