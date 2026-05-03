package observability

import (
	"context"
	"testing"
)

func TestInitTracer_NoExporterCreatesPropagatingNonRecordingSpans(t *testing.T) {
	t.Setenv("OTEL_TRACES_EXPORTER", "")

	shutdown, err := InitTracer(context.Background(), "vectis-test")
	if err != nil {
		t.Fatalf("init tracer: %v", err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	_, span := Tracer("vectis-test").Start(context.Background(), "test.span")
	defer span.End()

	sc := span.SpanContext()
	if !sc.IsValid() {
		t.Fatal("expected valid span context for propagation")
	}

	if span.IsRecording() {
		t.Fatal("expected no-exporter spans to skip recording work")
	}
}
