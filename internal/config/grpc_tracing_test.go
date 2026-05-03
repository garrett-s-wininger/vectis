package config

import (
	"context"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/observability"
	"vectis/internal/testutil/grpctest"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
)

type traceCaptureQueueServer struct {
	api.UnimplementedQueueServiceServer
	got chan trace.SpanContext
}

func (s *traceCaptureQueueServer) Enqueue(ctx context.Context, _ *api.JobRequest) (*api.Empty, error) {
	s.got <- trace.SpanContextFromContext(ctx)
	return &api.Empty{}, nil
}

func TestGRPCTracingPropagation_Insecure(t *testing.T) {
	t.Setenv("VECTIS_GRPC_TLS_INSECURE", "true")

	shutdownTracer, err := observability.InitTracer(context.Background(), "vectis-test-grpc-tracing")
	if err != nil {
		t.Fatalf("init tracer: %v", err)
	}
	t.Cleanup(func() {
		_ = shutdownTracer(context.Background())
	})

	srvOpts, err := GRPCServerOptions()
	if err != nil {
		t.Fatalf("server opts: %v", err)
	}

	capture := &traceCaptureQueueServer{got: make(chan trace.SpanContext, 1)}
	dialOpts, err := GRPCClientDialOptions("")
	if err != nil {
		t.Fatalf("client dial opts: %v", err)
	}

	server := grpctest.StartServerWithOptions(t, grpctest.Options{
		ServerOptions: srvOpts,
		DialOptions:   dialOpts,
	}, func(srv *grpc.Server) {
		api.RegisterQueueServiceServer(srv, capture)
	})

	client := api.NewQueueServiceClient(server.Conn)

	parentCtx, span := observability.Tracer("vectis-test-grpc-tracing").Start(context.Background(), "parent")
	parentSC := span.SpanContext()
	if !parentSC.IsValid() {
		t.Fatal("expected valid parent span context")
	}

	_, err = client.Enqueue(parentCtx, &api.JobRequest{Job: &api.Job{}})
	span.End()
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	select {
	case serverSC := <-capture.got:
		if !serverSC.IsValid() {
			t.Fatal("expected valid server span context")
		}

		if serverSC.TraceID() != parentSC.TraceID() {
			t.Fatalf("trace id mismatch: got %s, want %s", serverSC.TraceID(), parentSC.TraceID())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for server span context")
	}
}
