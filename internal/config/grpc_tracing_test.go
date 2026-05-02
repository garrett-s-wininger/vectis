package config

import (
	"context"
	"net"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/observability"

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

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = lis.Close() })

	srv := grpc.NewServer(srvOpts...)
	t.Cleanup(srv.Stop)

	capture := &traceCaptureQueueServer{got: make(chan trace.SpanContext, 1)}
	api.RegisterQueueServiceServer(srv, capture)
	go func() { _ = srv.Serve(lis) }()

	dialOpts, err := GRPCClientDialOptions(lis.Addr().String())
	if err != nil {
		t.Fatalf("client dial opts: %v", err)
	}

	conn, err := grpc.NewClient(lis.Addr().String(), dialOpts...)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	client := api.NewQueueServiceClient(conn)

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
