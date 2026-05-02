package trace

import (
	"context"
	"net"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/config"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/observability"
	"vectis/internal/queue"

	"google.golang.org/grpc"
)

func TestEnvelopeTrace_QueueRoundTrip_ToWorkerContext(t *testing.T) {
	t.Setenv("VECTIS_GRPC_TLS_INSECURE", "true")

	shutdownTracer, err := observability.InitTracer(context.Background(), "vectis-test-envelope-trace")
	if err != nil {
		t.Fatalf("init tracer: %v", err)
	}
	t.Cleanup(func() { _ = shutdownTracer(context.Background()) })

	srvOpts, err := config.GRPCServerOptions()
	if err != nil {
		t.Fatalf("server opts: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()

	grpcSrv := grpc.NewServer(srvOpts...)
	defer grpcSrv.Stop()

	queueSvc := queue.NewQueueService(mocks.NewMockLogger())
	api.RegisterQueueServiceServer(grpcSrv, queueSvc)
	go func() { _ = grpcSrv.Serve(lis) }()

	dialOpts, err := config.GRPCClientDialOptions(lis.Addr().String())
	if err != nil {
		t.Fatalf("dial opts: %v", err)
	}

	conn, err := grpc.NewClient(lis.Addr().String(), dialOpts...)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	client := api.NewQueueServiceClient(conn)

	parentCtx, parentSpan := observability.Tracer("vectis-test-envelope-trace").Start(context.Background(), "api.trigger")
	parentSC := parentSpan.SpanContext()
	if !parentSC.IsValid() {
		t.Fatal("expected valid parent span context")
	}

	jobID := "job-trace"
	runID := "run-trace"
	action := "builtins/shell"
	req := &api.JobRequest{
		Job: &api.Job{
			Id:    &jobID,
			RunId: &runID,
			Root:  &api.Node{Uses: &action, With: map[string]string{"command": "echo hi"}},
		},
	}
	observability.InjectJobTraceContext(parentCtx, req)

	if _, err := client.Enqueue(parentCtx, req); err != nil {
		parentSpan.End()
		t.Fatalf("enqueue: %v", err)
	}
	parentSpan.End()

	deqCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	deqReq, err := client.Dequeue(deqCtx, &api.Empty{})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}

	if deqReq.GetJob() == nil {
		t.Fatal("dequeue returned nil job")
	}

	workerCtx := observability.ExtractJobTraceContext(context.Background(), deqReq)
	_, workerSpan := observability.Tracer("vectis-test-envelope-trace").Start(workerCtx, "worker.handle_job")
	workerSC := workerSpan.SpanContext()
	workerSpan.End()

	if !workerSC.IsValid() {
		t.Fatal("expected valid worker span context")
	}

	if workerSC.TraceID() != parentSC.TraceID() {
		t.Fatalf("trace id mismatch: got %s want %s", workerSC.TraceID(), parentSC.TraceID())
	}
}
