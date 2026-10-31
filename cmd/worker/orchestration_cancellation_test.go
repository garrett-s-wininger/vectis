package main

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"
	"vectis/internal/orchestrator"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type stalledChoreographyStream struct {
	grpc.ServerStream
	first   *atomic.Bool
	entered chan struct{}
}

func (s *stalledChoreographyStream) SendMsg(message any) error {
	if s.first.CompareAndSwap(false, true) {
		close(s.entered)
		<-s.Context().Done()
		return s.Context().Err()
	}

	return s.ServerStream.SendMsg(message)
}

func TestWorkerFault_StalledStreamCancellationReleasesGate(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	service := orchestrator.New(1)
	t.Cleanup(service.Close)

	listener := bufconn.Listen(1024 * 1024)
	entered := make(chan struct{})
	var first atomic.Bool
	server := grpc.NewServer(grpc.StreamInterceptor(func(srv any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return handler(srv, &stalledChoreographyStream{ServerStream: stream, first: &first, entered: entered})
	}))

	orchestrator.RegisterOrchestratorService(server, service, interfaces.NewLogger("stalled-stream-test"))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })

	conn, err := grpc.NewClient("passthrough:///stalled", grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return listener.DialContext(ctx)
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = conn.Close() })
	ch := newGRPCExecutionChoreographer(api.NewOrchestratorServiceClient(conn))
	callCtx, cancelCall := context.WithCancel(ctx)
	defer cancelCall()

	done := make(chan error, 1)
	go func() { _, err := ch.loadRun(callCtx, &api.LoadRunRequest{RunId: stringPtr("stalled")}); done <- err }()

	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("server never reached response boundary")
	}

	waitCtx, cancelWait := context.WithCancel(ctx)
	cancelWait()
	waitDone := make(chan error, 1)

	go func() {
		_, err := ch.loadRun(waitCtx, &api.LoadRunRequest{RunId: stringPtr("waiting")})
		waitDone <- err
	}()

	select {
	case err := <-waitDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiting cancellation: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("cancelled caller blocked behind stalled stream")
	}

	cancelCall()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("exchange cancellation: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("stream exchange ignored cancellation")
	}

	if _, err := ch.loadRun(ctx, &api.LoadRunRequest{RunId: stringPtr("recovered")}); err != nil {
		t.Fatalf("stream did not reconnect: %v", err)
	}
}
