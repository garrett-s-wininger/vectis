package cli_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"vectis/internal/cli"
	"vectis/internal/interfaces/mocks"
)

func TestDeferShutdownWithTimeout_CallsShutdown(t *testing.T) {
	called := false
	fn := cli.DeferShutdownWithTimeout(nil, "test", func(ctx context.Context) error {
		called = true
		return nil
	}, time.Second)
	fn()
	if !called {
		t.Error("expected shutdown to be called")
	}
}

func TestDeferShutdownWithTimeout_LogsWarnOnError(t *testing.T) {
	logger := mocks.NewMockLogger()
	expectedErr := errors.New("shutdown failure")
	fn := cli.DeferShutdownWithTimeout(logger, "test", func(ctx context.Context) error {
		return expectedErr
	}, time.Second)
	fn()

	warns := logger.GetWarnCalls()
	if len(warns) == 0 {
		t.Fatal("expected warn log on shutdown error")
	}

	if !strings.Contains(warns[0], "shutdown failure") {
		t.Errorf("expected warn message to contain error, got: %s", warns[0])
	}
}

func TestDeferShutdown(t *testing.T) {
	called := false
	fn := cli.DeferShutdown(nil, "test", func(ctx context.Context) error {
		called = true
		return nil
	})

	fn()

	if !called {
		t.Error("expected shutdown to be called")
	}
}

func TestServeHTTP_ContextCancellation(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = cli.ServeHTTP(ctx, srv, func() error {
		return srv.Serve(ln)
	}, time.Second, "test", nil)

	if err != nil {
		t.Errorf("expected nil after context cancellation, got %v", err)
	}
}

func TestServeHTTP_PropagatesNonServerError(t *testing.T) {
	expectedErr := errors.New("unexpected server error")
	err := cli.ServeHTTP(context.Background(), &http.Server{}, func() error {
		return expectedErr
	}, time.Second, "test", nil)

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected %v, got %v", expectedErr, err)
	}
}

func TestServeHTTP_ErrServerClosedReturnsNil(t *testing.T) {
	err := cli.ServeHTTP(context.Background(), &http.Server{}, func() error {
		return http.ErrServerClosed
	}, time.Second, "test", nil)

	if err != nil {
		t.Errorf("expected nil for ErrServerClosed, got %v", err)
	}
}

func TestServeHTTP_LoggerNotRequiredForShutdown(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = cli.ServeHTTP(ctx, srv, func() error {
		return srv.Serve(ln)
	}, time.Second, "test", nil)

	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestServeGRPC_ContextCancellation(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	srv := grpc.NewServer()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = cli.ServeGRPC(ctx, srv, ln, "test", nil)
	if err != nil {
		t.Errorf("expected nil after context cancellation, got %v", err)
	}
}

func TestServeGRPC_ErrServerStoppedReturnsNil(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	srv := grpc.NewServer()
	go srv.Serve(ln)
	srv.GracefulStop()

	err = cli.ServeGRPC(context.Background(), srv, ln, "test", nil)
	if err != nil {
		t.Errorf("expected nil for ErrServerStopped, got %v", err)
	}
}

func TestServeGRPC_PropagatesNonServerError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ln.Close()

	srv := grpc.NewServer()
	err = cli.ServeGRPC(context.Background(), srv, ln, "test", nil)
	if err == nil {
		t.Error("expected error from closed listener, got nil")
	}
}

func TestConfigureVersion_SetsVersion(t *testing.T) {
	cmd := &cobra.Command{}
	cli.ConfigureVersion(cmd)

	if cmd.Version == "" {
		t.Error("expected version to be set")
	}
}
