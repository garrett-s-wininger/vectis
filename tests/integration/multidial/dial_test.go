//go:build integration

package multidial_test

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"
	"vectis/internal/multidial"
	"vectis/internal/queue"
	"vectis/internal/registry"

	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type noopTestLogger struct{}

func (noopTestLogger) Debug(string, ...any)                   {}
func (noopTestLogger) Info(string, ...any)                    {}
func (noopTestLogger) Warn(string, ...any)                    {}
func (noopTestLogger) Error(string, ...any)                   {}
func (noopTestLogger) Fatal(string, ...any)                   {}
func (noopTestLogger) SetLevel(interfaces.Level)              {}
func (noopTestLogger) WithOutput(io.Writer) interfaces.Logger { return noopTestLogger{} }

type countingListener struct {
	net.Listener
	accepts atomic.Int32
}

func (c *countingListener) Accept() (net.Conn, error) {
	conn, err := c.Listener.Accept()
	if err == nil {
		c.accepts.Add(1)
	}

	return conn, err
}

func TestDialQueueAndLog_SingleRegistryConnection(t *testing.T) {
	ctx := context.Background()
	logger := noopTestLogger{}

	qlis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer qlis.Close()

	llis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer llis.Close()

	innerReg, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	regLis := &countingListener{Listener: innerReg}
	defer regLis.Close()

	qs := grpc.NewServer()
	queue.RegisterQueueService(qs, logger, queue.QueueOptions{})
	go func() { _ = qs.Serve(qlis) }()
	defer qs.Stop()

	ls := grpc.NewServer()
	hs := health.NewServer()
	healthgrpc.RegisterHealthServer(ls, hs)
	hs.SetServingStatus("log", healthpb.HealthCheckResponse_SERVING)
	api.RegisterLogServiceServer(ls, &api.UnimplementedLogServiceServer{})
	go func() { _ = ls.Serve(llis) }()
	defer ls.Stop()

	regSvc := registry.NewRegistryService(logger)
	rs := grpc.NewServer()
	api.RegisterRegistryServiceServer(rs, regSvc)
	go func() { _ = rs.Serve(regLis) }()
	defer rs.Stop()

	time.Sleep(50 * time.Millisecond)

	regConn, err := grpc.NewClient(regLis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}

	regAPI := api.NewRegistryServiceClient(regConn)
	qAddr := qlis.Addr().String()
	lAddr := llis.Addr().String()
	compQ := api.Component_COMPONENT_QUEUE
	compL := api.Component_COMPONENT_LOG
	if _, err := regAPI.Register(ctx, &api.Registration{Component: &compQ, Address: &qAddr}); err != nil {
		t.Fatal(err)
	}

	if _, err := regAPI.Register(ctx, &api.Registration{Component: &compL, Address: &lAddr}); err != nil {
		t.Fatal(err)
	}
	regConn.Close()
	regLis.accepts.Store(0)

	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("discovery.registry.address", regLis.Addr().String())

	_, _, cleanup, err := multidial.DialQueueAndLog(ctx, logger)
	if err != nil {
		t.Fatalf("DialQueueAndLog: %v", err)
	}
	cleanup()

	n := regLis.accepts.Load()
	if n != 1 {
		t.Fatalf("expected exactly 1 TCP connection to registry listener, got %d (two clients would use two connections)", n)
	}
}

func TestDialQueueAndLog_BothPinnedSkipsRegistry(t *testing.T) {
	ctx := context.Background()
	logger := noopTestLogger{}

	qlis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer qlis.Close()

	llis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer llis.Close()

	qs := grpc.NewServer()
	queue.RegisterQueueService(qs, logger, queue.QueueOptions{})
	go func() { _ = qs.Serve(qlis) }()
	defer qs.Stop()

	ls := grpc.NewServer()
	hs := health.NewServer()
	healthgrpc.RegisterHealthServer(ls, hs)
	hs.SetServingStatus("log", healthpb.HealthCheckResponse_SERVING)
	api.RegisterLogServiceServer(ls, &api.UnimplementedLogServiceServer{})
	go func() { _ = ls.Serve(llis) }()
	defer ls.Stop()

	time.Sleep(50 * time.Millisecond)

	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("worker.queue.address", qlis.Addr().String())
	viper.Set("worker.log.address", llis.Addr().String())

	start := time.Now()
	_, _, cleanup, err := multidial.DialQueueAndLog(ctx, logger)
	if err != nil {
		t.Fatalf("DialQueueAndLog: %v", err)
	}
	cleanup()

	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("pinned dial took %v; likely tried registry retries", d)
	}
}
