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
	"vectis/internal/testutil/grpcservices"
	"vectis/internal/testutil/grpctest"

	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type noopTestLogger struct{}

func (noopTestLogger) Debug(string, ...any)                           {}
func (noopTestLogger) Info(string, ...any)                            {}
func (noopTestLogger) Warn(string, ...any)                            {}
func (noopTestLogger) Error(string, ...any)                           {}
func (noopTestLogger) Fatal(string, ...any)                           {}
func (noopTestLogger) SetLevel(interfaces.Level)                      {}
func (noopTestLogger) WithOutput(io.Writer) interfaces.Logger         { return noopTestLogger{} }
func (noopTestLogger) WithField(string, string) interfaces.Logger     { return noopTestLogger{} }
func (noopTestLogger) WithFields(map[string]string) interfaces.Logger { return noopTestLogger{} }

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

	innerReg, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	regLis := &countingListener{Listener: innerReg}

	queueServer, _, _ := grpcservices.StartQueueServer(t, logger)
	logServer := startLogServer(t)
	regServer := grpctest.StartServerOnListener(t, regLis, func(srv *grpc.Server) {
		grpcservices.RegisterRegistryService(srv, logger)
	})

	regConn, err := grpc.NewClient(regLis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}

	regAPI := api.NewRegistryServiceClient(regConn)
	qAddr := queueServer.Addr()
	lAddr := logServer.Addr()
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
	viper.Set("discovery.registry.address", regServer.Addr())

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

	queueServer, _, _ := grpcservices.StartQueueServer(t, logger)
	logServer := startLogServer(t)

	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("worker.queue.address", queueServer.Addr())
	viper.Set("worker.log.address", logServer.Addr())

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

func startLogServer(t *testing.T) *grpctest.Server {
	t.Helper()

	return grpctest.StartServer(t, func(srv *grpc.Server) {
		hs := health.NewServer()
		healthgrpc.RegisterHealthServer(srv, hs)
		hs.SetServingStatus("log", healthpb.HealthCheckResponse_SERVING)
		api.RegisterLogServiceServer(srv, &api.UnimplementedLogServiceServer{})
	})
}
