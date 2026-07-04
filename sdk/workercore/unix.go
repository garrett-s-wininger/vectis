package workercore

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

	api "vectis/api/gen/go"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

func UnixEndpoint(socketPath string) string {
	socketPath = strings.TrimSpace(socketPath)
	if socketPath == "" {
		return ""
	}

	if strings.HasPrefix(socketPath, "unix://") {
		return socketPath
	}

	return "unix://" + socketPath
}

func SocketPathFromEndpoint(endpoint string) (string, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return "", fmt.Errorf("unix endpoint is required")
	}

	if strings.HasPrefix(endpoint, "unix://") {
		path := strings.TrimPrefix(endpoint, "unix://")
		if strings.TrimSpace(path) == "" {
			return "", fmt.Errorf("unix endpoint %q has no path", endpoint)
		}

		return path, nil
	}

	return endpoint, nil
}

func DialUnixCore(ctx context.Context, endpoint string) (*grpc.ClientConn, api.WorkerCoreServiceClient, error) {
	conn, err := dialUnixGRPC(ctx, endpoint, "worker-core")
	if err != nil {
		return nil, nil, fmt.Errorf("dial worker core socket: %w", err)
	}

	return conn, api.NewWorkerCoreServiceClient(conn), nil
}

func DialUnixShell(ctx context.Context, endpoint string) (*grpc.ClientConn, error) {
	conn, err := dialUnixGRPC(ctx, endpoint, "worker-core-shell")
	if err != nil {
		return nil, fmt.Errorf("dial worker core shell socket: %w", err)
	}

	return conn, nil
}

func NewUnixCoreServer(socketPath string, core Core, opts ServiceOptions, grpcOpts ...grpc.ServerOption) (*grpc.Server, net.Listener, error) {
	return newUnixServer(socketPath, "worker core", func(server *grpc.Server) {
		api.RegisterWorkerCoreServiceServer(server, NewService(core, opts))
	}, grpcOpts...)
}

func NewUnixShellServer(socketPath string, shell api.WorkerCoreShellServiceServer, grpcOpts ...grpc.ServerOption) (*grpc.Server, net.Listener, error) {
	if shell == nil {
		return nil, nil, fmt.Errorf("worker core shell service is required")
	}

	return newUnixServer(socketPath, "worker core shell", func(server *grpc.Server) {
		api.RegisterWorkerCoreShellServiceServer(server, shell)
	}, grpcOpts...)
}

func newUnixServer(socketPath, label string, register func(*grpc.Server), grpcOpts ...grpc.ServerOption) (*grpc.Server, net.Listener, error) {
	socketPath = strings.TrimSpace(socketPath)
	if socketPath == "" {
		return nil, nil, fmt.Errorf("%s socket path is required", label)
	}

	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		return nil, nil, fmt.Errorf("remove stale %s socket: %w", label, err)
	}

	if err := os.MkdirAll(filepath.Dir(socketPath), 0o755); err != nil {
		return nil, nil, fmt.Errorf("create %s socket directory: %w", label, err)
	}

	var listenConfig net.ListenConfig
	ln, err := listenConfig.Listen(context.Background(), "unix", socketPath)
	if err != nil {
		return nil, nil, fmt.Errorf("listen %s socket: %w", label, err)
	}

	if err := os.Chmod(socketPath, 0o600); err != nil {
		_ = ln.Close()
		_ = os.Remove(socketPath)
		return nil, nil, fmt.Errorf("chmod %s socket: %w", label, err)
	}

	server := grpc.NewServer(grpcOpts...)
	register(server)

	return server, ln, nil
}

func dialUnixGRPC(ctx context.Context, endpoint, targetName string) (*grpc.ClientConn, error) {
	socketPath, err := SocketPathFromEndpoint(endpoint)
	if err != nil {
		return nil, err
	}

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", socketPath)
	}

	conn, err := grpc.NewClient(
		"passthrough:///"+targetName,
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	if err != nil {
		return nil, err
	}

	if err := waitForClientConnReady(ctx, conn); err != nil {
		_ = conn.Close()
		return nil, err
	}

	return conn, nil
}

func waitForClientConnReady(ctx context.Context, conn *grpc.ClientConn) error {
	conn.Connect()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return nil
		}

		if !conn.WaitForStateChange(ctx, state) {
			if err := ctx.Err(); err != nil {
				return err
			}

			return fmt.Errorf("connection state remained %s", state)
		}
	}
}
