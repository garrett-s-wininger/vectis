package workercore

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

	api "vectis/api/gen/go"
	"vectis/internal/utils"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultCoreSocketName  = "worker-core.sock"
	defaultShellSocketName = "worker-core-shell.sock"
)

func DefaultCoreSocketPath() string {
	return filepath.Join(utils.RuntimeDir(), defaultCoreSocketName)
}

func DefaultShellSocketPath() string {
	return filepath.Join(utils.RuntimeDir(), defaultShellSocketName)
}

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

func DialUnixCore(ctx context.Context, socketPath string) (*RemoteCore, func(), error) {
	socketPath, err := SocketPathFromEndpoint(socketPath)
	if err != nil {
		return nil, nil, fmt.Errorf("worker core socket path: %w", err)
	}

	if socketPath == "" {
		return nil, nil, fmt.Errorf("worker core socket path is required")
	}

	conn, err := dialUnixGRPC(ctx, socketPath, "worker-core")
	if err != nil {
		return nil, nil, fmt.Errorf("dial worker core socket: %w", err)
	}

	return NewRemoteCore(api.NewWorkerCoreServiceClient(conn)), func() { _ = conn.Close() }, nil
}

func dialUnixShell(ctx context.Context, endpoint string) (*grpc.ClientConn, error) {
	socketPath, err := SocketPathFromEndpoint(endpoint)
	if err != nil {
		return nil, fmt.Errorf("worker core shell endpoint: %w", err)
	}

	return dialUnixGRPC(ctx, socketPath, "worker-core-shell")
}

func dialUnixGRPC(ctx context.Context, socketPath, targetName string) (*grpc.ClientConn, error) {
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

func NewUnixCoreServer(socketPath string, core api.WorkerCoreServiceServer, opts ...grpc.ServerOption) (*grpc.Server, net.Listener, error) {
	socketPath = strings.TrimSpace(socketPath)
	if socketPath == "" {
		return nil, nil, fmt.Errorf("worker core socket path is required")
	}

	if core == nil {
		return nil, nil, fmt.Errorf("worker core service is required")
	}

	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		return nil, nil, fmt.Errorf("remove stale worker core socket: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(socketPath), 0o755); err != nil {
		return nil, nil, fmt.Errorf("create worker core socket directory: %w", err)
	}

	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, nil, fmt.Errorf("listen worker core socket: %w", err)
	}

	if err := os.Chmod(socketPath, 0o600); err != nil {
		_ = ln.Close()
		_ = os.Remove(socketPath)
		return nil, nil, fmt.Errorf("chmod worker core socket: %w", err)
	}

	server := grpc.NewServer(opts...)
	api.RegisterWorkerCoreServiceServer(server, core)

	return server, ln, nil
}

func NewUnixShellServer(socketPath string, shell api.WorkerCoreShellServiceServer, opts ...grpc.ServerOption) (*grpc.Server, net.Listener, error) {
	socketPath = strings.TrimSpace(socketPath)
	if socketPath == "" {
		return nil, nil, fmt.Errorf("worker core shell socket path is required")
	}

	if shell == nil {
		return nil, nil, fmt.Errorf("worker core shell service is required")
	}

	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		return nil, nil, fmt.Errorf("remove stale worker core shell socket: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(socketPath), 0o755); err != nil {
		return nil, nil, fmt.Errorf("create worker core shell socket directory: %w", err)
	}

	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, nil, fmt.Errorf("listen worker core shell socket: %w", err)
	}

	if err := os.Chmod(socketPath, 0o600); err != nil {
		_ = ln.Close()
		_ = os.Remove(socketPath)
		return nil, nil, fmt.Errorf("chmod worker core shell socket: %w", err)
	}

	server := grpc.NewServer(opts...)
	api.RegisterWorkerCoreShellServiceServer(server, shell)

	return server, ln, nil
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
