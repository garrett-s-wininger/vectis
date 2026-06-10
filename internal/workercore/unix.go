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

func DialUnixCore(ctx context.Context, socketPath string) (*RemoteCore, func(), error) {
	socketPath = strings.TrimSpace(socketPath)
	if socketPath == "" {
		return nil, nil, fmt.Errorf("worker core socket path is required")
	}

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", socketPath)
	}

	conn, err := grpc.NewClient(
		"passthrough:///worker-core",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	if err != nil {
		return nil, nil, fmt.Errorf("dial worker core socket: %w", err)
	}

	if err := waitForClientConnReady(ctx, conn); err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("worker core socket not ready: %w", err)
	}

	return NewRemoteCore(api.NewWorkerCoreServiceClient(conn)), func() { _ = conn.Close() }, nil
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
