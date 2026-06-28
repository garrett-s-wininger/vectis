package workercore

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/dal"
	"vectis/internal/testutil/socktest"

	"google.golang.org/grpc"
)

func TestUnixCoreTransportExecutesTask(t *testing.T) {
	socketPath := socktest.ShortPath(t, "worker-core.sock")
	server, listener, err := NewUnixCoreServer(socketPath, fakeWorkerCoreServer{
		execute: func(_ context.Context, req *api.ExecuteWorkerCoreTaskRequest) (*api.ExecuteWorkerCoreTaskResponse, error) {
			if req.GetTaskKey() != dal.RootTaskKey {
				t.Fatalf("task key = %q, want %q", req.GetTaskKey(), dal.RootTaskKey)
			}

			if req.GetSession().GetSessionId() != "session-1" {
				t.Fatalf("session id = %q, want session-1", req.GetSession().GetSessionId())
			}

			return &api.ExecuteWorkerCoreTaskResponse{Outcome: api.RunOutcome_RUN_OUTCOME_SUCCESS.Enum()}, nil
		},
	})

	if err != nil {
		t.Fatalf("NewUnixCoreServer: %v", err)
	}
	defer server.Stop()

	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("worker core server: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	core, cleanup, err := DialUnixCore(ctx, socketPath)
	if err != nil {
		t.Fatalf("DialUnixCore: %v", err)
	}
	defer cleanup()

	err = core.ExecuteTask(ctx, ExecuteTaskRequest{
		Job:     &api.Job{},
		TaskKey: dal.RootTaskKey,
		Session: NewTaskSession(TaskSessionOptions{
			SessionID: "session-1",
		}),
	})

	if err != nil {
		t.Fatalf("ExecuteTask over unix socket: %v", err)
	}
}

func TestUnixCoreTransportValidatesSocketPath(t *testing.T) {
	if _, _, err := NewUnixCoreServer("", fakeWorkerCoreServer{}); err == nil {
		t.Fatal("NewUnixCoreServer empty path error = nil")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, _, err := DialUnixCore(ctx, ""); err == nil {
		t.Fatal("DialUnixCore empty path error = nil")
	}
}

func TestUnixCoreServerSecuresSocketPath(t *testing.T) {
	socketPath := socktest.ShortPath(t, "worker-core.sock")
	server, listener, err := NewUnixCoreServer(socketPath, fakeWorkerCoreServer{})
	if err != nil {
		t.Fatalf("NewUnixCoreServer: %v", err)
	}
	defer server.Stop()
	defer listener.Close()

	assertSocketPathModes(t, socketPath)
}

func TestUnixShellServerSecuresSocketPath(t *testing.T) {
	socketPath := socktest.ShortPath(t, "worker-core-shell.sock")
	server, listener, err := NewUnixShellServer(socketPath, fakeWorkerCoreShellServer{})
	if err != nil {
		t.Fatalf("NewUnixShellServer: %v", err)
	}
	defer server.Stop()
	defer listener.Close()

	assertSocketPathModes(t, socketPath)
}

func TestUnixPeerCredentialsRequireCurrentUID(t *testing.T) {
	socketPath := socktest.ShortPath(t, "peercred.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	defer listener.Close()

	accepted := make(chan net.Conn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			acceptErr <- err
			return
		}

		accepted <- conn
	}()

	client, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatalf("dial unix: %v", err)
	}
	defer client.Close()

	var serverConn net.Conn
	select {
	case err := <-acceptErr:
		t.Fatalf("accept unix: %v", err)
	case serverConn = <-accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out accepting unix peer")
	}
	defer serverConn.Close()

	_, ok, err := unixPeerUID(serverConn)
	if err != nil {
		t.Fatalf("unixPeerUID: %v", err)
	}

	if !ok {
		t.Skip("peer credentials are not supported on this platform")
	}

	if err := verifyUnixPeerCredentials(serverConn, os.Getuid()); err != nil {
		t.Fatalf("verify current uid: %v", err)
	}

	if err := verifyUnixPeerCredentials(serverConn, os.Getuid()+1); err == nil {
		t.Fatal("verify wrong uid succeeded")
	}
}

func assertSocketPathModes(t *testing.T, socketPath string) {
	t.Helper()

	dirInfo, err := os.Stat(filepath.Dir(socketPath))
	if err != nil {
		t.Fatalf("stat socket dir: %v", err)
	}

	if got := dirInfo.Mode().Perm(); got != unixSocketDirMode {
		t.Fatalf("socket dir mode = %v, want %v", got, os.FileMode(unixSocketDirMode))
	}

	socketInfo, err := os.Stat(socketPath)
	if err != nil {
		t.Fatalf("stat socket: %v", err)
	}

	if got := socketInfo.Mode().Perm(); got != unixSocketFileMode {
		t.Fatalf("socket mode = %v, want %v", got, os.FileMode(unixSocketFileMode))
	}
}

type fakeWorkerCoreServer struct {
	api.UnimplementedWorkerCoreServiceServer
	describe func(context.Context, *api.DescribeWorkerCoreRequest) (*api.DescribeWorkerCoreResponse, error)
	execute  func(context.Context, *api.ExecuteWorkerCoreTaskRequest) (*api.ExecuteWorkerCoreTaskResponse, error)
}

func (s fakeWorkerCoreServer) DescribeCore(ctx context.Context, req *api.DescribeWorkerCoreRequest) (*api.DescribeWorkerCoreResponse, error) {
	if s.describe != nil {
		return s.describe(ctx, req)
	}

	return CoreDescriptionProto(CoreDescription{}), nil
}

func (s fakeWorkerCoreServer) ExecuteTask(ctx context.Context, req *api.ExecuteWorkerCoreTaskRequest) (*api.ExecuteWorkerCoreTaskResponse, error) {
	if s.execute != nil {
		return s.execute(ctx, req)
	}

	return &api.ExecuteWorkerCoreTaskResponse{}, nil
}

type fakeWorkerCoreShellServer struct {
	api.UnimplementedWorkerCoreShellServiceServer
}
