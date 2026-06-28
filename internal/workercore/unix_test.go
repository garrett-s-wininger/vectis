package workercore

import (
	"context"
	"errors"
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
