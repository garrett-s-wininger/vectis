package workercore

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/socktest"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func TestServiceExecuteTaskUsesShellCallbacks(t *testing.T) {
	socketPath := socktest.ShortPath(t, "worker-core-shell.sock")
	shell := NewShellServer()
	server, listener, err := NewUnixShellServer(socketPath, shell)
	if err != nil {
		t.Fatalf("NewUnixShellServer: %v", err)
	}
	defer server.Stop()

	go func() {
		if err := server.Serve(listener); err != nil && err != grpc.ErrServerStopped {
			t.Errorf("worker core shell server: %v", err)
		}
	}()

	logClient := mocks.NewMockLogClient()
	artifacts := &recordingShellArtifactPublisher{
		result: action.ArtifactPublishResult{
			Name:            "artifact",
			BlobKey:         "blob-1",
			BlobAlgorithm:   "sha256",
			BlobDigest:      "abc",
			SizeBytes:       7,
			ArtifactShardID: "artifact-a",
		},
	}

	unregister, err := shell.RegisterSession(NewTaskSession(TaskSessionOptions{
		SessionID:         "session-1",
		LogClient:         logClient,
		Logger:            mocks.NewMockLogger(),
		ArtifactPublisher: artifacts,
	}))

	if err != nil {
		t.Fatalf("RegisterSession: %v", err)
	}
	defer unregister()

	core := fakeCoreFunc(func(ctx context.Context, req ExecuteTaskRequest) error {
		stream, err := req.Session.LogClient().StreamLogs(ctx)
		if err != nil {
			return err
		}

		runID := "run-1"
		if err := stream.Send(&api.LogChunk{RunId: &runID, Data: []byte("hello")}); err != nil {
			return err
		}

		if err := stream.CloseSend(); err != nil {
			return err
		}

		result, err := req.Session.ArtifactPublisher().PublishArtifact(ctx, action.ArtifactPublishRequest{
			Name:        "artifact",
			Path:        "artifact.txt",
			ContentType: "text/plain",
			Reader:      strings.NewReader("payload"),
		})

		if err != nil {
			return err
		}

		if result.BlobKey != "blob-1" {
			t.Fatalf("artifact result blob key = %q, want blob-1", result.BlobKey)
		}

		return nil
	})

	service := NewService(core, ServiceOptions{Logger: mocks.NewMockLogger()})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := service.ExecuteTask(ctx, &api.ExecuteWorkerCoreTaskRequest{
		Job:     &api.Job{},
		TaskKey: proto.String(dal.RootTaskKey),
		Session: &api.WorkerCoreTaskSession{
			SessionId:        proto.String("session-1"),
			ShellEndpoint:    proto.String(UnixEndpoint(socketPath)),
			LogsEnabled:      proto.Bool(true),
			ArtifactsEnabled: proto.Bool(true),
		},
	})

	if err != nil {
		t.Fatalf("ExecuteTask: %v", err)
	}

	if resp.GetOutcome() != api.RunOutcome_RUN_OUTCOME_SUCCESS {
		t.Fatalf("outcome = %s", resp.GetOutcome())
	}

	chunks := logClient.GetChunks()
	if len(chunks) != 1 || string(chunks[0].GetData()) != "hello" {
		t.Fatalf("forwarded log chunks = %#v", chunks)
	}

	if artifacts.data != "payload" {
		t.Fatalf("artifact payload = %q, want payload", artifacts.data)
	}

	if artifacts.name != "artifact" || artifacts.path != "artifact.txt" {
		t.Fatalf("artifact request name/path = %q/%q", artifacts.name, artifacts.path)
	}
}

func TestServiceCancelTaskForwardsToCore(t *testing.T) {
	core := &recordingCancellableCore{}
	service := NewService(core, ServiceOptions{Logger: mocks.NewMockLogger()})

	resp, err := service.CancelTask(context.Background(), &api.CancelWorkerCoreTaskRequest{
		SessionId: proto.String("execution-1"),
		RunId:     proto.String("run-1"),
		TaskKey:   proto.String("root"),
		Reason:    proto.String("durable request"),
	})

	if err != nil {
		t.Fatalf("CancelTask: %v", err)
	}

	if resp == nil {
		t.Fatal("CancelTask response is nil")
	}

	if core.cancel.SessionID != "execution-1" || core.cancel.RunID != "run-1" || core.cancel.Reason != "durable request" {
		t.Fatalf("cancel request = %#v", core.cancel)
	}
}

type fakeCoreFunc func(context.Context, ExecuteTaskRequest) error

func (f fakeCoreFunc) ExecuteTask(ctx context.Context, req ExecuteTaskRequest) error {
	return f(ctx, req)
}

type recordingCancellableCore struct {
	cancel CancelTaskRequest
}

func (c *recordingCancellableCore) ExecuteTask(context.Context, ExecuteTaskRequest) error {
	return nil
}

func (c *recordingCancellableCore) CancelTask(_ context.Context, req CancelTaskRequest) error {
	c.cancel = req
	return nil
}

type recordingShellArtifactPublisher struct {
	result action.ArtifactPublishResult
	name   string
	path   string
	data   string
}

func (p *recordingShellArtifactPublisher) PublishArtifact(_ context.Context, req action.ArtifactPublishRequest) (action.ArtifactPublishResult, error) {
	data, err := io.ReadAll(req.Reader)
	if err != nil {
		return action.ArtifactPublishResult{}, err
	}

	p.name = req.Name
	p.path = req.Path
	p.data = string(data)

	return p.result, nil
}
