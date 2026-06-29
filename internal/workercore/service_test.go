package workercore

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/socktest"
	workersdk "vectis/sdk/workercore"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
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
		RunID:             "run-1",
		LogClient:         logClient,
		Logger:            mocks.NewMockLogger(),
		ArtifactPublisher: artifacts,
	}))

	if err != nil {
		t.Fatalf("RegisterSession: %v", err)
	}
	defer unregister()

	core := fakeCoreFunc(func(ctx context.Context, req ExecuteTaskRequest) error {
		if got, want := req.Session.CheckoutCacheRemoteURLs(), []string{"https://mirror.invalid/vectis.git"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("checkout cache remote urls = %+v, want %+v", got, want)
		}

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
		Job:     &api.Job{RunId: proto.String("run-1")},
		TaskKey: proto.String(dal.RootTaskKey),
		Session: &api.WorkerCoreTaskSession{
			SessionId:               proto.String("session-1"),
			ShellEndpoint:           proto.String(UnixEndpoint(socketPath)),
			LogsEnabled:             proto.Bool(true),
			ArtifactsEnabled:        proto.Bool(true),
			CheckoutCacheRemoteUrls: []string{"https://mirror.invalid/vectis.git"},
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

func TestShellServerRejectsMismatchedLogChunkRun(t *testing.T) {
	socketPath := socktest.ShortPath(t, "worker-core-shell.sock")
	shell := NewShellServer()
	server, listener, err := NewUnixShellServer(socketPath, shell)
	if err != nil {
		t.Fatalf("NewUnixShellServer: %v", err)
	}
	defer server.Stop()

	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("worker core shell server: %v", err)
		}
	}()

	logClient := mocks.NewMockLogClient()
	unregister, err := shell.RegisterSession(NewTaskSession(TaskSessionOptions{
		SessionID: "session-1",
		RunID:     "run-1",
		LogClient: logClient,
		Logger:    mocks.NewMockLogger(),
	}))
	if err != nil {
		t.Fatalf("RegisterSession: %v", err)
	}
	defer unregister()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := dialUnixShell(ctx, UnixEndpoint(socketPath))
	if err != nil {
		t.Fatalf("dial shell: %v", err)
	}
	defer conn.Close()

	stream, err := api.NewWorkerCoreShellServiceClient(conn).StreamLogs(ctx)
	if err != nil {
		t.Fatalf("StreamLogs: %v", err)
	}

	err = stream.Send(&api.WorkerCoreLogChunk{
		SessionId: proto.String("session-1"),
		Chunk: &api.LogChunk{
			RunId: proto.String("run-2"),
			Data:  []byte("wrong run"),
		},
	})

	if err == nil {
		_, err = stream.CloseAndRecv()
	}

	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("StreamLogs error = %v, want InvalidArgument", err)
	}

	if !strings.Contains(status.Convert(err).Message(), "run_id") {
		t.Fatalf("StreamLogs error message = %q, want run_id", status.Convert(err).Message())
	}

	if chunks := logClient.GetChunks(); len(chunks) != 0 {
		t.Fatalf("mismatched log chunk was forwarded: %+v", chunks)
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

func TestServiceExecuteTaskRejectsMismatchedWorkloadIdentity(t *testing.T) {
	jobID := "job-worker-core"
	runID := "run-worker-core"
	coreCalled := false
	service := NewService(fakeCoreFunc(func(context.Context, ExecuteTaskRequest) error {
		coreCalled = true
		return nil
	}), ServiceOptions{Logger: mocks.NewMockLogger()})

	_, err := service.ExecuteTask(context.Background(), &api.ExecuteWorkerCoreTaskRequest{
		Job: &api.Job{
			Id:    proto.String(jobID),
			RunId: proto.String(runID),
			Root:  &api.Node{},
		},
		TaskKey: proto.String(dal.RootTaskKey),
		Session: &api.WorkerCoreTaskSession{
			SessionId: proto.String("execution-1"),
			WorkloadIdentity: &api.WorkerCoreWorkloadIdentity{
				SpiffeId:    proto.String("spiffe://vectis.local/cell/local/job/job-worker-core/run/other-run/execution/execution-1"),
				JobId:       proto.String(jobID),
				RunId:       proto.String("other-run"),
				ExecutionId: proto.String("execution-1"),
			},
		},
	})

	if err == nil {
		t.Fatal("ExecuteTask accepted mismatched workload identity")
	}

	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("ExecuteTask error = %v, want InvalidArgument", err)
	}

	if !strings.Contains(st.Message(), "run_id") {
		t.Fatalf("ExecuteTask error message = %q, want run_id mismatch", st.Message())
	}

	if coreCalled {
		t.Fatal("core was called for mismatched workload identity")
	}
}

func TestServiceExecuteTaskMapsCoreFailureReasonCode(t *testing.T) {
	service := NewService(fakeCoreFunc(func(context.Context, ExecuteTaskRequest) error {
		return errors.New("provider rejected task")
	}), ServiceOptions{Logger: mocks.NewMockLogger()})

	resp, err := service.ExecuteTask(context.Background(), &api.ExecuteWorkerCoreTaskRequest{
		Job:     &api.Job{},
		TaskKey: proto.String(dal.RootTaskKey),
		Session: &api.WorkerCoreTaskSession{
			SessionId: proto.String("session-1"),
		},
	})

	if err != nil {
		t.Fatalf("ExecuteTask: %v", err)
	}

	if resp.GetOutcome() != api.RunOutcome_RUN_OUTCOME_FAILURE {
		t.Fatalf("outcome = %s, want failure", resp.GetOutcome())
	}

	if resp.GetReasonCode() != workersdk.ReasonExecutionFailed {
		t.Fatalf("reason code = %q, want %q", resp.GetReasonCode(), workersdk.ReasonExecutionFailed)
	}

	if !strings.Contains(resp.GetMessage(), "provider rejected task") {
		t.Fatalf("message = %q, want provider error", resp.GetMessage())
	}
}

func TestServiceWarmCheckoutCache(t *testing.T) {
	core := &recordingWarmCore{
		result: WarmCheckoutCacheResult{
			Warmed: 1,
			Failures: []CheckoutCacheWarmFailure{
				{RemoteURL: "https://mirror.invalid/fail.git", Message: "fetch failed"},
			},
		},
	}

	service := NewService(core, ServiceOptions{Logger: mocks.NewMockLogger()})
	resp, err := service.WarmCheckoutCache(context.Background(), &api.WarmWorkerCoreCheckoutCacheRequest{
		RemoteUrls: []string{"https://mirror.invalid/warm.git"},
	})

	if err != nil {
		t.Fatalf("WarmCheckoutCache: %v", err)
	}

	if !reflect.DeepEqual(core.req.RemoteURLs, []string{"https://mirror.invalid/warm.git"}) {
		t.Fatalf("warm request = %+v", core.req)
	}

	if resp.GetWarmed() != 1 ||
		len(resp.GetFailures()) != 1 ||
		resp.GetFailures()[0].GetRemoteUrl() != "https://mirror.invalid/fail.git" ||
		resp.GetFailures()[0].GetMessage() != "fetch failed" {
		t.Fatalf("warm response = %+v", resp)
	}
}

type fakeCoreFunc func(context.Context, ExecuteTaskRequest) error

func (f fakeCoreFunc) ExecuteTask(ctx context.Context, req ExecuteTaskRequest) error {
	return f(ctx, req)
}

type recordingWarmCore struct {
	req    WarmCheckoutCacheRequest
	result WarmCheckoutCacheResult
}

func (c *recordingWarmCore) ExecuteTask(context.Context, ExecuteTaskRequest) error {
	return nil
}

func (c *recordingWarmCore) WarmCheckoutCache(_ context.Context, req WarmCheckoutCacheRequest) (WarmCheckoutCacheResult, error) {
	c.req = req
	return c.result, nil
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
