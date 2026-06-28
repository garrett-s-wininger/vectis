package workercore

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	api "vectis/api/gen/go"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func TestSDKServiceExecutesTaskWithShellCallbacks(t *testing.T) {
	shellSocket := shortSocketPath(t, "worker-core-shell.sock")
	shell := &recordingShellServer{
		artifact: &api.WorkerCoreArtifact{
			Name:            proto.String("artifact"),
			Path:            proto.String("artifact.txt"),
			ContentType:     proto.String("text/plain"),
			BlobKey:         proto.String("blob-1"),
			BlobAlgorithm:   proto.String("sha256"),
			BlobDigest:      proto.String("abc"),
			SizeBytes:       proto.Int64(7),
			ArtifactShardId: proto.String("artifact-a"),
		},
	}

	shellServer, shellListener, err := NewUnixShellServer(shellSocket, shell)
	if err != nil {
		t.Fatalf("NewUnixShellServer: %v", err)
	}
	defer shellServer.Stop()

	serveGRPC(t, shellServer, shellListener)
	coreSocket := shortSocketPath(t, "worker-core.sock")
	coreServer, coreListener, err := NewUnixCoreServer(coreSocket, callbackCore{}, ServiceOptions{})
	if err != nil {
		t.Fatalf("NewUnixCoreServer: %v", err)
	}
	defer coreServer.Stop()

	serveGRPC(t, coreServer, coreListener)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, client, err := DialUnixCore(ctx, coreSocket)
	if err != nil {
		t.Fatalf("DialUnixCore: %v", err)
	}
	defer conn.Close()

	desc, err := client.DescribeCore(ctx, &api.DescribeWorkerCoreRequest{})
	if err != nil {
		t.Fatalf("DescribeCore: %v", err)
	}

	if desc.GetProtocolVersion() != ProtocolVersion {
		t.Fatalf("protocol version = %q, want %q", desc.GetProtocolVersion(), ProtocolVersion)
	}

	resp, err := client.ExecuteTask(ctx, &api.ExecuteWorkerCoreTaskRequest{
		Job: &api.Job{
			RunId: proto.String("run-1"),
		},
		TaskKey: proto.String("root"),
		Session: &api.WorkerCoreTaskSession{
			SessionId:        proto.String("session-1"),
			ShellEndpoint:    proto.String(UnixEndpoint(shellSocket)),
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

	if got := shell.logData(); got != "hello from sdk core\n" {
		t.Fatalf("log data = %q", got)
	}

	if got := shell.artifactData(); got != "payload" {
		t.Fatalf("artifact data = %q", got)
	}
}

func TestSDKServiceCancelTaskForwardsToCore(t *testing.T) {
	core := &recordingCore{}
	service := NewService(core, ServiceOptions{})

	resp, err := service.CancelTask(context.Background(), &api.CancelWorkerCoreTaskRequest{
		SessionId: proto.String("execution-1"),
		RunId:     proto.String("run-1"),
		TaskKey:   proto.String("root"),
		Reason:    proto.String("remote request"),
	})

	if err != nil {
		t.Fatalf("CancelTask: %v", err)
	}

	if resp == nil {
		t.Fatal("CancelTask response is nil")
	}

	if core.cancel.SessionID != "execution-1" || core.cancel.RunID != "run-1" || core.cancel.Reason != "remote request" {
		t.Fatalf("cancel request = %#v", core.cancel)
	}
}

func TestSDKServiceExecuteTaskReturnsReasonCode(t *testing.T) {
	service := NewService(resultCore{
		result: ExternalUnavailable("jenkins unavailable"),
	}, ServiceOptions{})

	resp, err := service.ExecuteTask(context.Background(), &api.ExecuteWorkerCoreTaskRequest{
		Job:     &api.Job{},
		TaskKey: proto.String("root"),
		Session: &api.WorkerCoreTaskSession{
			SessionId: proto.String("session-1"),
		},
	})

	if err != nil {
		t.Fatalf("ExecuteTask: %v", err)
	}

	if resp.GetOutcome() != api.RunOutcome_RUN_OUTCOME_UNKNOWN {
		t.Fatalf("outcome = %s, want unknown", resp.GetOutcome())
	}

	if resp.GetReasonCode() != ReasonExternalUnavailable {
		t.Fatalf("reason code = %q, want %q", resp.GetReasonCode(), ReasonExternalUnavailable)
	}

	if resp.GetMessage() != "jenkins unavailable" {
		t.Fatalf("message = %q, want provider message", resp.GetMessage())
	}
}

func TestResultHelpersNormalizeReasonCodes(t *testing.T) {
	tests := []struct {
		name   string
		result Result
		want   string
	}{
		{name: "failure", result: Failure("failed"), want: ReasonExecutionFailed},
		{name: "unknown", result: Unknown("lost"), want: ReasonUnknown},
		{name: "cancelled", result: Cancelled("stop"), want: ReasonCancelled},
		{name: "custom", result: FailureWithReason("jenkins.stage_failed", "failed"), want: "jenkins.stage_failed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := resultProto(tt.result)
			if resp.GetReasonCode() != tt.want {
				t.Fatalf("reason code = %q, want %q", resp.GetReasonCode(), tt.want)
			}
		})
	}
}

type callbackCore struct{}

func (callbackCore) Describe(context.Context) (Description, error) {
	return Description{
		Capabilities:       []Capability{{Name: "sdk-test", Version: "v1"}},
		SupportedIsolation: []string{"host"},
	}, nil
}

func (callbackCore) ExecuteTask(ctx context.Context, task Task) (Result, error) {
	stream, err := task.Session.OpenLogStream(ctx)
	if err != nil {
		return Result{}, err
	}

	if err := stream.Send(&api.LogChunk{
		RunId: proto.String(task.Job.GetRunId()),
		Data:  []byte("hello from sdk core\n"),
	}); err != nil {
		_ = stream.Close()
		return Result{}, err
	}

	if err := stream.Close(); err != nil {
		return Result{}, err
	}

	artifact, err := task.Session.PublishArtifact(ctx, ArtifactRequest{
		Name:        "artifact",
		Path:        "artifact.txt",
		ContentType: "text/plain",
		Reader:      strings.NewReader("payload"),
	})

	if err != nil {
		return Result{}, err
	}

	if artifact.BlobKey != "blob-1" {
		return Failuref("artifact blob key = %q", artifact.BlobKey), nil
	}

	return Success(), nil
}

func (callbackCore) CancelTask(context.Context, CancelRequest) error {
	return nil
}

type recordingCore struct {
	cancel CancelRequest
}

func (c *recordingCore) Describe(context.Context) (Description, error) {
	return Description{
		SupportedIsolation: []string{"host"},
		Capabilities:       []Capability{{Name: CapabilityExecute, Version: "v1"}, {Name: CapabilityCancelTask, Version: "v1"}},
	}, nil
}

func (c *recordingCore) ExecuteTask(context.Context, Task) (Result, error) {
	return Success(), nil
}

func (c *recordingCore) CancelTask(_ context.Context, req CancelRequest) error {
	c.cancel = req
	return nil
}

type resultCore struct {
	result Result
	err    error
}

func (c resultCore) Describe(context.Context) (Description, error) {
	return Description{
		SupportedIsolation: []string{"host"},
		Capabilities:       []Capability{{Name: CapabilityExecute, Version: "v1"}, {Name: CapabilityCancelTask, Version: "v1"}},
	}, nil
}

func (c resultCore) ExecuteTask(context.Context, Task) (Result, error) {
	return c.result, c.err
}

func (c resultCore) CancelTask(context.Context, CancelRequest) error {
	return nil
}

type recordingShellServer struct {
	api.UnimplementedWorkerCoreShellServiceServer

	mu       sync.Mutex
	logs     []byte
	artifact *api.WorkerCoreArtifact
	data     []byte
}

func (s *recordingShellServer) StreamLogs(stream api.WorkerCoreShellService_StreamLogsServer) error {
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(&api.Empty{})
		}

		if err != nil {
			return err
		}

		s.mu.Lock()
		s.logs = append(s.logs, chunk.GetChunk().GetData()...)
		s.mu.Unlock()
	}
}

func (s *recordingShellServer) PublishArtifact(stream api.WorkerCoreShellService_PublishArtifactServer) error {
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(s.artifact)
		}

		if err != nil {
			return err
		}

		s.mu.Lock()
		s.data = append(s.data, chunk.GetData()...)
		s.mu.Unlock()
	}
}

func (s *recordingShellServer) logData() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return string(s.logs)
}

func (s *recordingShellServer) artifactData() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return string(s.data)
}

func serveGRPC(t *testing.T, server *grpc.Server, listener net.Listener) {
	t.Helper()

	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("grpc server: %v", err)
		}
	}()
}

func shortSocketPath(t *testing.T, name string) string {
	t.Helper()

	dir, err := os.MkdirTemp("/tmp", "vectis-sdk-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	return filepath.Join(dir, name)
}
