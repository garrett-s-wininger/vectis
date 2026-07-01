package conformance

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	api "vectis/api/gen/go"
	sdk "vectis/sdk/workercore"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type CoreFactory func(*testing.T) sdk.Core
type CoreEndpointFactory func(*testing.T) string

type Options struct {
	// RequireLogCallback verifies that ExecuteTask sends at least one log chunk
	// through the shell callback socket.
	RequireLogCallback bool
	// RequireArtifactCallback verifies that ExecuteTask publishes at least one
	// artifact through the shell callback socket.
	RequireArtifactCallback bool
	Timeout                 time.Duration
}

func RunCoreSuite(t *testing.T, factory CoreFactory, opts Options) {
	t.Helper()

	if factory == nil {
		t.Fatal("worker-core conformance factory is required")
	}

	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	t.Run("describe", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		desc, err := factory(t).Describe(ctx)
		if err != nil {
			t.Fatalf("Describe: %v", err)
		}

		validateDescription(t, desc, true)
	})

	t.Run("execute_success", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		sessionProto := &api.WorkerCoreTaskSession{
			SessionId:        proto.String("conformance-session"),
			LogsEnabled:      proto.Bool(opts.RequireLogCallback),
			ArtifactsEnabled: proto.Bool(opts.RequireArtifactCallback),
		}

		var shell *recordingShell
		var shellServer *grpc.Server
		if opts.RequireLogCallback || opts.RequireArtifactCallback {
			socketPath := shortSocketPath(t, "worker-core-shell.sock")
			shell = &recordingShell{artifact: &api.WorkerCoreArtifact{
				Name:            proto.String("conformance-artifact"),
				Path:            proto.String("conformance.txt"),
				ContentType:     proto.String("text/plain"),
				BlobKey:         proto.String("blob-conformance"),
				BlobAlgorithm:   proto.String("sha256"),
				BlobDigest:      proto.String("abc"),
				SizeBytes:       proto.Int64(1),
				ArtifactShardId: proto.String("artifact-conformance"),
			}}

			var listener net.Listener
			var err error
			shellServer, listener, err = sdk.NewUnixShellServer(socketPath, shell)
			if err != nil {
				t.Fatalf("NewUnixShellServer: %v", err)
			}
			defer shellServer.Stop()
			serveGRPC(t, shellServer, listener)

			sessionProto.ShellEndpoint = proto.String(sdk.UnixEndpoint(socketPath))
		}

		session, err := sdk.NewSession(sessionProto)
		if err != nil {
			t.Fatalf("NewSession: %v", err)
		}

		uses := "builtins/shell"
		job := &api.Job{
			Id:    proto.String("conformance-job"),
			RunId: proto.String("conformance-run"),
			Root: &api.Node{
				Id:   proto.String("root"),
				Uses: proto.String(uses),
				With: map[string]string{"command": "printf conformance"},
			},
		}

		result, err := factory(t).ExecuteTask(ctx, sdk.Task{
			Job:     job,
			TaskKey: "root",
			Session: session,
		})
		if err != nil {
			t.Fatalf("ExecuteTask: %v", err)
		}

		if result.Outcome != api.RunOutcome_RUN_OUTCOME_SUCCESS {
			t.Fatalf("outcome = %s message=%q, want success", result.Outcome, result.Message)
		}

		if opts.RequireLogCallback && len(shell.logBytes()) == 0 {
			t.Fatal("core did not send log chunks through the shell callback")
		}

		if opts.RequireArtifactCallback && len(shell.artifactBytes()) == 0 {
			t.Fatal("core did not publish an artifact through the shell callback")
		}
	})

	t.Run("cancel_task", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		if err := factory(t).CancelTask(ctx, sdk.CancelRequest{
			SessionID: "conformance-session",
			RunID:     "conformance-run",
			TaskKey:   "root",
			Reason:    "conformance",
		}); err != nil {
			t.Fatalf("CancelTask: %v", err)
		}
	})
}

func RunCoreServerSuite(t *testing.T, factory CoreEndpointFactory, opts Options) {
	t.Helper()

	if factory == nil {
		t.Fatal("worker-core server conformance factory is required")
	}

	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	t.Run("describe_protocol", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		_, client := dialCoreServer(t, ctx, factory)
		resp, err := client.DescribeCore(ctx, &api.DescribeWorkerCoreRequest{})
		if err != nil {
			t.Fatalf("DescribeCore: %v", err)
		}

		validateDescription(t, descriptionFromProto(resp), false)
	})

	t.Run("execute_success_protocol", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		_, client := dialCoreServer(t, ctx, factory)
		req, shell := executeRequest(t, opts)
		resp, err := client.ExecuteTask(ctx, req)
		if err != nil {
			t.Fatalf("ExecuteTask: %v", err)
		}

		if resp.GetOutcome() != api.RunOutcome_RUN_OUTCOME_SUCCESS {
			t.Fatalf("outcome = %s message=%q reason=%q, want success", resp.GetOutcome(), resp.GetMessage(), resp.GetReasonCode())
		}

		if got := resp.GetReasonCode(); got != "" {
			t.Fatalf("success reason code = %q, want empty", got)
		}

		if opts.RequireLogCallback && len(shell.logBytes()) == 0 {
			t.Fatal("core did not send log chunks through the shell callback")
		}

		if opts.RequireArtifactCallback && len(shell.artifactBytes()) == 0 {
			t.Fatal("core did not publish an artifact through the shell callback")
		}
	})

	t.Run("cancel_task_protocol", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		_, client := dialCoreServer(t, ctx, factory)
		if _, err := client.CancelTask(ctx, &api.CancelWorkerCoreTaskRequest{
			SessionId: proto.String("conformance-session"),
			RunId:     proto.String("conformance-run"),
			TaskKey:   proto.String("root"),
			Reason:    proto.String("conformance"),
		}); err != nil {
			t.Fatalf("CancelTask: %v", err)
		}
	})
}

func validateDescription(t *testing.T, desc sdk.Description, allowEmptyProtocol bool) {
	t.Helper()

	if desc.ProtocolVersion == "" && allowEmptyProtocol {
		// The SDK fills the current protocol version when serving DescribeCore.
	} else if desc.ProtocolVersion != sdk.ProtocolVersion {
		t.Fatalf("protocol version = %q, want %q", desc.ProtocolVersion, sdk.ProtocolVersion)
	}

	if len(desc.SupportedIsolation) == 0 {
		t.Fatal("SupportedIsolation is empty")
	}

	for _, capability := range []string{
		sdk.CapabilityExecute,
		sdk.CapabilityCancelTask,
		sdk.CapabilityShellLogCallback,
		sdk.CapabilityShellArtifactPush,
	} {
		if !sdk.HasCapability(desc, capability) {
			t.Fatalf("missing required capability %q", capability)
		}
	}
}

func dialCoreServer(t *testing.T, ctx context.Context, factory CoreEndpointFactory) (*grpc.ClientConn, api.WorkerCoreServiceClient) {
	t.Helper()

	endpoint := factory(t)
	conn, client, err := sdk.DialUnixCore(ctx, endpoint)
	if err != nil {
		t.Fatalf("DialUnixCore: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return conn, client
}

func executeRequest(t *testing.T, opts Options) (*api.ExecuteWorkerCoreTaskRequest, *recordingShell) {
	t.Helper()

	session := &api.WorkerCoreTaskSession{
		SessionId:        proto.String("conformance-session"),
		LogsEnabled:      proto.Bool(opts.RequireLogCallback),
		ArtifactsEnabled: proto.Bool(opts.RequireArtifactCallback),
	}

	var shell *recordingShell
	if opts.RequireLogCallback || opts.RequireArtifactCallback {
		socketPath := shortSocketPath(t, "worker-core-shell.sock")
		shell = &recordingShell{artifact: &api.WorkerCoreArtifact{
			Name:            proto.String("conformance-artifact"),
			Path:            proto.String("conformance.txt"),
			ContentType:     proto.String("text/plain"),
			BlobKey:         proto.String("blob-conformance"),
			BlobAlgorithm:   proto.String("sha256"),
			BlobDigest:      proto.String("abc"),
			SizeBytes:       proto.Int64(1),
			ArtifactShardId: proto.String("artifact-conformance"),
		}}

		server, listener, err := sdk.NewUnixShellServer(socketPath, shell)
		if err != nil {
			t.Fatalf("NewUnixShellServer: %v", err)
		}
		t.Cleanup(server.Stop)
		serveGRPC(t, server, listener)

		session.ShellEndpoint = proto.String(sdk.UnixEndpoint(socketPath))
	} else {
		shell = &recordingShell{}
	}

	uses := "builtins/shell"
	return &api.ExecuteWorkerCoreTaskRequest{
		Job: &api.Job{
			Id:    proto.String("conformance-job"),
			RunId: proto.String("conformance-run"),
			Root: &api.Node{
				Id:   proto.String("root"),
				Uses: proto.String(uses),
				With: map[string]string{"command": "printf conformance"},
			},
		},
		TaskKey: proto.String("root"),
		Session: session,
	}, shell
}

func descriptionFromProto(resp *api.DescribeWorkerCoreResponse) sdk.Description {
	if resp == nil {
		return sdk.Description{}
	}

	out := sdk.Description{
		ProtocolVersion:    resp.GetProtocolVersion(),
		SupportedIsolation: append([]string(nil), resp.GetSupportedIsolation()...),
		Metadata:           cloneStringMap(resp.GetMetadata()),
	}

	for _, capability := range resp.GetCapabilities() {
		if capability == nil {
			continue
		}

		out.Capabilities = append(out.Capabilities, sdk.Capability{
			Name:     capability.GetName(),
			Version:  capability.GetVersion(),
			Metadata: cloneStringMap(capability.GetMetadata()),
		})
	}

	return out
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}

	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}

	return out
}

type recordingShell struct {
	api.UnimplementedWorkerCoreShellServiceServer

	artifact *api.WorkerCoreArtifact
	logs     []byte
	data     []byte
}

func (s *recordingShell) StreamLogs(stream api.WorkerCoreShellService_StreamLogsServer) error {
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(&api.Empty{})
		}

		if err != nil {
			return err
		}

		s.logs = append(s.logs, chunk.GetChunk().GetData()...)
	}
}

func (s *recordingShell) PublishArtifact(stream api.WorkerCoreShellService_PublishArtifactServer) error {
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(s.artifact)
		}
		if err != nil {
			return err
		}

		s.data = append(s.data, chunk.GetData()...)
	}
}

func (s *recordingShell) logBytes() []byte {
	return append([]byte(nil), s.logs...)
}

func (s *recordingShell) artifactBytes() []byte {
	return append([]byte(nil), s.data...)
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

	dir, err := os.MkdirTemp(shortTempRoot(), "vectis-core-conf-") //nolint:usetesting // Keep Unix socket paths short on platforms with long test temp roots.
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	return filepath.Join(dir, name)
}

func shortTempRoot() string {
	if runtime.GOOS == "windows" {
		return ""
	}

	return "/tmp"
}
