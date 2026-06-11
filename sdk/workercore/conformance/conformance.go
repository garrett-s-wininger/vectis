package conformance

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	api "vectis/api/gen/go"
	sdk "vectis/sdk/workercore"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type CoreFactory func(*testing.T) sdk.Core

type Options struct {
	RequireLogCallback      bool
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

		if desc.ProtocolVersion != "" && desc.ProtocolVersion != sdk.ProtocolVersion {
			t.Fatalf("protocol version = %q, want empty or %q", desc.ProtocolVersion, sdk.ProtocolVersion)
		}

		if len(desc.SupportedIsolation) == 0 {
			t.Fatal("SupportedIsolation is empty")
		}
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
		if err == io.EOF {
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
		if err == io.EOF {
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
		if err := server.Serve(listener); err != nil && err != grpc.ErrServerStopped {
			t.Errorf("grpc server: %v", err)
		}
	}()
}

func shortSocketPath(t *testing.T, name string) string {
	t.Helper()

	dir, err := os.MkdirTemp("/tmp", "vectis-core-conf-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	return filepath.Join(dir, name)
}
