package conformance_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	api "vectis/api/gen/go"
	sdk "vectis/sdk/workercore"
	"vectis/sdk/workercore/conformance"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func TestRunCoreSuite(t *testing.T) {
	conformance.RunCoreSuite(t, func(t *testing.T) sdk.Core {
		t.Helper()
		return fixtureCore{}
	}, conformance.Options{
		RequireLogCallback:      true,
		RequireArtifactCallback: true,
	})
}

func TestRunCoreServerSuite(t *testing.T) {
	conformance.RunCoreServerSuite(t, func(t *testing.T) string {
		t.Helper()

		socketPath := shortSocketPath(t, "worker-core.sock")
		server, listener, err := sdk.NewUnixCoreServer(socketPath, fixtureCore{}, sdk.ServiceOptions{})
		if err != nil {
			t.Fatalf("NewUnixCoreServer: %v", err)
		}
		t.Cleanup(server.Stop)

		go func() {
			if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				t.Errorf("worker core server: %v", err)
			}
		}()

		return socketPath
	}, conformance.Options{
		RequireLogCallback:      true,
		RequireArtifactCallback: true,
	})
}

type fixtureCore struct{}

func (fixtureCore) Describe(context.Context) (sdk.Description, error) {
	return sdk.Description{
		ProtocolVersion:    sdk.ProtocolVersion,
		SupportedIsolation: []string{"host"},
		Capabilities: []sdk.Capability{
			{Name: sdk.CapabilityExecute, Version: "v1"},
			{Name: sdk.CapabilityCancelTask, Version: "v1"},
			{Name: sdk.CapabilityShellLogCallback, Version: "v1"},
			{Name: sdk.CapabilityShellArtifactPush, Version: "v1"},
		},
	}, nil
}

func (fixtureCore) ExecuteTask(ctx context.Context, task sdk.Task) (sdk.Result, error) {
	stream, err := task.Session.OpenLogStream(ctx)
	if err != nil {
		return sdk.Result{}, err
	}

	if err := stream.Send(&api.LogChunk{
		RunId: proto.String(task.Job.GetRunId()),
		Data:  []byte("conformance log\n"),
	}); err != nil {
		_ = stream.Close()
		return sdk.Result{}, err
	}

	if err := stream.Close(); err != nil {
		return sdk.Result{}, err
	}

	if _, err := task.Session.PublishArtifact(ctx, sdk.ArtifactRequest{
		Name:        "conformance",
		Path:        "conformance.txt",
		ContentType: "text/plain",
		Reader:      strings.NewReader("artifact"),
	}); err != nil {
		return sdk.Result{}, err
	}

	return sdk.Success(), nil
}

func (fixtureCore) CancelTask(context.Context, sdk.CancelRequest) error {
	return nil
}

func shortSocketPath(t *testing.T, name string) string {
	t.Helper()

	dir, err := os.MkdirTemp("/tmp", "vectis-core-conf-test-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	return filepath.Join(dir, name)
}
