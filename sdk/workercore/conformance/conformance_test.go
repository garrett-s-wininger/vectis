package conformance_test

import (
	"context"
	"strings"
	"testing"

	api "vectis/api/gen/go"
	sdk "vectis/sdk/workercore"
	"vectis/sdk/workercore/conformance"

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
