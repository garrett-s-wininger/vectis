package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	api "vectis/api/gen/go"
	sdk "vectis/sdk/workercore"

	"google.golang.org/protobuf/proto"
)

func main() {
	socketPath := flag.String("socket", "/tmp/vectis-worker-core-example.sock", "Unix socket served by the example worker core")
	flag.Parse()

	core := sampleCore{}
	server, listener, err := sdk.NewUnixCoreServer(*socketPath, core, sdk.ServiceOptions{})
	if err != nil {
		log.Fatalf("create worker core server: %v", err)
	}
	defer os.Remove(*socketPath)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		server.GracefulStop()
	}()

	log.Printf("example worker core listening on %s", *socketPath)
	if err := server.Serve(listener); err != nil && ctx.Err() == nil {
		log.Fatalf("serve worker core: %v", err)
	}
}

type sampleCore struct{}

func (sampleCore) Describe(context.Context) (sdk.Description, error) {
	return sdk.Description{
		ProtocolVersion:    sdk.ProtocolVersion,
		SupportedIsolation: []string{"host"},
		Capabilities: []sdk.Capability{
			{Name: "example.external-runner", Version: "v1"},
		},
		Metadata: map[string]string{
			"provider": "example",
		},
	}, nil
}

func (sampleCore) ExecuteTask(ctx context.Context, task sdk.Task) (sdk.Result, error) {
	if err := sendLog(ctx, task, fmt.Sprintf("example external core accepted task %q\n", task.TaskKey)); err != nil {
		return sdk.Result{}, err
	}

	timer := time.NewTimer(25 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return sdk.Unknown(ctx.Err().Error()), nil
	case <-timer.C:
	}

	if task.Session.ArtifactsEnabled() {
		if _, err := task.Session.PublishArtifact(ctx, sdk.ArtifactRequest{
			Name:        "external-core-summary",
			Path:        "external-core-summary.txt",
			ContentType: "text/plain",
			Reader:      strings.NewReader("example external core completed\n"),
		}); err != nil {
			return sdk.Result{}, err
		}
	}

	if err := sendLog(ctx, task, "example external core finished\n"); err != nil {
		return sdk.Result{}, err
	}

	return sdk.Success(), nil
}

func sendLog(ctx context.Context, task sdk.Task, message string) error {
	if !task.Session.LogsEnabled() {
		return nil
	}

	stream, err := task.Session.OpenLogStream(ctx)
	if err != nil {
		return err
	}

	if err := stream.Send(&api.LogChunk{
		RunId: proto.String(task.Job.GetRunId()),
		Data:  []byte(message),
	}); err != nil {
		_ = stream.Close()
		return err
	}

	return stream.Close()
}
