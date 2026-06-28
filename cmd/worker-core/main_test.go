package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/socktest"
	"vectis/internal/workercore"

	"google.golang.org/grpc"
)

const workerCoreProcessHelperEnv = "VECTIS_WORKER_CORE_PROCESS_HELPER"

func TestWorkerCoreProcessSmoke(t *testing.T) {
	socketPath := socktest.ShortPath(t, "worker-core.sock")
	shellSocketPath := socktest.ShortPath(t, "worker-core-shell.sock")
	workspaceRoot := t.TempDir()

	var output bytes.Buffer
	cmd := exec.Command(os.Args[0], "-test.run=^TestWorkerCoreProcessHelper$")
	cmd.Env = append(os.Environ(),
		workerCoreProcessHelperEnv+"=1",
		"VECTIS_WORKER_CORE_PROCESS_SOCKET="+socketPath,
		"VECTIS_WORKER_CORE_PROCESS_WORKSPACE_ROOT="+workspaceRoot,
	)

	cmd.Stdout = &output
	cmd.Stderr = &output

	if err := cmd.Start(); err != nil {
		t.Fatalf("start worker core helper: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() {
		stopWorkerCoreProcess(t, cmd, done, &output)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	core, cleanup := dialWorkerCoreProcess(t, ctx, socketPath, done, &output)
	defer cleanup()

	desc, err := core.Describe(ctx)
	if err != nil {
		t.Fatalf("Describe: %v\n%s", err, output.String())
	}

	if err := workercore.ValidateCoreDescription(desc, workercore.RequiredWorkerCoreCapabilities()); err != nil {
		t.Fatalf("ValidateCoreDescription: %v", err)
	}

	shell := workercore.NewShellServer()
	shellServer, shellListener, err := workercore.NewUnixShellServer(shellSocketPath, shell)
	if err != nil {
		t.Fatalf("NewUnixShellServer: %v", err)
	}
	defer shellServer.Stop()

	go func() {
		if err := shellServer.Serve(shellListener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("worker core shell server: %v", err)
		}
	}()

	logClient := mocks.NewMockLogClient()
	artifacts := &recordingProcessArtifactPublisher{}
	sessionID := "execution-smoke"
	runID := "run-worker-core-process-smoke"
	unregister, err := shell.RegisterSession(workercore.NewTaskSession(workercore.TaskSessionOptions{
		SessionID:         sessionID,
		RunID:             runID,
		ShellEndpoint:     workercore.UnixEndpoint(shellSocketPath),
		Logger:            interfaces.NewLogger("worker-core-process-test"),
		LogClient:         logClient,
		ArtifactPublisher: artifacts,
	}))

	if err != nil {
		t.Fatalf("RegisterSession: %v", err)
	}
	defer unregister()

	jobID := "job-worker-core-process-smoke"
	rootID := "root"
	writeID := "write-artifact"
	uploadID := "upload-artifact"
	sequenceUses := "builtins/sequence"
	shellUses := "builtins/shell"
	uploadUses := "builtins/upload-artifact"

	job := &api.Job{
		Id:    &jobID,
		RunId: &runID,
		Root: &api.Node{
			Id:   &rootID,
			Uses: &sequenceUses,
			Steps: []*api.Node{
				{
					Id:   &writeID,
					Uses: &shellUses,
					With: map[string]string{
						"command": "printf smoke-log && printf smoke-artifact > artifact.txt",
					},
				},
				{
					Id:   &uploadID,
					Uses: &uploadUses,
					With: map[string]string{
						"name":         "smoke",
						"path":         "artifact.txt",
						"content_type": "text/plain",
					},
				},
			},
		},
	}

	if err := core.ExecuteTask(ctx, workercore.ExecuteTaskRequest{
		Job:     job,
		TaskKey: dal.RootTaskKey,
		Session: workercore.NewTaskSession(workercore.TaskSessionOptions{
			SessionID:         sessionID,
			ShellEndpoint:     workercore.UnixEndpoint(shellSocketPath),
			LogClient:         logClient,
			Logger:            interfaces.NewLogger("worker-core-process-test"),
			ArtifactPublisher: artifacts,
		}),
	}); err != nil {
		t.Fatalf("ExecuteTask: %v\n%s", err, output.String())
	}

	logData := logChunksString(logClient.GetChunks())
	if !strings.Contains(logData, "smoke-log") {
		t.Fatalf("log data missing smoke output: %q", logData)
	}

	if artifacts.data != "smoke-artifact" {
		t.Fatalf("artifact data = %q, want smoke-artifact", artifacts.data)
	}

	if artifacts.name != "smoke" || artifacts.path != "artifact.txt" || artifacts.contentType != "text/plain" {
		t.Fatalf("artifact request = name:%q path:%q content_type:%q", artifacts.name, artifacts.path, artifacts.contentType)
	}

	if err := core.CancelTask(ctx, workercore.CancelTaskRequest{
		SessionID: sessionID,
		RunID:     runID,
		TaskKey:   dal.RootTaskKey,
		Reason:    "smoke",
	}); err != nil {
		t.Fatalf("CancelTask: %v", err)
	}
}

func TestWorkerCoreProcessHelper(t *testing.T) {
	if os.Getenv(workerCoreProcessHelperEnv) != "1" {
		return
	}

	socketPath := os.Getenv("VECTIS_WORKER_CORE_PROCESS_SOCKET")
	workspaceRoot := os.Getenv("VECTIS_WORKER_CORE_PROCESS_WORKSPACE_ROOT")
	if socketPath == "" || workspaceRoot == "" {
		t.Fatal("worker core helper missing socket or workspace root")
	}

	os.Args = []string{
		"vectis-worker-core",
		"--socket", socketPath,
		"--execution-backend", "host",
		"--workspace-root", workspaceRoot,
	}

	main()
}

func dialWorkerCoreProcess(t *testing.T, ctx context.Context, socketPath string, done <-chan error, output *bytes.Buffer) (*workercore.RemoteCore, func()) {
	t.Helper()

	var lastErr error
	for {
		select {
		case err := <-done:
			t.Fatalf("worker core helper exited before readiness: %v\n%s", err, output.String())
		default:
		}

		dialCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		core, cleanup, err := workercore.DialUnixCore(dialCtx, socketPath)
		cancel()
		if err == nil {
			return core, cleanup
		}

		lastErr = err
		if ctx.Err() != nil {
			t.Fatalf("timed out dialing worker core helper: %v; last error: %v\n%s", ctx.Err(), lastErr, output.String())
		}

		time.Sleep(25 * time.Millisecond)
	}
}

func stopWorkerCoreProcess(t *testing.T, cmd *exec.Cmd, done <-chan error, output *bytes.Buffer) {
	t.Helper()

	if cmd.Process == nil || cmd.ProcessState != nil {
		return
	}

	_ = cmd.Process.Signal(os.Interrupt)
	select {
	case err := <-done:
		if err != nil {
			t.Logf("worker core helper exited after interrupt: %v\n%s", err, output.String())
		}
	case <-time.After(2 * time.Second):
		_ = cmd.Process.Kill()
		err := <-done
		t.Logf("worker core helper killed after timeout: %v\n%s", err, output.String())
	}
}

func logChunksString(chunks []*api.LogChunk) string {
	var b strings.Builder
	for _, chunk := range chunks {
		b.Write(chunk.GetData())
	}

	return b.String()
}

type recordingProcessArtifactPublisher struct {
	mu          sync.Mutex
	name        string
	path        string
	contentType string
	data        string
}

func (p *recordingProcessArtifactPublisher) PublishArtifact(_ context.Context, req action.ArtifactPublishRequest) (action.ArtifactPublishResult, error) {
	data, err := io.ReadAll(req.Reader)
	if err != nil {
		return action.ArtifactPublishResult{}, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.name = req.Name
	p.path = filepath.ToSlash(req.Path)
	p.contentType = req.ContentType
	p.data = string(data)

	return action.ArtifactPublishResult{
		Name:            req.Name,
		Path:            filepath.ToSlash(req.Path),
		ContentType:     req.ContentType,
		BlobKey:         "blob-smoke",
		BlobAlgorithm:   "sha256",
		BlobDigest:      "digest-smoke",
		SizeBytes:       int64(len(data)),
		ArtifactShardID: "artifact-smoke",
	}, nil
}
