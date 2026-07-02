package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spf13/viper"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
	sourcepkg "vectis/internal/source"
	"vectis/internal/testutil/socktest"
	"vectis/internal/workercore"
	workersdk "vectis/sdk/workercore"

	"google.golang.org/grpc"
)

func TestWorkerCorePersistentCheckoutCacheRemoteURLs(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("VECTIS_SOURCE_REPOSITORIES", "")
	t.Setenv("VECTIS_API_SERVER_SOURCE_REPOSITORIES", "")
	t.Setenv("VECTIS_WORKER_SOURCE_REPOSITORIES", "")
	t.Setenv("VECTIS_WORKER_CORE_SOURCE_REPOSITORIES", `[
		{
			"repository_id":"large",
			"checkout_mode":"managed",
			"worker_cache_mode":"persistent",
			"canonical_url":"https://mirror.invalid/large.git",
			"fallback_remote_urls":["https://origin.invalid/large.git"]
		},
		{
			"repository_id":"small",
			"checkout_mode":"managed",
			"worker_cache_mode":"ephemeral",
			"canonical_url":"https://mirror.invalid/small.git"
		}
	]`)

	remotes, err := workerCorePersistentCheckoutCacheRemoteURLs()
	if err != nil {
		t.Fatal(err)
	}

	want := []string{"https://mirror.invalid/large.git", "https://origin.invalid/large.git"}
	if len(remotes) != len(want) {
		t.Fatalf("remotes = %v, want %v", remotes, want)
	}

	for i := range want {
		if remotes[i] != want[i] {
			t.Fatalf("remotes = %v, want %v", remotes, want)
		}
	}

	structured, err := workerCorePersistentCheckoutCacheRemotes()
	if err != nil {
		t.Fatal(err)
	}

	wantStructured := []workercore.CheckoutCacheRemote{
		{
			RemoteURL:          "https://mirror.invalid/large.git",
			FallbackRemoteURLs: []string{"https://origin.invalid/large.git"},
		},
	}

	if len(structured) != len(wantStructured) ||
		structured[0].RemoteURL != wantStructured[0].RemoteURL ||
		len(structured[0].FallbackRemoteURLs) != 1 ||
		structured[0].FallbackRemoteURLs[0] != wantStructured[0].FallbackRemoteURLs[0] {
		t.Fatalf("structured remotes = %+v, want %+v", structured, wantStructured)
	}
}

func TestWorkerCorePersistentCheckoutCacheRemotesResolveCredentials(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("VECTIS_SOURCE_REPOSITORIES", "")
	t.Setenv("VECTIS_API_SERVER_SOURCE_REPOSITORIES", "")
	t.Setenv("VECTIS_WORKER_SOURCE_REPOSITORIES", "")
	t.Setenv("VECTIS_WORKER_CORE_SOURCE_REPOSITORIES", `[
		{
			"repository_id":"private",
			"checkout_mode":"managed",
			"worker_cache_mode":"persistent",
			"canonical_url":"https://mirror.invalid/private.git",
			"credential_ref":"secret://git/private"
		}
	]`)

	var resolvedRepositoryID string
	remotes, err := workerCorePersistentCheckoutCacheRemotesWithCredentialResolver(func(_ context.Context, rec dal.SourceRepositoryRecord) (sourcepkg.GitCredentials, error) {
		resolvedRepositoryID = rec.RepositoryID
		return sourcepkg.GitCredentials{Username: "oauth2", Password: "token"}, nil
	})

	if err != nil {
		t.Fatal(err)
	}

	if resolvedRepositoryID != "private" {
		t.Fatalf("resolved repository id = %q, want private", resolvedRepositoryID)
	}

	if len(remotes) != 1 ||
		remotes[0].RemoteURL != "https://mirror.invalid/private.git" ||
		remotes[0].Credentials.Username != "oauth2" ||
		remotes[0].Credentials.Password != "token" {
		t.Fatalf("remotes = %+v, want credentialed private remote", remotes)
	}
}

func TestWorkerCorePersistentCheckoutCacheRemotesRequireCredentialResolver(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("VECTIS_SOURCE_REPOSITORIES", "")
	t.Setenv("VECTIS_API_SERVER_SOURCE_REPOSITORIES", "")
	t.Setenv("VECTIS_WORKER_SOURCE_REPOSITORIES", "")
	t.Setenv("VECTIS_WORKER_CORE_SOURCE_REPOSITORIES", `[
		{
			"repository_id":"private",
			"checkout_mode":"managed",
			"worker_cache_mode":"persistent",
			"canonical_url":"https://mirror.invalid/private.git",
			"credential_ref":"secret://git/private"
		}
	]`)

	t.Setenv("VECTIS_SECRETS_ENCRYPTEDFS_ROOT", "")
	t.Setenv("VECTIS_SECRETS_ENCRYPTEDFS_KEY_FILE", "")
	t.Setenv("VECTIS_WORKER_CORE_ENCRYPTEDFS_ROOT", "")
	t.Setenv("VECTIS_WORKER_CORE_ENCRYPTEDFS_KEY_FILE", "")

	_, err := workerCorePersistentCheckoutCacheRemotes()
	if err == nil || !strings.Contains(err.Error(), "credential_ref") {
		t.Fatalf("workerCorePersistentCheckoutCacheRemotes error = %v, want missing credential resolver", err)
	}
}

func TestWorkerCoreCapabilitiesAdvertiseCheckoutCacheWarmWithRoot(t *testing.T) {
	withoutRoot := workercore.CoreDescription{Capabilities: workerCoreCapabilities("")}
	if workercore.HasCoreCapability(withoutRoot, workersdk.CapabilityCheckoutCacheWarm) {
		t.Fatal("checkout cache warm capability advertised without cache root")
	}

	withRoot := workercore.CoreDescription{Capabilities: workerCoreCapabilities(t.TempDir())}
	if !workercore.HasCoreCapability(withRoot, workersdk.CapabilityCheckoutCacheWarm) {
		t.Fatal("checkout cache warm capability missing with cache root")
	}
}

func TestWorkerCoreExecutorConfigUsesCheckoutCacheGenerationRetention(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("VECTIS_SOURCE_REPOSITORIES", "")
	t.Setenv("VECTIS_API_SERVER_SOURCE_REPOSITORIES", "")
	t.Setenv("VECTIS_WORKER_SOURCE_REPOSITORIES", "")
	t.Setenv("VECTIS_WORKER_CORE_SOURCE_REPOSITORIES", "")

	viper.Set("checkout_cache_root", "/tmp/vectis-cache")
	viper.Set("checkout_cache_generations_to_keep", 7)
	viper.Set("checkout_cache_lease_ttl", 45*time.Minute)
	viper.Set("checkout_cache_max_bytes", int64(512<<20))
	viper.Set("checkout_cache_warm_parallelism", 4)

	cfg, err := workerCoreExecutorConfig()
	if err != nil {
		t.Fatal(err)
	}

	if cfg.CheckoutCacheRoot != "/tmp/vectis-cache" {
		t.Fatalf("checkout cache root = %q", cfg.CheckoutCacheRoot)
	}

	if cfg.CheckoutCacheGenerationsToKeep != 7 {
		t.Fatalf("checkout cache generations to keep = %d, want 7", cfg.CheckoutCacheGenerationsToKeep)
	}

	if cfg.CheckoutCacheLeaseTTL != 45*time.Minute {
		t.Fatalf("checkout cache lease TTL = %v, want 45m", cfg.CheckoutCacheLeaseTTL)
	}

	if cfg.CheckoutCacheMaxBytes != 512<<20 {
		t.Fatalf("checkout cache max bytes = %d, want 512MiB", cfg.CheckoutCacheMaxBytes)
	}

	if cfg.CheckoutCacheWarmParallelism != 4 {
		t.Fatalf("checkout cache warm parallelism = %d, want 4", cfg.CheckoutCacheWarmParallelism)
	}
}

const workerCoreProcessHelperEnv = "VECTIS_WORKER_CORE_PROCESS_HELPER"

func TestWorkerCoreProcessSmoke(t *testing.T) {
	socketPath := socktest.ShortPath(t, "worker-core.sock")
	shellSocketPath := socktest.ShortPath(t, "worker-core-shell.sock")
	workspaceRoot := t.TempDir()
	metricsPort := reserveWorkerCoreProcessMetricsPort(t)

	var output bytes.Buffer
	cmd := exec.Command(os.Args[0], "-test.run=^TestWorkerCoreProcessHelper$")
	cmd.Env = append(os.Environ(),
		workerCoreProcessHelperEnv+"=1",
		"VECTIS_WORKER_CORE_PROCESS_SOCKET="+socketPath,
		"VECTIS_WORKER_CORE_PROCESS_WORKSPACE_ROOT="+workspaceRoot,
		"VECTIS_WORKER_CORE_PROCESS_METRICS_PORT="+strconv.Itoa(metricsPort),
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
	scriptUses := "builtins/script"
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
					Uses: &scriptUses,
					With: map[string]string{
						"runner": "sh",
						"script": "printf smoke-log && printf smoke-artifact > artifact.txt",
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
	metricsPort := os.Getenv("VECTIS_WORKER_CORE_PROCESS_METRICS_PORT")
	if socketPath == "" || workspaceRoot == "" || metricsPort == "" {
		t.Fatal("worker core helper missing socket, workspace root, or metrics port")
	}

	os.Args = []string{
		"vectis-worker-core",
		"--socket", socketPath,
		"--metrics-host", "127.0.0.1",
		"--metrics-port", metricsPort,
		"--execution-backend", "host",
		"--workspace-root", workspaceRoot,
	}

	main()
}

func reserveWorkerCoreProcessMetricsPort(t *testing.T) int {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve worker-core metrics port: %v", err)
	}
	defer ln.Close()

	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok || addr.Port <= 0 {
		t.Fatalf("reserve worker-core metrics port returned %v", ln.Addr())
	}

	return addr.Port
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
