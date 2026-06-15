//go:build e2e

package local_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"vectis/internal/database"
)

var localSmokePorts = []int{
	8080, 8081, 8082, 8083, 8085, 8086, 8087, 8090,
	9081, 9082, 9083, 9084, 9085, 9086, 9087, 9089, 9090, 9091,
}

const (
	canonicalJobID            = "e2e-canonical-smoke"
	smokeSecretRef            = "encryptedfs://team/smoke-token"
	smokeSecretPlaintext      = "spiffe-secret"
	smokeArtifactName         = "e2e-smoke-report"
	smokeArtifactPath         = "reports/e2e-smoke.txt"
	smokeArtifactContent      = "canonical-artifact-ok\n"
	smokeRetryArtifactName    = "e2e-registry-retry"
	smokeRetryArtifactPath    = "reports/registry-retry.txt"
	smokeRetryArtifactContent = "canonical-registry-retry-attempt=2\ncanonical-registry-retry-succeeded\n"
)

type commandResult struct {
	stdout string
	stderr string
}

type jobCreateResult struct {
	Status string `json:"status"`
	JobID  string `json:"job_id"`
}

type jobRunResult struct {
	RunID    string `json:"run_id"`
	RunIndex int    `json:"run_index"`
}

type runShowResult struct {
	RunID          string          `json:"run_id"`
	Status         string          `json:"status"`
	TaskCompletion *taskCompletion `json:"task_completion,omitempty"`
}

type taskCompletion struct {
	Total          int `json:"total"`
	Succeeded      int `json:"succeeded"`
	TerminalFailed int `json:"terminal_failed"`
	Incomplete     int `json:"incomplete"`
}

type runTasksResult struct {
	Data []runTaskRow `json:"data"`
}

type runArtifactsResult struct {
	Data []runArtifactRow `json:"data"`
}

type runArtifactRow struct {
	Name            string          `json:"name"`
	Path            string          `json:"path"`
	ContentType     string          `json:"content_type,omitempty"`
	BlobDigest      string          `json:"blob_digest"`
	SizeBytes       int64           `json:"size_bytes"`
	ArtifactShardID string          `json:"artifact_shard_id"`
	Metadata        json.RawMessage `json:"metadata,omitempty"`
}

type secretEncryptedFSPutResult struct {
	Status string `json:"status"`
	Ref    string `json:"ref"`
	Bytes  int    `json:"bytes"`
}

type runTaskRow struct {
	TaskID   string              `json:"task_id"`
	Status   string              `json:"status"`
	Attempts []runTaskAttemptRow `json:"attempts"`
}

type runTaskAttemptRow struct {
	AttemptID       string `json:"attempt_id"`
	Status          string `json:"status"`
	ExecutionStatus string `json:"execution_status,omitempty"`
}

type localProcess struct {
	cmd    *exec.Cmd
	stdout *lockedBuffer
	stderr *lockedBuffer
	done   chan struct{}

	mu  sync.Mutex
	err error
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func TestE2ELocalTriggerSmoke(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("vectis-local e2e process cleanup uses Unix signals")
	}

	root := repoRoot(t)
	cli := e2eBinaryPath(t, root, "VECTIS_E2E_CLI", "vectis-cli")
	local := e2eBinaryPath(t, root, "VECTIS_E2E_LOCAL", "vectis-local")

	requireExecutable(t, cli, "vectis-cli")
	requireExecutable(t, local, "vectis-local")
	requireLocalPortsAvailable(t, localSmokePorts)

	dataHome := t.TempDir()
	env := commandEnv(map[string]string{
		"PATH":            filepath.Join(root, "bin") + string(os.PathListSeparator) + os.Getenv("PATH"),
		"XDG_DATA_HOME":   dataHome,
		"XDG_RUNTIME_DIR": shortTempDir(t, "vectis-e2e-runtime-*"),
		"VECTIS_ACTION_REGISTRY_ALLOWED_NAMESPACES":  "",
		"VECTIS_ACTION_REGISTRY_ALLOWED_SOURCES":     "",
		"VECTIS_ACTION_REGISTRY_LOCAL_ROOTS":         filepath.Join(root, "examples", "actions"),
		"VECTIS_ACTION_REGISTRY_REQUIRE_DIGEST_PINS": "false",
		"VECTIS_API_SERVER_HOST":                     "localhost",
		"VECTIS_API_SERVER_PORT":                     "8080",
		"VECTIS_API_TOKEN":                           "",
		"VECTIS_GRPC_TLS_INSECURE":                   "false",
		"VECTIS_LOCAL_GRPC_INSECURE":                 "false",
		"VECTIS_LOCAL_HTTP_TLS":                      "off",
		"VECTIS_LOCAL_DOCS_ENABLED":                  "false",
		database.EnvDatabaseDSN:                      "",
		database.EnvGlobalDatabaseDSN:                "",
		database.EnvCellDatabaseDSN:                  "",
		"VECTIS_WORKER_EXECUTION_BACKEND":            "host",
	})

	proc := startVectisLocal(t, root, env, local)
	if !truthyEnv("VECTIS_E2E_KEEP_LOCAL") {
		t.Cleanup(func() { stopVectisLocal(t, proc) })
	}

	waitForAPIReady(t, proc, "http://localhost:8080/health/ready", 2*time.Minute)

	seedLocalSmokeSecret(t, root, env, cli, dataHome)
	createStoredExampleJob(t, root, env, cli, filepath.Join(root, "examples", "e2e-canonical.json"), canonicalJobID)
	run := triggerStoredSmokeJob(t, root, env, cli, canonicalJobID)
	statuses, final := waitForRunTerminal(t, root, env, cli, run.RunID, 3*time.Minute)

	if final.Status != "succeeded" {
		t.Fatalf("run %s finished with status %q; observed statuses=%v\nvectis-local stderr:\n%s", run.RunID, final.Status, statuses, proc.stderr.String())
	}

	final = waitForRunTaskCompletionSucceeded(t, root, env, cli, run.RunID, 30*time.Second)

	if !containsStatus(statuses, "running") {
		t.Fatalf("run %s never reported running; observed statuses=%v", run.RunID, statuses)
	}

	if !containsStatus(statuses, "succeeded") {
		t.Fatalf("run %s never reported succeeded; observed statuses=%v", run.RunID, statuses)
	}

	if final.TaskCompletion == nil {
		t.Fatalf("run %s final detail missing task_completion", run.RunID)
	}

	if final.TaskCompletion.Total == 0 || final.TaskCompletion.Succeeded != final.TaskCompletion.Total ||
		final.TaskCompletion.TerminalFailed != 0 || final.TaskCompletion.Incomplete != 0 {
		t.Fatalf("run %s task completion = %+v, want all succeeded", run.RunID, *final.TaskCompletion)
	}

	assertRunTasksSucceeded(t, root, env, cli, run.RunID)
	assertRunLogsContain(t, root, env, cli, run.RunID,
		"Starting task execution: e2e-canonical-smoke task ",
		"Run "+run.RunID+" finished successfully.",
	)
	assertRunArtifact(t, root, env, cli, run.RunID, smokeArtifactName, smokeArtifactPath, smokeArtifactContent)
	assertRunArtifact(t, root, env, cli, run.RunID, smokeRetryArtifactName, smokeRetryArtifactPath, smokeRetryArtifactContent)
}

func TestE2ELocalConfigAsCodeTriggerSmoke(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("vectis-local e2e process cleanup uses Unix signals")
	}

	root := repoRoot(t)
	cli := e2eBinaryPath(t, root, "VECTIS_E2E_CLI", "vectis-cli")
	local := e2eBinaryPath(t, root, "VECTIS_E2E_LOCAL", "vectis-local")

	requireExecutable(t, cli, "vectis-cli")
	requireExecutable(t, local, "vectis-local")
	requireGit(t)
	requireLocalPortsAvailable(t, localSmokePorts)

	sourceRepo := createConfigAsCodeSmokeRepo(t, root)
	dataHome := t.TempDir()
	env := commandEnv(map[string]string{
		"XDG_DATA_HOME":   dataHome,
		"XDG_RUNTIME_DIR": shortTempDir(t, "vectis-e2e-runtime-*"),
		"VECTIS_ACTION_REGISTRY_ALLOWED_NAMESPACES":             "",
		"VECTIS_ACTION_REGISTRY_ALLOWED_SOURCES":                "",
		"VECTIS_ACTION_REGISTRY_LOCAL_ROOTS":                    filepath.Join(root, "examples", "actions"),
		"VECTIS_ACTION_REGISTRY_REQUIRE_DIGEST_PINS":            "false",
		"VECTIS_API_SERVER_HOST":                                "localhost",
		"VECTIS_API_SERVER_PORT":                                "8080",
		"VECTIS_API_TOKEN":                                      "",
		"VECTIS_GRPC_TLS_INSECURE":                              "false",
		"VECTIS_LOCAL_GRPC_INSECURE":                            "false",
		"VECTIS_LOCAL_HTTP_TLS":                                 "off",
		"VECTIS_LOCAL_DOCS_ENABLED":                             "false",
		"VECTIS_LOCAL_CONFIG_AS_CODE":                           "",
		"VECTIS_LOCAL_SOURCE_REPOSITORIES":                      "",
		"VECTIS_SOURCE_REPOSITORIES":                            "",
		"VECTIS_SOURCE_STORED_JOBS_ENABLED":                     "",
		"VECTIS_SOURCE_SYNC_CONFIGURED_REPOSITORIES_ON_STARTUP": "",
		database.EnvDatabaseDSN:                                 "",
		database.EnvGlobalDatabaseDSN:                           "",
		database.EnvCellDatabaseDSN:                             "",
		"VECTIS_WORKER_EXECUTION_BACKEND":                       "host",
	})

	proc := startVectisLocal(t, root, env, local, "--config-as-code", "--source-repository", "vectis-local="+sourceRepo)
	if !truthyEnv("VECTIS_E2E_KEEP_LOCAL") {
		t.Cleanup(func() { stopVectisLocal(t, proc) })
	}

	waitForAPIReady(t, proc, "http://localhost:8080/health/ready", 2*time.Minute)

	seedLocalSmokeSecret(t, root, env, cli, dataHome)
	run := triggerSourceSmokeJob(t, root, env, cli, "vectis-local", canonicalJobID)
	statuses, final := waitForRunTerminal(t, root, env, cli, run.RunID, 3*time.Minute)

	if final.Status != "succeeded" {
		t.Fatalf("source run %s finished with status %q; observed statuses=%v\nvectis-local stderr:\n%s", run.RunID, final.Status, statuses, proc.stderr.String())
	}

	final = waitForRunTaskCompletionSucceeded(t, root, env, cli, run.RunID, 30*time.Second)

	if !containsStatus(statuses, "running") {
		t.Fatalf("source run %s never reported running; observed statuses=%v", run.RunID, statuses)
	}

	if !containsStatus(statuses, "succeeded") {
		t.Fatalf("source run %s never reported succeeded; observed statuses=%v", run.RunID, statuses)
	}

	if final.TaskCompletion == nil {
		t.Fatalf("source run %s final detail missing task_completion", run.RunID)
	}

	if final.TaskCompletion.Total == 0 || final.TaskCompletion.Succeeded != final.TaskCompletion.Total ||
		final.TaskCompletion.TerminalFailed != 0 || final.TaskCompletion.Incomplete != 0 {
		t.Fatalf("source run %s task completion = %+v, want all succeeded", run.RunID, *final.TaskCompletion)
	}

	assertRunTasksSucceeded(t, root, env, cli, run.RunID)
	assertRunLogsContain(t, root, env, cli, run.RunID,
		"Starting task execution: e2e-canonical-smoke task ",
		"Run "+run.RunID+" finished successfully.",
	)

	assertRunArtifact(t, root, env, cli, run.RunID, smokeArtifactName, smokeArtifactPath, smokeArtifactContent)
	assertRunArtifact(t, root, env, cli, run.RunID, smokeRetryArtifactName, smokeRetryArtifactPath, smokeRetryArtifactContent)
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func repoRoot(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find repo root containing go.mod")
		}

		dir = parent
	}
}

func e2eBinaryPath(t *testing.T, root, envKey, binary string) string {
	t.Helper()

	if path := strings.TrimSpace(os.Getenv(envKey)); path != "" {
		if filepath.IsAbs(path) {
			return path
		}

		return filepath.Join(root, path)
	}

	return filepath.Join(root, "bin", binary)
}

func shortTempDir(t *testing.T, pattern string) string {
	t.Helper()

	dir, err := os.MkdirTemp("/private/tmp", pattern)
	if err != nil {
		t.Fatalf("create short temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func requireExecutable(t *testing.T, path, label string) {
	t.Helper()

	info, err := os.Stat(path)
	if err != nil {
		skipOrFatal(t, "%s binary %s is not available; run make build or set its VECTIS_E2E_* override", label, path)
	}

	if info.IsDir() {
		skipOrFatal(t, "%s path %s is a directory", label, path)
	}

	if runtime.GOOS != "windows" && info.Mode()&0o111 == 0 {
		skipOrFatal(t, "%s binary %s is not executable", label, path)
	}
}

func requireLocalPortsAvailable(t *testing.T, ports []int) {
	t.Helper()

	var busy []string
	for _, port := range ports {
		ln, err := net.Listen("tcp", ":"+strconv.Itoa(port))
		if err != nil {
			busy = append(busy, strconv.Itoa(port))
			continue
		}

		_ = ln.Close()
	}

	if len(busy) > 0 {
		skipOrFatal(t, "vectis-local smoke ports already in use: %s; stop the local stack before running this e2e test", strings.Join(busy, ", "))
	}
}

func requireGit(t *testing.T) {
	t.Helper()

	if _, err := exec.LookPath("git"); err != nil {
		skipOrFatal(t, "git binary is not available; config-as-code e2e smoke requires git")
	}
}

func createConfigAsCodeSmokeRepo(t *testing.T, root string) string {
	t.Helper()

	repo := filepath.Join(t.TempDir(), "source-repo")
	definitionPath := filepath.Join(repo, ".vectis", "jobs", canonicalJobID+".json")
	if err := os.MkdirAll(filepath.Dir(definitionPath), 0o755); err != nil {
		t.Fatalf("create source definition dir: %v", err)
	}

	definition, err := os.ReadFile(filepath.Join(root, "examples", "e2e-canonical.json"))
	if err != nil {
		t.Fatalf("read canonical example definition: %v", err)
	}

	if err := os.WriteFile(definitionPath, definition, 0o644); err != nil {
		t.Fatalf("write source definition: %v", err)
	}

	git := func(args ...string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := runCommand(ctx, repo, nil, nil, "git", args...); err != nil {
			t.Fatalf("git %s: %v", strings.Join(args, " "), err)
		}
	}

	git("init")
	git("config", "user.email", "vectis-e2e@example.invalid")
	git("config", "user.name", "Vectis E2E")
	git("add", ".vectis/jobs/"+canonicalJobID+".json")
	git("commit", "-m", "Add config-as-code smoke job")

	return repo
}

func startVectisLocal(t *testing.T, root string, env []string, local string, extraArgs ...string) *localProcess {
	t.Helper()

	args := append([]string{"--http-tls", "off", "--docs=false"}, extraArgs...)
	cmd := exec.Command(local, args...) // #nosec G204 -- e2e harness controls the binary path.
	cmd.Dir = root
	cmd.Env = env

	proc := &localProcess{
		cmd:    cmd,
		stdout: &lockedBuffer{},
		stderr: &lockedBuffer{},
		done:   make(chan struct{}),
	}
	cmd.Stdout = proc.stdout
	cmd.Stderr = proc.stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("start vectis-local: %v", err)
	}

	go func() {
		err := cmd.Wait()
		proc.mu.Lock()
		proc.err = err
		proc.mu.Unlock()
		close(proc.done)
	}()

	return proc
}

func stopVectisLocal(t *testing.T, proc *localProcess) {
	t.Helper()

	select {
	case <-proc.done:
		return
	default:
	}

	if proc.cmd.Process != nil {
		_ = proc.cmd.Process.Signal(syscall.SIGTERM)
	}

	select {
	case <-proc.done:
	case <-time.After(20 * time.Second):
		if proc.cmd.Process != nil {
			_ = proc.cmd.Process.Kill()
		}
		select {
		case <-proc.done:
		case <-time.After(5 * time.Second):
			t.Fatalf("vectis-local did not exit after SIGKILL\nstdout:\n%s\nstderr:\n%s", proc.stdout.String(), proc.stderr.String())
		}
	}
}

func waitForAPIReady(t *testing.T, proc *localProcess, url string, timeout time.Duration) {
	t.Helper()

	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		select {
		case <-proc.done:
			t.Fatalf("vectis-local exited before API became ready: %v\nstdout:\n%s\nstderr:\n%s", proc.exitErr(), proc.stdout.String(), proc.stderr.String())
		default:
		}

		resp, err := client.Get(url)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}

			lastErr = fmt.Errorf("status %s", resp.Status)
		} else {
			lastErr = err
		}

		time.Sleep(500 * time.Millisecond)
	}

	t.Fatalf("API did not become ready at %s within %s: %v\nstdout:\n%s\nstderr:\n%s", url, timeout, lastErr, proc.stdout.String(), proc.stderr.String())
}

func (p *localProcess) exitErr() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

func seedLocalSmokeSecret(t *testing.T, root string, env []string, cli, dataHome string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := runCommand(ctx, root, env, strings.NewReader(smokeSecretPlaintext),
		cli, "--format", "json", "secrets", "encryptedfs", "put", smokeSecretRef,
		"--root", localManagedSecretsDir(dataHome, "local"),
		"--key-file", localManagedSecretsKeyFile(dataHome, "local"),
		"--force")
	if err != nil {
		t.Fatalf("seed local smoke secret: %v", err)
	}

	var out secretEncryptedFSPutResult
	if err := json.Unmarshal([]byte(result.stdout), &out); err != nil {
		t.Fatalf("parse secrets encryptedfs put output: %v\nstdout:\n%s\nstderr:\n%s", err, result.stdout, result.stderr)
	}

	if out.Status != "ok" || out.Ref != smokeSecretRef || out.Bytes != len(smokeSecretPlaintext) {
		t.Fatalf("unexpected secrets encryptedfs put output: %+v", out)
	}
}

func localManagedSecretsDir(dataHome, cellID string) string {
	return filepath.Join(dataHome, "vectis", "cells", safePathPart(cellID), "secrets")
}

func localManagedSecretsKeyFile(dataHome, cellID string) string {
	return filepath.Join(dataHome, "vectis", "cells", safePathPart(cellID), "secrets.key")
}

func safePathPart(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "local"
	}

	replacer := strings.NewReplacer("/", "_", "\\", "_", ":", "_")
	return replacer.Replace(s)
}

func createStoredExampleJob(t *testing.T, root string, env []string, cli, path, jobID string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := runCommand(ctx, root, env, nil, cli, "--format", "json", "jobs", "create", path)
	if err != nil {
		t.Fatalf("create smoke job: %v", err)
	}

	var out jobCreateResult
	if err := json.Unmarshal([]byte(result.stdout), &out); err != nil {
		t.Fatalf("parse jobs create output: %v\nstdout:\n%s\nstderr:\n%s", err, result.stdout, result.stderr)
	}

	if out.Status != "created" || out.JobID != jobID {
		t.Fatalf("unexpected jobs create output: %+v", out)
	}
}

func triggerStoredSmokeJob(t *testing.T, root string, env []string, cli, jobID string) jobRunResult {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := runCommand(ctx, root, env, nil, cli, "--format", "json", "jobs", "trigger", jobID)
	if err != nil {
		t.Fatalf("trigger smoke job: %v", err)
	}

	var out jobRunResult
	if err := json.Unmarshal([]byte(result.stdout), &out); err != nil {
		t.Fatalf("parse jobs trigger output: %v\nstdout:\n%s\nstderr:\n%s", err, result.stdout, result.stderr)
	}

	if strings.TrimSpace(out.RunID) == "" {
		t.Fatalf("jobs trigger output missing run_id: %s", result.stdout)
	}

	return out
}

func triggerSourceSmokeJob(t *testing.T, root string, env []string, cli, repositoryID, jobID string) jobRunResult {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := runCommand(ctx, root, env, nil, cli, "--format", "json", "sources", "trigger", repositoryID, jobID)
	if err != nil {
		t.Fatalf("trigger source smoke job: %v", err)
	}

	var out jobRunResult
	if err := json.Unmarshal([]byte(result.stdout), &out); err != nil {
		t.Fatalf("parse sources trigger output: %v\nstdout:\n%s\nstderr:\n%s", err, result.stdout, result.stderr)
	}

	if strings.TrimSpace(out.RunID) == "" {
		t.Fatalf("sources trigger output missing run_id: %s", result.stdout)
	}

	return out
}

func waitForRunTerminal(t *testing.T, root string, env []string, cli, runID string, timeout time.Duration) ([]string, runShowResult) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	statuses := make([]string, 0, 4)
	var last runShowResult
	var lastErr error
	for time.Now().Before(deadline) {
		out, err := showRun(t, root, env, cli, runID)
		if err == nil {
			last = out
			status := strings.TrimSpace(strings.ToLower(out.Status))
			if status != "" && (len(statuses) == 0 || statuses[len(statuses)-1] != status) {
				statuses = append(statuses, status)
			}

			if runStatusTerminal(status) {
				return statuses, out
			}
		} else {
			lastErr = err
		}

		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("run %s did not reach a terminal state within %s; statuses=%v last=%+v err=%v", runID, timeout, statuses, last, lastErr)
	return nil, runShowResult{}
}

func waitForRunTaskCompletionSucceeded(t *testing.T, root string, env []string, cli, runID string, timeout time.Duration) runShowResult {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var last runShowResult
	var lastErr error
	for time.Now().Before(deadline) {
		out, err := showRun(t, root, env, cli, runID)
		if err == nil {
			last = out
			if taskCompletionSucceeded(out.TaskCompletion) {
				return out
			}
		} else {
			lastErr = err
		}

		time.Sleep(200 * time.Millisecond)
	}

	if last.TaskCompletion != nil {
		t.Fatalf("run %s task completion did not converge within %s; status=%s completion=%+v err=%v", runID, timeout, last.Status, *last.TaskCompletion, lastErr)
	}

	t.Fatalf("run %s task completion did not converge within %s; last=%+v err=%v", runID, timeout, last, lastErr)
	return runShowResult{}
}

func showRun(t *testing.T, root string, env []string, cli, runID string) (runShowResult, error) {
	t.Helper()

	showCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := runCommand(showCtx, root, env, nil, cli, "--format", "json", "runs", "show", runID)
	if err != nil {
		return runShowResult{}, err
	}

	var out runShowResult
	if err := json.Unmarshal([]byte(result.stdout), &out); err != nil {
		return runShowResult{}, fmt.Errorf("parse runs show output: %w", err)
	}

	return out, nil
}

func taskCompletionSucceeded(completion *taskCompletion) bool {
	return completion != nil &&
		completion.Total > 0 &&
		completion.Succeeded == completion.Total &&
		completion.TerminalFailed == 0 &&
		completion.Incomplete == 0
}

func assertRunTasksSucceeded(t *testing.T, root string, env []string, cli, runID string) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	var last runTasksResult
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		result, err := runCommand(ctx, root, env, nil, cli, "--format", "json", "runs", "tasks", runID)
		cancel()

		if err != nil {
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		}

		var out runTasksResult
		if err := json.Unmarshal([]byte(result.stdout), &out); err != nil {
			lastErr = fmt.Errorf("parse runs tasks output: %w", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}

		last = out
		if runTasksSucceeded(out) {
			return
		}

		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("run %s tasks did not converge to succeeded within 30s; last=%+v err=%v", runID, last, lastErr)
}

func runTasksSucceeded(out runTasksResult) bool {
	if len(out.Data) == 0 {
		return false
	}

	for _, task := range out.Data {
		if task.Status != "succeeded" {
			return false
		}

		if len(task.Attempts) == 0 {
			return false
		}

		for _, attempt := range task.Attempts {
			if attempt.Status != "succeeded" {
				return false
			}

			if attempt.ExecutionStatus != "" && attempt.ExecutionStatus != "succeeded" {
				return false
			}
		}
	}

	return true
}

func assertRunLogsContain(t *testing.T, root string, env []string, cli, runID string, needles ...string) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	var last commandResult
	var lastErr error
	var missing []string
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		result, err := runCommand(ctx, root, env, nil, cli, "logs", "run", runID)
		cancel()

		last = result
		if err != nil && !strings.Contains(err.Error(), " timed out:") {
			lastErr = err
			time.Sleep(500 * time.Millisecond)
			continue
		}

		missing = missingLogNeedles(result.stdout, needles)
		if len(missing) == 0 {
			return
		}

		time.Sleep(500 * time.Millisecond)
	}

	t.Fatalf("logs for run %s missing %v err=%v\nstdout:\n%s\nstderr:\n%s", runID, missing, lastErr, last.stdout, last.stderr)
}

func missingLogNeedles(logs string, needles []string) []string {
	missing := make([]string, 0)
	for _, needle := range needles {
		if !strings.Contains(logs, needle) {
			missing = append(missing, needle)
		}
	}

	return missing
}

func assertRunArtifact(t *testing.T, root string, env []string, cli, runID, artifactName, artifactPath, artifactContent string) {
	t.Helper()

	artifact := waitForRunArtifact(t, root, env, cli, runID, artifactName, 30*time.Second)

	if artifact == nil {
		t.Fatalf("run %s artifacts missing %q", runID, artifactName)
	}

	if artifact.Path != artifactPath || artifact.ContentType != "text/plain" ||
		artifact.SizeBytes != int64(len(artifactContent)) ||
		strings.TrimSpace(artifact.BlobDigest) == "" ||
		strings.TrimSpace(artifact.ArtifactShardID) == "" {
		t.Fatalf("unexpected artifact manifest: %+v", *artifact)
	}

	downloadPath := filepath.Join(t.TempDir(), filepath.Base(artifactPath))
	downloadCtx, downloadCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer downloadCancel()

	if _, err := runCommand(downloadCtx, root, env, nil, cli, "--format", "json", "runs", "artifacts", "download", runID, artifactName, "--output", downloadPath); err != nil {
		t.Fatalf("download run artifact: %v", err)
	}

	data, err := os.ReadFile(downloadPath)
	if err != nil {
		t.Fatalf("read downloaded artifact: %v", err)
	}

	if string(data) != artifactContent {
		t.Fatalf("downloaded artifact %s content = %q, want %q", artifactName, string(data), artifactContent)
	}
}

func waitForRunArtifact(t *testing.T, root string, env []string, cli, runID, artifactName string, timeout time.Duration) *runArtifactRow {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var last runArtifactsResult
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		result, err := runCommand(ctx, root, env, nil, cli, "--format", "json", "runs", "artifacts", "list", runID)
		cancel()

		if err != nil {
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		}

		var out runArtifactsResult
		if err := json.Unmarshal([]byte(result.stdout), &out); err != nil {
			lastErr = fmt.Errorf("parse runs artifacts list output: %w", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}

		last = out
		for i := range out.Data {
			if out.Data[i].Name == artifactName {
				artifact := out.Data[i]
				return &artifact
			}
		}

		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("run %s artifact %q did not appear within %s; last=%+v err=%v", runID, artifactName, timeout, last, lastErr)
	return nil
}

func runStatusTerminal(status string) bool {
	switch strings.TrimSpace(strings.ToLower(status)) {
	case "succeeded", "failed", "orphaned", "cancelled", "abandoned", "aborted":
		return true
	default:
		return false
	}
}

func containsStatus(statuses []string, want string) bool {
	for _, status := range statuses {
		if status == want {
			return true
		}
	}
	return false
}

func runCommand(ctx context.Context, dir string, env []string, stdin io.Reader, name string, args ...string) (commandResult, error) {
	cmd := exec.CommandContext(ctx, name, args...) // #nosec G204 -- e2e harness controls command names/args.
	if dir != "" {
		cmd.Dir = dir
	}

	if env != nil {
		cmd.Env = env
	}

	if stdin != nil {
		cmd.Stdin = stdin
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	result := commandResult{stdout: stdout.String(), stderr: stderr.String()}
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return result, fmt.Errorf("%s %s timed out: stdout=%q stderr=%q", name, strings.Join(args, " "), result.stdout, result.stderr)
		}

		return result, fmt.Errorf("%s %s failed: %w\nstdout:\n%s\nstderr:\n%s", name, strings.Join(args, " "), err, result.stdout, result.stderr)
	}

	return result, nil
}

func commandEnv(overrides map[string]string) []string {
	out := make([]string, 0, len(os.Environ())+len(overrides))
	for _, entry := range os.Environ() {
		key, _, ok := strings.Cut(entry, "=")
		if ok {
			if _, overridden := overrides[key]; overridden {
				continue
			}
		}

		out = append(out, entry)
	}

	for key, value := range overrides {
		out = append(out, key+"="+value)
	}

	return out
}

func skipOrFatal(t *testing.T, format string, args ...any) {
	t.Helper()

	msg := fmt.Sprintf(format, args...)
	if truthyEnv("VECTIS_E2E_REQUIRE") {
		t.Fatal(msg)
	}

	t.Skip(msg)
}

func truthyEnv(key string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}
