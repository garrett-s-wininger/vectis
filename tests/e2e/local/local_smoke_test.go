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
	8080, 8081, 8082, 8083, 8085, 8086, 8087,
	9081, 9082, 9083, 9084, 9085, 9086, 9087, 9089, 9090,
}

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

	env := commandEnv(map[string]string{
		"XDG_DATA_HOME":                   t.TempDir(),
		"XDG_RUNTIME_DIR":                 shortTempDir(t, "vectis-e2e-runtime-*"),
		"VECTIS_API_TOKEN":                "",
		"VECTIS_LOCAL_GRPC_INSECURE":      "true",
		"VECTIS_LOCAL_HTTP_TLS":           "off",
		"VECTIS_LOCAL_DOCS_ENABLED":       "false",
		database.EnvDatabaseDSN:           "",
		database.EnvGlobalDatabaseDSN:     "",
		database.EnvCellDatabaseDSN:       "",
		"VECTIS_WORKER_EXECUTION_BACKEND": "host",
	})

	proc := startVectisLocal(t, root, env, local)
	if !truthyEnv("VECTIS_E2E_KEEP_LOCAL") {
		t.Cleanup(func() { stopVectisLocal(t, proc) })
	}

	waitForAPIReady(t, proc, "http://localhost:8080/health/ready", 2*time.Minute)

	jobID := "local-e2e-smoke-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	createStoredSmokeJob(t, root, env, cli, jobID)
	run := triggerStoredSmokeJob(t, root, env, cli, jobID)
	statuses, final := waitForRunTerminal(t, root, env, cli, run.RunID, 2*time.Minute)

	if final.Status != "succeeded" {
		t.Fatalf("run %s finished with status %q; observed statuses=%v\nvectis-local stderr:\n%s", run.RunID, final.Status, statuses, proc.stderr.String())
	}

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
	assertRunLogsContain(t, root, env, cli, run.RunID, "local-smoke-start", "local-smoke-finish")
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

func startVectisLocal(t *testing.T, root string, env []string, local string) *localProcess {
	t.Helper()

	cmd := exec.Command(local, "--grpc-insecure", "--http-tls", "off", "--docs=false") // #nosec G204 -- e2e harness controls the binary path.
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

func createStoredSmokeJob(t *testing.T, root string, env []string, cli, jobID string) {
	t.Helper()

	body := smokeJobDefinition(jobID)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := runCommand(ctx, root, env, strings.NewReader(body), cli, "--format", "json", "jobs", "create", "-")
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

func waitForRunTerminal(t *testing.T, root string, env []string, cli, runID string, timeout time.Duration) ([]string, runShowResult) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	statuses := make([]string, 0, 4)
	var last runShowResult
	var lastErr error
	for time.Now().Before(deadline) {
		showCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		result, err := runCommand(showCtx, root, env, nil, cli, "--format", "json", "runs", "show", runID)
		cancel()

		if err == nil {
			var out runShowResult
			if err := json.Unmarshal([]byte(result.stdout), &out); err != nil {
				lastErr = fmt.Errorf("parse runs show output: %w", err)
			} else {
				last = out
				status := strings.TrimSpace(strings.ToLower(out.Status))
				if status != "" && (len(statuses) == 0 || statuses[len(statuses)-1] != status) {
					statuses = append(statuses, status)
				}

				if runStatusTerminal(status) {
					return statuses, out
				}
			}
		} else {
			lastErr = err
		}

		time.Sleep(200 * time.Millisecond)
	}

	t.Fatalf("run %s did not reach a terminal state within %s; statuses=%v last=%+v err=%v", runID, timeout, statuses, last, lastErr)
	return nil, runShowResult{}
}

func assertRunTasksSucceeded(t *testing.T, root string, env []string, cli, runID string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := runCommand(ctx, root, env, nil, cli, "--format", "json", "runs", "tasks", runID)
	if err != nil {
		t.Fatalf("list run tasks: %v", err)
	}

	var out runTasksResult
	if err := json.Unmarshal([]byte(result.stdout), &out); err != nil {
		t.Fatalf("parse runs tasks output: %v\nstdout:\n%s\nstderr:\n%s", err, result.stdout, result.stderr)
	}

	if len(out.Data) == 0 {
		t.Fatalf("run %s returned no tasks", runID)
	}

	for _, task := range out.Data {
		if task.Status != "succeeded" {
			t.Fatalf("task %s status = %q, want succeeded", task.TaskID, task.Status)
		}

		if len(task.Attempts) == 0 {
			t.Fatalf("task %s returned no attempts", task.TaskID)
		}

		for _, attempt := range task.Attempts {
			if attempt.Status != "succeeded" {
				t.Fatalf("task %s attempt %s status = %q, want succeeded", task.TaskID, attempt.AttemptID, attempt.Status)
			}

			if attempt.ExecutionStatus != "" && attempt.ExecutionStatus != "succeeded" {
				t.Fatalf("task %s attempt %s execution_status = %q, want succeeded", task.TaskID, attempt.AttemptID, attempt.ExecutionStatus)
			}
		}
	}
}

func assertRunLogsContain(t *testing.T, root string, env []string, cli, runID string, needles ...string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := runCommand(ctx, root, env, nil, cli, "logs", "run", runID)
	if err != nil && !strings.Contains(err.Error(), " timed out:") {
		t.Fatalf("stream run logs: %v", err)
	}

	for _, needle := range needles {
		if !strings.Contains(result.stdout, needle) {
			t.Fatalf("logs for run %s missing %q\nstdout:\n%s\nstderr:\n%s", runID, needle, result.stdout, result.stderr)
		}
	}
}

func smokeJobDefinition(jobID string) string {
	return fmt.Sprintf(`{
  "id": %q,
  "root": {
    "id": "root",
    "uses": "builtins/shell",
    "with": {
      "command": "echo local-smoke-start; sleep 2; echo local-smoke-finish"
    }
  }
}
`, jobID)
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
