//go:build e2e

package podman_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

const (
	smokeSecretRef       = "encryptedfs://team/smoke-token"
	smokeSecretPlaintext = "spiffe-secret"
	smokeSecretRoot      = "/data/vectis/secrets/encryptedfs"
	smokeSecretKeyFile   = "/secrets/encryptedfs.key"
)

var requiredPodmanImages = []string{
	"vectis-api:latest",
	"vectis-artifact:latest",
	"vectis-catalog:latest",
	"vectis-cli:latest",
	"vectis-cron:latest",
	"vectis-docs:latest",
	"vectis-log:latest",
	"vectis-orchestrator:latest",
	"vectis-queue:latest",
	"vectis-reconciler:latest",
	"vectis-registry:latest",
	"vectis-secrets:latest",
	"vectis-spiffe:latest",
	"vectis-worker:latest",
	"vectis-worker-core:latest",
	"docker.io/library/alpine:3.21",
	"docker.io/library/postgres:18-alpine",
	"docker.io/prom/prometheus:v3.11.0-distroless",
	"docker.io/opensearchproject/opensearch:2.19.1",
	"docker.io/fluent/fluent-bit:5.0.4",
	"cr.jaegertracing.io/jaegertracing/jaeger:2.17.0",
	"docker.io/opensearchproject/opensearch-dashboards:2.19.1",
	"docker.io/grafana/grafana:13.0.0-23943897787",
}

var podmanResourceNames = []string{
	"vectis-postgres-data",
	"vectis-queue-data",
	"vectis-artifact-data",
	"vectis-log-data",
	"vectis-secrets-data",
	"vectis-spiffe-data",
	"vectis-podman-secrets",
}

type commandResult struct {
	stdout string
	stderr string
}

type jobRunResult struct {
	RunID    string `json:"run_id"`
	RunIndex int    `json:"run_index"`
}

type runShowResult struct {
	RunID  string `json:"run_id"`
	Status string `json:"status"`
}

func TestE2EPodmanSecretsExample(t *testing.T) {
	root := repoRoot(t)
	cli := e2eCLIPath(t, root)
	ctx := context.Background()

	requireExecutable(t, cli, "vectis-cli")
	requireCommand(t, "podman")
	requirePodman(t, ctx)
	requirePodmanImages(t, ctx)
	preparePodmanResources(t, ctx)

	deployConfigDir := t.TempDir()
	env := commandEnv(map[string]string{
		"VECTIS_API_TOKEN":         "",
		"VECTIS_DEPLOY_CONFIG_DIR": deployConfigDir,
	})

	if !truthyEnv("VECTIS_E2E_KEEP_PODMAN") {
		t.Cleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			_, _ = runCommand(cleanupCtx, root, env, nil, cli, "deploy", "podman", "down")
			cleanupPodmanResources(t, cleanupCtx)
		})
	}

	upCtx, cancelUp := context.WithTimeout(ctx, 5*time.Minute)
	defer cancelUp()
	if _, err := runCommand(upCtx, root, env, nil, cli, "deploy", "podman", "--profile", "simple", "up"); err != nil {
		t.Fatalf("start podman stack: %v", err)
	}

	waitForAPIReady(t, "http://localhost:8080/health/ready", 2*time.Minute)
	seedPodmanSecret(t, ctx)

	run := submitSecretsExample(t, ctx, root, env, cli)
	final := waitForRunStatus(t, ctx, root, env, cli, run.RunID, 2*time.Minute)
	if final.Status != "succeeded" {
		t.Fatalf("run %s finished with status %q", run.RunID, final.Status)
	}

	logCtx, cancelLogs := context.WithTimeout(ctx, time.Minute)
	defer cancelLogs()

	logs, err := runCommand(logCtx, root, env, nil, cli, "logs", "run", run.RunID)
	if err != nil {
		t.Fatalf("stream run logs: %v", err)
	}

	if !strings.Contains(logs.stdout, "Run "+run.RunID+" finished successfully.") {
		t.Fatalf("logs did not include completion line for %s\nstdout:\n%s\nstderr:\n%s", run.RunID, logs.stdout, logs.stderr)
	}
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

func e2eCLIPath(t *testing.T, root string) string {
	t.Helper()

	if path := strings.TrimSpace(os.Getenv("VECTIS_E2E_CLI")); path != "" {
		if filepath.IsAbs(path) {
			return path
		}

		return filepath.Join(root, path)
	}

	return filepath.Join(root, "bin", "vectis-cli")
}

func requireExecutable(t *testing.T, path, label string) {
	t.Helper()

	info, err := os.Stat(path)
	if err != nil {
		skipOrFatal(t, "%s binary %s is not available; run mage build or set VECTIS_E2E_CLI", label, path)
	}

	if info.IsDir() {
		skipOrFatal(t, "%s path %s is a directory", label, path)
	}

	if runtime.GOOS != "windows" && info.Mode()&0o111 == 0 {
		skipOrFatal(t, "%s binary %s is not executable", label, path)
	}
}

func requireCommand(t *testing.T, name string) {
	t.Helper()

	if _, err := exec.LookPath(name); err != nil {
		skipOrFatal(t, "%s is not available on PATH", name)
	}
}

func requirePodman(t *testing.T, ctx context.Context) {
	t.Helper()

	checkCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if _, err := runCommand(checkCtx, "", os.Environ(), nil, "podman", "version"); err != nil {
		skipOrFatal(t, "podman is not usable: %v", err)
	}
}

func requirePodmanImages(t *testing.T, ctx context.Context) {
	t.Helper()

	if truthyEnv("VECTIS_E2E_ALLOW_IMAGE_PULL") {
		return
	}

	var missing []string
	for _, image := range requiredPodmanImages {
		checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		_, err := runCommand(checkCtx, "", os.Environ(), nil, "podman", "image", "exists", image)
		cancel()

		if err != nil {
			missing = append(missing, image)
		}
	}

	if len(missing) > 0 {
		skipOrFatal(t, "missing Podman images: %s; run mage imagesComponents and pre-pull external images, or set VECTIS_E2E_ALLOW_IMAGE_PULL=true", strings.Join(missing, ", "))
	}
}

func preparePodmanResources(t *testing.T, ctx context.Context) {
	t.Helper()

	if truthyEnv("VECTIS_E2E_PODMAN_RESET") {
		cleanupPodmanResources(t, ctx)
		return
	}

	var existing []string
	if podmanResourceExists(ctx, "pod", "vectis") {
		existing = append(existing, "pod/vectis")
	}

	for _, name := range podmanResourceNames {
		if podmanResourceExists(ctx, "volume", name) {
			existing = append(existing, "volume/"+name)
		}

		if podmanResourceExists(ctx, "secret", name) {
			existing = append(existing, "secret/"+name)
		}
	}

	if len(existing) > 0 {
		skipOrFatal(t, "existing Podman Vectis resources found: %s; set VECTIS_E2E_PODMAN_RESET=true to let the e2e test remove and recreate them", strings.Join(existing, ", "))
	}
}

func podmanResourceExists(ctx context.Context, kind, name string) bool {
	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	_, err := runCommand(checkCtx, "", os.Environ(), nil, "podman", kind, "exists", name)
	return err == nil
}

func cleanupPodmanResources(t *testing.T, ctx context.Context) {
	t.Helper()

	_, _ = runCommand(ctx, "", os.Environ(), nil, "podman", "pod", "rm", "-f", "vectis")
	for _, name := range podmanResourceNames {
		_, _ = runCommand(ctx, "", os.Environ(), nil, "podman", "volume", "rm", "-f", name)
		_, _ = runCommand(ctx, "", os.Environ(), nil, "podman", "secret", "rm", "-f", name)
	}
}

func waitForAPIReady(t *testing.T, url string, timeout time.Duration) {
	t.Helper()

	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
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

	t.Fatalf("API did not become ready at %s within %s: %v", url, timeout, lastErr)
}

func seedPodmanSecret(t *testing.T, ctx context.Context) {
	t.Helper()

	seedCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	_, err := runCommand(
		seedCtx,
		"",
		os.Environ(),
		strings.NewReader(smokeSecretPlaintext),
		"podman",
		"run",
		"--rm",
		"-i",
		"--pod",
		"vectis",
		"-v",
		"vectis-secrets-data:/data",
		"-v",
		"vectis-podman-secrets:/secrets:ro",
		"vectis-cli:latest",
		"secrets",
		"encryptedfs",
		"put",
		smokeSecretRef,
		"--root",
		smokeSecretRoot,
		"--key-file",
		smokeSecretKeyFile,
		"--force",
	)

	if err != nil {
		t.Fatalf("seed podman encryptedfs secret: %v", err)
	}
}

func submitSecretsExample(t *testing.T, ctx context.Context, root string, env []string, cli string) jobRunResult {
	t.Helper()

	runCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	result, err := runCommand(runCtx, root, env, nil, cli, "--format", "json", "jobs", "run", "examples/secrets.json")
	if err != nil {
		t.Fatalf("submit secrets example: %v", err)
	}

	var out jobRunResult
	if err := json.Unmarshal([]byte(result.stdout), &out); err != nil {
		t.Fatalf("parse jobs run output: %v\nstdout:\n%s\nstderr:\n%s", err, result.stdout, result.stderr)
	}

	if strings.TrimSpace(out.RunID) == "" {
		t.Fatalf("jobs run output missing run_id: %s", result.stdout)
	}

	return out
}

func waitForRunStatus(t *testing.T, ctx context.Context, root string, env []string, cli, runID string, timeout time.Duration) runShowResult {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var last runShowResult
	var lastErr error
	for time.Now().Before(deadline) {
		showCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		result, err := runCommand(showCtx, root, env, nil, cli, "--format", "json", "runs", "show", runID)
		cancel()
		if err == nil {
			var out runShowResult
			if err := json.Unmarshal([]byte(result.stdout), &out); err != nil {
				lastErr = fmt.Errorf("parse runs show output: %w", err)
			} else {
				last = out
				if runStatusTerminal(out.Status) {
					return out
				}
			}
		} else {
			lastErr = err
		}

		time.Sleep(500 * time.Millisecond)
	}

	t.Fatalf("run %s did not reach a terminal state within %s; last=%+v err=%v", runID, timeout, last, lastErr)
	return runShowResult{}
}

func runStatusTerminal(status string) bool {
	switch strings.TrimSpace(strings.ToLower(status)) {
	case "succeeded", "failed", "orphaned", "cancelled", "abandoned", "aborted":
		return true
	default:
		return false
	}
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
