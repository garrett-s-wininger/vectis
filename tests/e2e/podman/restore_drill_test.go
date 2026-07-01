//go:build e2e

package podman_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	podmanRestoreDrillArtifactName    = "podman-restore-report"
	podmanRestoreDrillArtifactContent = "podman-restore-artifact-ok\n"
)

var podmanRestoreDrillVolumes = []string{
	"vectis-postgres-data",
	"vectis-queue-data",
	"vectis-artifact-data",
	"vectis-log-data",
	"vectis-secrets-data",
	"vectis-spiffe-data",
}

type podmanVolumeArchiveEvidence struct {
	Volume    string `json:"volume"`
	Archive   string `json:"archive"`
	SHA256    string `json:"sha256"`
	SizeBytes int64  `json:"size_bytes"`
}

type podmanRestoreDrillEvidence struct {
	SchemaVersion    int                           `json:"schema_version"`
	Deployment       string                        `json:"deployment"`
	Profile          string                        `json:"profile"`
	ExpectedTopology string                        `json:"expected_topology"`
	VolumeArchives   []podmanVolumeArchiveEvidence `json:"volume_archives"`
	PreRestoreRunID  string                        `json:"pre_restore_run_id"`
	PostRestoreRunID string                        `json:"post_restore_run_id"`
	RecordedAt       string                        `json:"recorded_at"`
}

func TestE2EPodmanReferenceRestoreDrill(t *testing.T) {
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

	podmanDeployUp(t, ctx, root, env, cli)
	waitForAPIReady(t, "http://localhost:8080/health/ready", 2*time.Minute)
	seedPodmanSecret(t, ctx)

	jobPath := writePodmanRestoreDrillJob(t)
	before := submitPodmanRestoreDrillJob(t, ctx, root, env, cli, jobPath)
	assertPodmanRestoreDrillRun(t, ctx, root, env, cli, before.RunID)

	evidenceRoot := t.TempDir()
	expectedTopology := runPodmanRestoreDrillCommandToFile(t, root, env, cli, filepath.Join(evidenceRoot, "podman.expected-topology.json"), "--format", "json", "backup", "expect", "podman", "--profile", "simple")

	podmanDeployDown(t, ctx, root, env, cli)

	backupDir := filepath.Join(evidenceRoot, "platform-backup")
	archives := backupPodmanVolumes(t, ctx, backupDir, podmanRestoreDrillVolumes)

	destroyCtx, cancelDestroy := context.WithTimeout(ctx, 2*time.Minute)
	cleanupPodmanResources(t, destroyCtx)
	cancelDestroy()

	restorePodmanVolumes(t, ctx, archives)

	podmanDeployUp(t, ctx, root, env, cli)
	waitForAPIReady(t, "http://localhost:8080/health/ready", 2*time.Minute)

	assertPodmanRestoreDrillRun(t, ctx, root, env, cli, before.RunID)

	after := submitPodmanRestoreDrillJob(t, ctx, root, env, cli, jobPath)
	assertPodmanRestoreDrillRun(t, ctx, root, env, cli, after.RunID)

	evidencePath := writePodmanRestoreDrillEvidence(t, filepath.Join(evidenceRoot, "podman.restore-drill.json"), expectedTopology, archives, before.RunID, after.RunID)
	t.Logf("podman reference restore drill evidence: expected_topology=%s restore_drill=%s archives=%d", expectedTopology, evidencePath, len(archives))
}

func podmanDeployUp(t *testing.T, ctx context.Context, root string, env []string, cli string) {
	t.Helper()

	upCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	if _, err := runCommand(upCtx, root, env, nil, cli, "deploy", "podman", "--profile", "simple", "up"); err != nil {
		t.Fatalf("start podman stack: %v", err)
	}
}

func podmanDeployDown(t *testing.T, ctx context.Context, root string, env []string, cli string) {
	t.Helper()

	downCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if _, err := runCommand(downCtx, root, env, nil, cli, "deploy", "podman", "--profile", "simple", "down"); err != nil {
		t.Fatalf("stop podman stack: %v", err)
	}
}

func writePodmanRestoreDrillJob(t *testing.T) string {
	t.Helper()

	job := map[string]any{
		"id": "podman-reference-restore-smoke",
		"secrets": []map[string]any{
			{
				"id":  "smoke-token",
				"ref": smokeSecretRef,
				"task_keys": []string{
					"root",
				},
				"delivery": map[string]any{
					"type": "file",
					"path": "smoke/token",
				},
			},
		},
		"root": map[string]any{
			"id":   "root",
			"uses": "builtins/sequence",
			"with": map[string]string{
				"execution": "local",
			},
			"ports": map[string]any{
				"steps": map[string]any{
					"nodes": []map[string]any{
						{
							"id":   "secret-check",
							"uses": "builtins/shell",
							"with": map[string]string{
								"command": `test -n "$VECTIS_SECRETS_DIR" && test "$(cksum < "$VECTIS_SECRETS_DIR/smoke/token")" = "2931055294 13" && echo podman-restore-secret-ok`,
							},
						},
						{
							"id":   "artifact-write",
							"uses": "builtins/shell",
							"with": map[string]string{
								"command": "mkdir -p reports; printf 'podman-restore-artifact-ok\\n' > reports/restore.txt; echo podman-restore-artifact-written",
							},
						},
						{
							"id":   "artifact-upload",
							"uses": "builtins/upload-artifact",
							"with": map[string]string{
								"name":          podmanRestoreDrillArtifactName,
								"path":          "reports/restore.txt",
								"content_type":  "text/plain",
								"metadata_json": `{"suite":"podman-reference-restore-drill"}`,
							},
						},
					},
				},
			},
		},
	}

	data, err := json.MarshalIndent(job, "", "  ")
	if err != nil {
		t.Fatalf("marshal podman restore drill job: %v", err)
	}

	data = append(data, '\n')
	path := filepath.Join(t.TempDir(), "podman-reference-restore-smoke.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write podman restore drill job: %v", err)
	}

	return path
}

func submitPodmanRestoreDrillJob(t *testing.T, ctx context.Context, root string, env []string, cli string, jobPath string) jobRunResult {
	t.Helper()

	runCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	result, err := runCommand(runCtx, root, env, nil, cli, "--format", "json", "jobs", "run", jobPath)
	if err != nil {
		t.Fatalf("submit podman restore drill job: %v", err)
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

func assertPodmanRestoreDrillRun(t *testing.T, ctx context.Context, root string, env []string, cli string, runID string) {
	t.Helper()

	final := waitForRunStatus(t, ctx, root, env, cli, runID, 3*time.Minute)
	if final.Status != "succeeded" {
		t.Fatalf("run %s finished with status %q", runID, final.Status)
	}

	assertPodmanRunLogsContain(t, ctx, root, env, cli, runID,
		"podman-restore-secret-ok",
		"podman-restore-artifact-written",
		"Run "+runID+" finished successfully.",
	)

	assertPodmanRunArtifact(t, ctx, root, env, cli, runID, podmanRestoreDrillArtifactName, podmanRestoreDrillArtifactContent)
}

func assertPodmanRunLogsContain(t *testing.T, ctx context.Context, root string, env []string, cli string, runID string, wants ...string) {
	t.Helper()

	logCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()

	logs, err := runCommand(logCtx, root, env, nil, cli, "logs", "run", runID)
	if err != nil {
		t.Fatalf("stream run logs for %s: %v", runID, err)
	}

	for _, want := range wants {
		if !strings.Contains(logs.stdout, want) {
			t.Fatalf("logs for %s did not include %q\nstdout:\n%s\nstderr:\n%s", runID, want, logs.stdout, logs.stderr)
		}
	}
}

func assertPodmanRunArtifact(t *testing.T, ctx context.Context, root string, env []string, cli string, runID, name, want string) {
	t.Helper()

	output := filepath.Join(t.TempDir(), name+".txt")
	downloadCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	if _, err := runCommand(downloadCtx, root, env, nil, cli, "runs", "artifacts", "download", runID, name, "--output", output); err != nil {
		t.Fatalf("download artifact %s for %s: %v", name, runID, err)
	}

	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatalf("read downloaded artifact %s for %s: %v", name, runID, err)
	}

	if string(data) != want {
		t.Fatalf("artifact %s for %s = %q, want %q", name, runID, string(data), want)
	}
}

func backupPodmanVolumes(t *testing.T, ctx context.Context, backupDir string, volumes []string) []podmanVolumeArchiveEvidence {
	t.Helper()

	if err := os.MkdirAll(backupDir, 0o700); err != nil {
		t.Fatalf("mkdir podman backup dir: %v", err)
	}

	archives := make([]podmanVolumeArchiveEvidence, 0, len(volumes))
	for _, volume := range volumes {
		if !podmanResourceExists(ctx, "volume", volume) {
			t.Fatalf("podman volume %s does not exist before backup", volume)
		}

		archive := filepath.Join(backupDir, volume+".tar")
		exportPodmanVolume(t, ctx, volume, archive)

		info, err := os.Stat(archive)
		if err != nil {
			t.Fatalf("stat volume archive %s: %v", archive, err)
		}

		if info.Size() == 0 {
			t.Fatalf("volume archive %s is empty", archive)
		}

		archives = append(archives, podmanVolumeArchiveEvidence{
			Volume:    volume,
			Archive:   archive,
			SHA256:    fileSHA256(t, archive),
			SizeBytes: info.Size(),
		})
	}

	return archives
}

func exportPodmanVolume(t *testing.T, ctx context.Context, volume, output string) {
	t.Helper()

	exportCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	file, err := os.Create(output)
	if err != nil {
		t.Fatalf("create volume archive %s: %v", output, err)
	}

	cmd := exec.CommandContext(exportCtx, "podman", "volume", "export", volume) // #nosec G204 -- e2e harness controls command names/args.
	cmd.Stdout = file
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	runErr := cmd.Run()
	closeErr := file.Close()
	if runErr != nil {
		if errors.Is(exportCtx.Err(), context.DeadlineExceeded) {
			t.Fatalf("podman volume export %s timed out: stderr=%q", volume, stderr.String())
		}

		t.Fatalf("podman volume export %s failed: %v\nstderr:\n%s", volume, runErr, stderr.String())
	}

	if closeErr != nil {
		t.Fatalf("close volume archive %s: %v", output, closeErr)
	}
}

func restorePodmanVolumes(t *testing.T, ctx context.Context, archives []podmanVolumeArchiveEvidence) {
	t.Helper()

	for _, archive := range archives {
		removeCtx, cancelRemove := context.WithTimeout(ctx, 30*time.Second)
		_, _ = runCommand(removeCtx, "", os.Environ(), nil, "podman", "volume", "rm", "-f", archive.Volume)
		cancelRemove()

		createCtx, cancelCreate := context.WithTimeout(ctx, 30*time.Second)
		if _, err := runCommand(createCtx, "", os.Environ(), nil, "podman", "volume", "create", archive.Volume); err != nil {
			cancelCreate()
			t.Fatalf("create restored podman volume %s: %v", archive.Volume, err)
		}
		cancelCreate()

		importCtx, cancelImport := context.WithTimeout(ctx, 2*time.Minute)
		if _, err := runCommand(importCtx, "", os.Environ(), nil, "podman", "volume", "import", archive.Volume, archive.Archive); err != nil {
			cancelImport()
			t.Fatalf("import restored podman volume %s: %v", archive.Volume, err)
		}
		cancelImport()
	}
}

func runPodmanRestoreDrillCommandToFile(t *testing.T, root string, env []string, cli string, outputPath string, args ...string) string {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := runCommand(ctx, root, env, nil, cli, args...)
	if err != nil {
		t.Fatalf("run %s: %v", strings.Join(args, " "), err)
	}

	if err := os.WriteFile(outputPath, []byte(result.stdout), 0o600); err != nil {
		t.Fatalf("write podman restore evidence %s: %v", outputPath, err)
	}

	return outputPath
}

func writePodmanRestoreDrillEvidence(t *testing.T, path, expectedTopology string, archives []podmanVolumeArchiveEvidence, preRestoreRunID, postRestoreRunID string) string {
	t.Helper()

	evidence := podmanRestoreDrillEvidence{
		SchemaVersion:    1,
		Deployment:       "podman",
		Profile:          "simple",
		ExpectedTopology: expectedTopology,
		VolumeArchives:   archives,
		PreRestoreRunID:  preRestoreRunID,
		PostRestoreRunID: postRestoreRunID,
		RecordedAt:       time.Now().UTC().Format(time.RFC3339Nano),
	}

	data, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		t.Fatalf("marshal podman restore evidence: %v", err)
	}

	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write podman restore evidence: %v", err)
	}

	return path
}

func fileSHA256(t *testing.T, path string) string {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s for sha256: %v", path, err)
	}
	defer file.Close()

	h := sha256.New()
	if _, err := io.Copy(h, file); err != nil {
		t.Fatalf("hash %s: %v", path, err)
	}

	return hex.EncodeToString(h.Sum(nil))
}
