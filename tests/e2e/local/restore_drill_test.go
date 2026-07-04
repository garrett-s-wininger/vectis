//go:build e2e

package local_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"vectis/internal/database"
)

func TestE2ELocalRestoreDrill(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("vectis-local e2e process cleanup uses Unix signals")
	}

	root := repoRoot(t)
	cli := e2eBinaryPath(t, root, "VECTIS_E2E_CLI", "vectis-cli")
	local := e2eBinaryPath(t, root, "VECTIS_E2E_LOCAL", "vectis-local")

	requireExecutable(t, cli, "vectis-cli")
	requireExecutable(t, local, "vectis-local")
	ports := localRestoreDrillPortSet()
	requireLocalPortsAvailable(t, ports.all())

	sourceDataHome := t.TempDir()
	sourceEnv := localRestoreDrillEnv(t, root, sourceDataHome, ports)
	prepareLocalRestoreDrillEvidenceDirs(t, sourceDataHome)

	proc := startVectisLocal(t, root, sourceEnv, local)
	sourceStopped := false
	if !truthyEnv("VECTIS_E2E_KEEP_LOCAL") {
		t.Cleanup(func() {
			if !sourceStopped {
				stopVectisLocal(t, proc)
			}
		})
	}

	waitForAPIReady(t, proc, ports.readyURL(), 2*time.Minute)
	runLocalRestoreDrillHealthCheck(t, root, sourceEnv, cli, sourceDataHome)

	seedLocalSmokeSecret(t, root, sourceEnv, cli, sourceDataHome)
	before := runLocalRestoreDrillSmokeJob(t, root, sourceEnv, cli)
	beforeStatuses, beforeFinal := waitForRunTerminal(t, root, sourceEnv, cli, before.RunID, 3*time.Minute)
	if beforeFinal.Status != "succeeded" {
		t.Fatalf("pre-restore run %s finished with status %q; observed statuses=%v\nvectis-local stderr:\n%s", before.RunID, beforeFinal.Status, beforeStatuses, proc.stderr.String())
	}

	waitForRunTaskCompletionSucceeded(t, root, sourceEnv, cli, before.RunID, 30*time.Second)
	assertRunTasksSucceeded(t, root, sourceEnv, cli, before.RunID)
	assertRunLogsContain(t, root, sourceEnv, cli, before.RunID,
		"canonical-secret-ok",
		"canonical-fanout-ok",
		"Run "+before.RunID+" finished successfully.",
	)

	assertRunArtifact(t, root, sourceEnv, cli, before.RunID, smokeArtifactName, smokeArtifactPath, smokeArtifactContent)
	assertRunArtifact(t, root, sourceEnv, cli, before.RunID, smokeRetryArtifactName, smokeRetryArtifactPath, smokeRetryArtifactContent)

	stopVectisLocal(t, proc)
	sourceStopped = true

	backupDataHome := filepath.Join(t.TempDir(), "data-home")
	copyLocalRestoreDrillTree(t, sourceDataHome, backupDataHome)

	restoredDataHome := t.TempDir()
	copyLocalRestoreDrillTree(t, backupDataHome, restoredDataHome)
	prepareLocalRestoreDrillEvidenceDirs(t, restoredDataHome)
	assertLocalRestoreDrillTLSMaterial(t, restoredDataHome)

	restoredEnv := localRestoreDrillEnv(t, root, restoredDataHome, ports)
	evidence := verifyLocalRestoreDrillBackupEvidence(t, root, restoredEnv, cli, restoredDataHome)
	t.Logf("local restore drill evidence: inventory=%s manifest=%s reports=%d", evidence.InventoryPath, evidence.ManifestPath, len(evidence.StorageReportPaths))

	restoredProc := startVectisLocal(t, root, restoredEnv, local)
	if !truthyEnv("VECTIS_E2E_KEEP_LOCAL") {
		t.Cleanup(func() { stopVectisLocal(t, restoredProc) })
	}

	waitForAPIReady(t, restoredProc, ports.readyURL(), 2*time.Minute)
	runLocalRestoreDrillHealthCheck(t, root, restoredEnv, cli, restoredDataHome)

	restoredBefore, err := showRun(t, root, restoredEnv, cli, before.RunID)
	if err != nil {
		t.Fatalf("show restored pre-restore run: %v\nvectis-local stderr:\n%s", err, restoredProc.stderr.String())
	}

	if restoredBefore.Status != "succeeded" {
		t.Fatalf("restored pre-restore run %s status = %q, want succeeded", before.RunID, restoredBefore.Status)
	}

	waitForRunTaskCompletionSucceeded(t, root, restoredEnv, cli, before.RunID, 30*time.Second)
	assertRunTasksSucceeded(t, root, restoredEnv, cli, before.RunID)
	assertRunLogsContain(t, root, restoredEnv, cli, before.RunID,
		"canonical-secret-ok",
		"canonical-fanout-ok",
		"Run "+before.RunID+" finished successfully.",
	)

	assertRunArtifact(t, root, restoredEnv, cli, before.RunID, smokeArtifactName, smokeArtifactPath, smokeArtifactContent)
	assertRunArtifact(t, root, restoredEnv, cli, before.RunID, smokeRetryArtifactName, smokeRetryArtifactPath, smokeRetryArtifactContent)

	after := runLocalRestoreDrillSmokeJob(t, root, restoredEnv, cli)
	afterStatuses, afterFinal := waitForRunTerminal(t, root, restoredEnv, cli, after.RunID, 3*time.Minute)
	if afterFinal.Status != "succeeded" {
		t.Fatalf("post-restore run %s finished with status %q; observed statuses=%v\nvectis-local stderr:\n%s", after.RunID, afterFinal.Status, afterStatuses, restoredProc.stderr.String())
	}

	waitForRunTaskCompletionSucceeded(t, root, restoredEnv, cli, after.RunID, 30*time.Second)
	assertRunTasksSucceeded(t, root, restoredEnv, cli, after.RunID)
	assertRunLogsContain(t, root, restoredEnv, cli, after.RunID,
		"canonical-secret-ok",
		"canonical-fanout-ok",
		"Run "+after.RunID+" finished successfully.",
	)

	assertRunArtifact(t, root, restoredEnv, cli, after.RunID, smokeArtifactName, smokeArtifactPath, smokeArtifactContent)
	assertRunArtifact(t, root, restoredEnv, cli, after.RunID, smokeRetryArtifactName, smokeRetryArtifactPath, smokeRetryArtifactContent)

	evidence.RestoreValidationPath = writeLocalRestoreDrillRestoreValidation(t, root, restoredEnv, cli, evidence, after.RunID)
	t.Logf("local restore drill validation: %s", evidence.RestoreValidationPath)
}

type localRestoreDrillEvidence struct {
	InventoryPath         string
	ManifestPath          string
	StorageReportPaths    []string
	VerificationPath      string
	RestoreValidationPath string
}

type localRestoreDrillRestoreValidation struct {
	Status       string `json:"status"`
	Verification struct {
		Status  string `json:"status"`
		Summary struct {
			StorageReportsVerified int `json:"storage_reports_verified"`
		} `json:"summary"`
	} `json:"verification"`
	SmokeRun struct {
		RunID  string `json:"run_id"`
		Status string `json:"status"`
		Passed bool   `json:"passed"`
	} `json:"smoke_run"`
}

type localRestoreDrillInventory struct {
	LocalState []localRestoreDrillInventoryPath `json:"local_state"`
}

type localRestoreDrillInventoryPath struct {
	ID      string `json:"id"`
	Path    string `json:"path"`
	Enabled bool   `json:"enabled"`
	Exists  bool   `json:"exists"`
}

type localRestoreDrillPorts struct {
	api                 int
	queue               int
	registry            int
	log                 int
	cellIngress         int
	artifact            int
	orchestrator        int
	secrets             int
	queueMetrics        int
	workerMetrics       int
	logMetrics          int
	logForwarderMetrics int
	reconcilerMetrics   int
	catalogMetrics      int
	cellIngressMetrics  int
	artifactMetrics     int
	orchestratorMetrics int
	secretsMetrics      int
}

func localRestoreDrillPortSet() localRestoreDrillPorts {
	return localRestoreDrillPorts{
		api:                 18080,
		queue:               18081,
		registry:            18082,
		log:                 18083,
		cellIngress:         18085,
		artifact:            18086,
		orchestrator:        18087,
		secrets:             18090,
		queueMetrics:        19081,
		workerMetrics:       19082,
		logMetrics:          19083,
		logForwarderMetrics: 19084,
		reconcilerMetrics:   19085,
		catalogMetrics:      19086,
		cellIngressMetrics:  19087,
		artifactMetrics:     19089,
		orchestratorMetrics: 19090,
		secretsMetrics:      19091,
	}
}

func (p localRestoreDrillPorts) all() []int {
	return []int{
		p.api,
		p.queue,
		p.registry,
		p.log,
		p.cellIngress,
		p.artifact,
		p.orchestrator,
		p.secrets,
		p.queueMetrics,
		p.workerMetrics,
		p.logMetrics,
		p.logForwarderMetrics,
		p.reconcilerMetrics,
		p.catalogMetrics,
		p.cellIngressMetrics,
		p.artifactMetrics,
		p.orchestratorMetrics,
		p.secretsMetrics,
	}
}

func (p localRestoreDrillPorts) readyURL() string {
	return fmt.Sprintf("http://localhost:%d/health/ready", p.api)
}

func localRestoreDrillEnv(t *testing.T, root, dataHome string, ports localRestoreDrillPorts) []string {
	t.Helper()

	tlsDir := filepath.Join(dataHome, "vectis", "local-tls")
	tmpDir := filepath.Join(dataHome, "tmp")
	return commandEnv(map[string]string{
		"PATH":            filepath.Join(root, "bin") + string(os.PathListSeparator) + os.Getenv("PATH"),
		"TMPDIR":          tmpDir,
		"XDG_DATA_HOME":   dataHome,
		"XDG_RUNTIME_DIR": shortTempDir(t, "vectis-e2e-restore-runtime-*"),
		"VECTIS_ACTION_REGISTRY_ALLOWED_NAMESPACES":  "",
		"VECTIS_ACTION_REGISTRY_ALLOWED_SOURCES":     "",
		"VECTIS_ACTION_REGISTRY_LOCAL_ROOTS":         filepath.Join(root, "examples", "actions"),
		"VECTIS_ACTION_REGISTRY_REQUIRE_DIGEST_PINS": "false",
		"VECTIS_API_SERVER_HOST":                     "localhost",
		"VECTIS_API_SERVER_PORT":                     strconv.Itoa(ports.api),
		"VECTIS_API_TOKEN":                           "",
		"VECTIS_ARTIFACT_GRPC_PORT":                  strconv.Itoa(ports.artifact),
		"VECTIS_ARTIFACT_METRICS_PORT":               strconv.Itoa(ports.artifactMetrics),
		"VECTIS_CATALOG_METRICS_PORT":                strconv.Itoa(ports.catalogMetrics),
		"VECTIS_CELL_ID":                             "local",
		"VECTIS_CELL_INGRESS_PORT":                   strconv.Itoa(ports.cellIngress),
		"VECTIS_CELL_INGRESS_METRICS_PORT":           strconv.Itoa(ports.cellIngressMetrics),
		"VECTIS_GRPC_TLS_INSECURE":                   "false",
		"VECTIS_GRPC_TLS_CA_FILE":                    "",
		"VECTIS_GRPC_TLS_CERT_FILE":                  "",
		"VECTIS_GRPC_TLS_KEY_FILE":                   "",
		"VECTIS_GRPC_TLS_CLIENT_CA_FILE":             "",
		"VECTIS_GRPC_TLS_CLIENT_CERT_FILE":           "",
		"VECTIS_GRPC_TLS_CLIENT_KEY_FILE":            "",
		"VECTIS_GRPC_TLS_SERVER_NAME":                "",
		"VECTIS_LOG_FORWARDER_METRICS_PORT":          strconv.Itoa(ports.logForwarderMetrics),
		"VECTIS_LOG_GRPC_PORT":                       strconv.Itoa(ports.log),
		"VECTIS_LOG_METRICS_PORT":                    strconv.Itoa(ports.logMetrics),
		"VECTIS_LOCAL_GRPC_INSECURE":                 "false",
		"VECTIS_LOCAL_HTTP_TLS":                      "off",
		"VECTIS_LOCAL_DOCS_ENABLED":                  "false",
		"VECTIS_LOCAL_TLS_DIR":                       tlsDir,
		"VECTIS_ORCHESTRATOR_METRICS_PORT":           strconv.Itoa(ports.orchestratorMetrics),
		"VECTIS_ORCHESTRATOR_PORT":                   strconv.Itoa(ports.orchestrator),
		"VECTIS_QUEUE_METRICS_PORT":                  strconv.Itoa(ports.queueMetrics),
		"VECTIS_QUEUE_PORT":                          strconv.Itoa(ports.queue),
		"VECTIS_RECONCILER_METRICS_PORT":             strconv.Itoa(ports.reconcilerMetrics),
		"VECTIS_REGISTRY_PORT":                       strconv.Itoa(ports.registry),
		"VECTIS_SECRETS_METRICS_PORT":                strconv.Itoa(ports.secretsMetrics),
		"VECTIS_SECRETS_PORT":                        strconv.Itoa(ports.secrets),
		"VECTIS_SECRETS_ENCRYPTEDFS_ROOT":            localManagedSecretsDir(dataHome, "local"),
		"VECTIS_SECRETS_ENCRYPTEDFS_KEY_FILE":        localManagedSecretsKeyFile(dataHome, "local"),
		"VECTIS_SOURCE_CHECKOUT_ROOT":                filepath.Join(dataHome, "vectis", "source-checkouts"),
		database.EnvDatabaseDriver:                   "sqlite3",
		database.EnvDatabaseDSN:                      "",
		database.EnvGlobalDatabaseDSN:                "",
		database.EnvCellDatabaseDSN:                  "",
		"VECTIS_WORKER_EXECUTION_BACKEND":            "host",
		"VECTIS_WORKER_METRICS_PORT":                 strconv.Itoa(ports.workerMetrics),
	})
}

func prepareLocalRestoreDrillEvidenceDirs(t *testing.T, dataHome string) {
	t.Helper()

	for _, dir := range []string{
		filepath.Join(dataHome, "tmp"),
		filepath.Join(dataHome, "vectis", "source-checkouts"),
		filepath.Join(dataHome, "vectis", "log-forwarder", "spool"),
		filepath.Join(dataHome, "tmp", "vectis-log-spool", "pending"),
	} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir restore drill evidence dir %s: %v", dir, err)
		}
	}
}

func assertLocalRestoreDrillTLSMaterial(t *testing.T, dataHome string) {
	t.Helper()

	tlsDir := filepath.Join(dataHome, "vectis", "local-tls")
	for _, name := range []string{"ca.pem", "ca.key", "server.pem", "server.key", "client-ca-bundle.pem"} {
		path := filepath.Join(tlsDir, name)
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("restored TLS material %s is missing: %v", path, err)
		}

		if info.IsDir() || info.Size() == 0 {
			t.Fatalf("restored TLS material %s is not a non-empty file", path)
		}
	}
}

func runLocalRestoreDrillHealthCheck(t *testing.T, root string, env []string, cli string, dataHome string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := runCommand(ctx, root, localRestoreDrillBackupEvidenceEnv(env, dataHome), nil, cli, "--format", "json", "health", "check"); err != nil {
		t.Fatalf("health check after restore: %v", err)
	}
}

func runLocalRestoreDrillSmokeJob(t *testing.T, root string, env []string, cli string) jobRunResult {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := runCommand(ctx, root, env, nil, cli, "--format", "json", "jobs", "run", filepath.Join(root, "examples", "e2e-canonical.json"))
	if err != nil {
		t.Fatalf("run restore drill smoke job: %v", err)
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

func verifyLocalRestoreDrillBackupEvidence(t *testing.T, root string, env []string, cli string, dataHome string) localRestoreDrillEvidence {
	t.Helper()

	evidenceDir := filepath.Join(t.TempDir(), "evidence")
	if err := os.MkdirAll(evidenceDir, 0o755); err != nil {
		t.Fatalf("mkdir restore evidence dir: %v", err)
	}

	backupEnv := localRestoreDrillBackupEvidenceEnv(env, dataHome)
	inventory := runLocalRestoreDrillCommandToFile(t, root, backupEnv, cli, filepath.Join(evidenceDir, "restored.inventory.json"), "--format", "json", "backup", "inventory")
	manifest := runLocalRestoreDrillCommandToFile(t, root, backupEnv, cli, filepath.Join(evidenceDir, "restored.backup-manifest.json"), "--format", "json", "backup", "manifest", inventory)

	localState := readLocalRestoreDrillInventoryPaths(t, inventory)
	reportSpecs := []struct {
		id      string
		surface string
		name    string
	}{
		{id: "queue.persistence", surface: "queue", name: "queue.storage-report.json"},
		{id: "log.storage", surface: "logs", name: "logs.storage-report.json"},
		{id: "artifact.storage", surface: "artifact", name: "artifact.storage-report.json"},
		{id: "log_forwarder.spool", surface: "log-forwarder-spool", name: "log-forwarder-spool.storage-report.json"},
		{id: "worker.pending_log_spool", surface: "worker-log-spool", name: "worker-log-spool.storage-report.json"},
	}

	reportPaths := make([]string, 0, len(reportSpecs))
	for _, spec := range reportSpecs {
		path := requireLocalRestoreDrillInventoryPath(t, localState, spec.id)
		report := filepath.Join(evidenceDir, spec.name)
		runLocalRestoreDrillCommandToFile(t, root, backupEnv, cli, report, "--format", "json", "storage", "verify", spec.surface, "--dir", path)
		reportPaths = append(reportPaths, report)
	}

	args := []string{"--format", "json", "backup", "verify", "--storage-max-age", "24h"}
	for _, path := range reportPaths {
		args = append(args, "--storage-report", path)
	}

	args = append(args, manifest)
	verification := runLocalRestoreDrillCommandToFile(t, root, backupEnv, cli, filepath.Join(evidenceDir, "restored.backup-verification.json"), args...)

	return localRestoreDrillEvidence{
		InventoryPath:      inventory,
		ManifestPath:       manifest,
		StorageReportPaths: reportPaths,
		VerificationPath:   verification,
	}
}

func writeLocalRestoreDrillRestoreValidation(t *testing.T, root string, env []string, cli string, evidence localRestoreDrillEvidence, smokeRunID string) string {
	t.Helper()

	args := []string{
		"--format", "json",
		"backup", "restore-validation",
		"--storage-max-age", "24h",
		"--smoke-run", smokeRunID,
		"--deployment", "local",
		"--profile", "restore-drill",
	}

	for _, report := range evidence.StorageReportPaths {
		args = append(args, "--storage-report", report)
	}

	args = append(args, evidence.ManifestPath)
	validationPath := runLocalRestoreDrillCommandToFile(t, root, env, cli, filepath.Join(filepath.Dir(evidence.ManifestPath), "restored.restore-validation.json"), args...)
	data, err := os.ReadFile(validationPath)
	if err != nil {
		t.Fatalf("read restore validation: %v", err)
	}

	var validation localRestoreDrillRestoreValidation
	if err := json.Unmarshal(data, &validation); err != nil {
		t.Fatalf("parse restore validation: %v\n%s", err, string(data))
	}

	if validation.Status != "ok" || validation.Verification.Status != "ok" {
		t.Fatalf("restore validation status = %+v", validation)
	}

	if validation.Verification.Summary.StorageReportsVerified < len(evidence.StorageReportPaths) {
		t.Fatalf("restore validation verified %d storage report(s), want at least %d", validation.Verification.Summary.StorageReportsVerified, len(evidence.StorageReportPaths))
	}

	if validation.SmokeRun.RunID != smokeRunID || validation.SmokeRun.Status != "succeeded" || !validation.SmokeRun.Passed {
		t.Fatalf("restore validation smoke run = %+v, want run_id=%s succeeded", validation.SmokeRun, smokeRunID)
	}

	return validationPath
}

func localRestoreDrillBackupEvidenceEnv(env []string, dataHome string) []string {
	globalDB := filepath.Join(dataHome, "vectis", "global", "db.sqlite3")
	cellDB := filepath.Join(dataHome, "vectis", "cells", "local", "db.sqlite3")
	tlsDir := filepath.Join(dataHome, "vectis", "local-tls")
	return localRestoreDrillEnvWithOverrides(env, map[string]string{
		database.EnvDatabaseDriver:         "sqlite3",
		database.EnvDatabaseDSN:            globalDB,
		database.EnvGlobalDatabaseDSN:      globalDB,
		database.EnvCellDatabaseDSN:        cellDB,
		"VECTIS_GRPC_TLS_CA_FILE":          filepath.Join(tlsDir, "ca.pem"),
		"VECTIS_GRPC_TLS_CERT_FILE":        filepath.Join(tlsDir, "server.pem"),
		"VECTIS_GRPC_TLS_KEY_FILE":         filepath.Join(tlsDir, "server.key"),
		"VECTIS_GRPC_TLS_CLIENT_CA_FILE":   filepath.Join(tlsDir, "client-ca-bundle.pem"),
		"VECTIS_GRPC_TLS_CLIENT_CERT_FILE": filepath.Join(tlsDir, "server.pem"),
		"VECTIS_GRPC_TLS_CLIENT_KEY_FILE":  filepath.Join(tlsDir, "server.key"),
	})
}

func localRestoreDrillEnvWithOverrides(env []string, overrides map[string]string) []string {
	out := make([]string, 0, len(env)+len(overrides))
	seen := make(map[string]bool, len(overrides))
	for _, entry := range env {
		key, _, ok := strings.Cut(entry, "=")
		if ok {
			if value, overridden := overrides[key]; overridden {
				out = append(out, key+"="+value)
				seen[key] = true
				continue
			}
		}

		out = append(out, entry)
		if ok {
			seen[key] = true
		}
	}

	for key, value := range overrides {
		if !seen[key] {
			out = append(out, key+"="+value)
		}
	}

	return out
}

func runLocalRestoreDrillCommandToFile(t *testing.T, root string, env []string, cli string, outputPath string, args ...string) string {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := runCommand(ctx, root, env, nil, cli, args...)
	if err != nil {
		t.Fatalf("run %s: %v", strings.Join(args, " "), err)
	}

	if err := os.WriteFile(outputPath, []byte(result.stdout), 0o600); err != nil {
		t.Fatalf("write restore evidence %s: %v", outputPath, err)
	}

	return outputPath
}

func readLocalRestoreDrillInventoryPaths(t *testing.T, path string) map[string]localRestoreDrillInventoryPath {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read restore inventory: %v", err)
	}

	var inventory localRestoreDrillInventory
	if err := json.Unmarshal(data, &inventory); err != nil {
		t.Fatalf("decode restore inventory: %v\n%s", err, string(data))
	}

	out := make(map[string]localRestoreDrillInventoryPath, len(inventory.LocalState))
	for _, item := range inventory.LocalState {
		out[item.ID] = item
	}

	return out
}

func requireLocalRestoreDrillInventoryPath(t *testing.T, paths map[string]localRestoreDrillInventoryPath, id string) string {
	t.Helper()

	item, ok := paths[id]
	if !ok {
		t.Fatalf("restored inventory is missing local_state path %s", id)
	}

	if !item.Enabled {
		t.Fatalf("restored inventory path %s is disabled", id)
	}

	if strings.TrimSpace(item.Path) == "" {
		t.Fatalf("restored inventory path %s is empty", id)
	}

	if !item.Exists {
		t.Fatalf("restored inventory path %s does not exist: %s", id, item.Path)
	}

	return item.Path
}

func copyLocalRestoreDrillTree(t *testing.T, src, dst string) {
	t.Helper()

	err := filepath.WalkDir(src, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}

		target := filepath.Join(dst, rel)
		info, err := entry.Info()
		if err != nil {
			return err
		}

		if entry.IsDir() {
			return os.MkdirAll(target, info.Mode().Perm())
		}

		if !info.Mode().IsRegular() {
			return nil
		}

		return copyLocalRestoreDrillFile(path, target, info.Mode().Perm())
	})

	if err != nil {
		t.Fatalf("copy %s to %s: %v", src, dst, err)
	}
}

func copyLocalRestoreDrillFile(src, dst string, mode fs.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}

	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}

	_, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}

	return closeErr
}
