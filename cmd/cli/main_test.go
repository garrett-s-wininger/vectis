package main

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
	"vectis/api/gen/go"
	"vectis/internal/retention"
)

func TestEffectiveToken_envOverridesFile(t *testing.T) {
	t.Setenv("VECTIS_API_TOKEN", "env-token")

	if got := effectiveToken(); got != "env-token" {
		t.Fatalf("expected env-token, got %s", got)
	}
}

func TestEffectiveToken_fallbackToFile(t *testing.T) {
	t.Setenv("VECTIS_API_TOKEN", "")

	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("XDG_CONFIG_HOME", tmpDir)

	path, err := cliTokenFilePath()
	if err != nil {
		t.Fatalf("token path: %v", err)
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir token dir: %v", err)
	}

	if err := os.WriteFile(path, []byte("file-token\n"), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}

	if got := effectiveToken(); got != "file-token" {
		t.Fatalf("expected file-token, got %s", got)
	}
}

func TestEffectiveToken_empty(t *testing.T) {
	t.Setenv("VECTIS_API_TOKEN", "")
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("XDG_CONFIG_HOME", tmpDir)

	if got := effectiveToken(); got != "" {
		t.Fatalf("expected empty, got %s", got)
	}
}

func TestTokenPersistence(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("XDG_CONFIG_HOME", tmpDir)

	// Write
	if err := writePersistedToken("secret"); err != nil {
		t.Fatal(err)
	}

	// Read
	if got := readPersistedToken(); got != "secret" {
		t.Fatalf("expected secret, got %s", got)
	}

	// Delete
	if err := deletePersistedToken(); err != nil {
		t.Fatal(err)
	}

	if got := readPersistedToken(); got != "" {
		t.Fatalf("expected empty after delete, got %s", got)
	}
}

func TestPrintRetentionReport_includesTaskCascadeCounts(t *testing.T) {
	cutoff := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)
	report := retention.Report{
		DryRun: true,
		Cutoffs: retention.Cutoffs{
			TerminalRuns:    &cutoff,
			JobDefinitions:  &cutoff,
			IdempotencyKeys: &cutoff,
			AuditLog:        &cutoff,
		},
		Counts: retention.Counts{
			TerminalRuns:        3,
			RunDispatchEvents:   4,
			RunTasks:            5,
			TaskAttempts:        6,
			RunSegments:         7,
			SegmentExecutions:   8,
			TaskDispatchIntents: 9,
			JobDefinitions:      10,
			IdempotencyKeys:     11,
			AuditLog:            12,
		},
	}

	var buf bytes.Buffer
	printRetentionReport(&buf, report, retention.FileReport{RunLogFiles: 13, RunLogBytes: 14})

	out := buf.String()
	for _, want := range []string{
		"would_delete.terminal_runs=3",
		"would_delete.run_dispatch_events=4",
		"would_delete.run_tasks=5",
		"would_delete.task_attempts=6",
		"would_delete.run_segments=7",
		"would_delete.segment_executions=8",
		"would_delete.task_dispatch_intents=9",
		"would_delete.job_definitions=10",
		"would_delete.idempotency_keys=11",
		"would_delete.audit_log=12",
		"would_delete.run_log_files=13",
		"would_delete.run_log_bytes=14",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestSetIdempotencyHeader(t *testing.T) {
	req, err := http.NewRequest(http.MethodPost, "http://example.test", nil)
	if err != nil {
		t.Fatal(err)
	}

	setIdempotencyHeader(req, " retry-key ")

	if got := req.Header.Get("Idempotency-Key"); got != "retry-key" {
		t.Fatalf("Idempotency-Key = %q, want retry-key", got)
	}
}

func TestTriggerJob_sendsIdempotencyKey(t *testing.T) {
	oldKey := triggerIdemKey
	triggerIdemKey = "trigger-retry-key"
	t.Cleanup(func() { triggerIdemKey = oldKey })

	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/jobs/trigger/job-1" {
			t.Errorf("path=%s", r.URL.Path)
		}

		if got := r.Header.Get("Idempotency-Key"); got != "trigger-retry-key" {
			t.Errorf("Idempotency-Key=%q", got)
		}

		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"job_id": "job-1", "run_id": "run-1", "run_index": 1,
		})
	})

	triggerJob(&cobra.Command{}, []string{"job-1"})
}

func TestTriggerJob_sendsTargetCells(t *testing.T) {
	oldCells := triggerCellIDs
	triggerCellIDs = []string{"iad-a", "pdx-b", "iad-a", "sjc-c,pdx-b"}
	t.Cleanup(func() { triggerCellIDs = oldCells })

	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/jobs/trigger/job-1" {
			t.Errorf("path=%s", r.URL.Path)
		}

		var body struct {
			CellIDs []string `json:"cell_ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode body: %v", err)
		}

		wantCells := []string{"iad-a", "pdx-b", "sjc-c"}
		if strings.Join(body.CellIDs, ",") != strings.Join(wantCells, ",") {
			t.Errorf("cell_ids=%v, want %v", body.CellIDs, wantCells)
		}

		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"job_id": "job-1",
			"runs": []map[string]any{
				{"run_id": "run-a", "run_index": 1, "cell_id": "iad-a"},
				{"run_id": "run-b", "run_index": 2, "cell_id": "pdx-b"},
				{"run_id": "run-c", "run_index": 3, "cell_id": "sjc-c"},
			},
		})
	})

	var buf bytes.Buffer
	if err := triggerJobWithOutput(&cobra.Command{}, []string{"job-1"}, &buf); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{"CELL", "RUN ID", "iad-a", "run-a", "pdx-b", "run-b", "sjc-c", "run-c"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestWriteTriggerJobResult_jsonOutputIncludesMultiCellRuns(t *testing.T) {
	withOutputFormat(t, outputJSON)

	result := jobRunResult{
		JobID: "job-1",
		Runs: []jobRunCellResult{
			{RunID: "run-a", RunIndex: 1, CellID: "iad-a"},
			{RunID: "run-b", RunIndex: 2, CellID: "pdx-b"},
		},
	}

	var buf bytes.Buffer
	if err := writeTriggerJobResult(&cobra.Command{}, &buf, result); err != nil {
		t.Fatal(err)
	}

	var decoded jobRunResult
	if err := json.Unmarshal(buf.Bytes(), &decoded); err != nil {
		t.Fatalf("invalid JSON output: %v\n%s", err, buf.String())
	}

	if decoded.JobID != "job-1" || len(decoded.Runs) != 2 || decoded.Runs[1].CellID != "pdx-b" {
		t.Fatalf("unexpected JSON trigger output: %+v", decoded)
	}
}

func TestTriggerJobRequestBody_rejectsEmptyCell(t *testing.T) {
	if _, err := triggerJobRequestBody([]string{"iad-a,"}); err == nil {
		t.Fatal("expected empty cell error")
	}
}

func TestCellsStatus_tableOutput(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/cells/status" {
			t.Errorf("path=%s", r.URL.Path)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"cells": []map[string]any{
				{
					"cell_id":               "pdx-b",
					"ingress_required":      true,
					"ingress_configured":    false,
					"ingress_reachable":     false,
					"status":                "missing_route",
					"queued":                3,
					"stuck":                 2,
					"task_dispatch_pending": 6,
					"catalog_pending":       4,
					"catalog_failed":        1,
					"catalog_total":         9,
					"error":                 "cell ingress endpoint is not configured",
				},
				{
					"cell_id":               "iad-a",
					"ingress_required":      true,
					"ingress_configured":    true,
					"ingress_reachable":     true,
					"status":                "ready",
					"queued":                1,
					"stuck":                 0,
					"task_dispatch_pending": 0,
					"catalog_pending":       0,
					"catalog_failed":        0,
					"catalog_total":         5,
				},
			},
		})
	})

	var buf bytes.Buffer
	if err := cellsStatus(&buf); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{"CELL", "STATUS", "TASK PENDING", "CATALOG P/F/T", "iad-a", "ready", "0/0/5", "pdx-b", "missing_route", "6", "4/1/9"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}

	if strings.Index(out, "iad-a") > strings.Index(out, "pdx-b") {
		t.Fatalf("expected cells to be sorted by cell ID, got:\n%s", out)
	}
}

func TestCellsStatus_jsonOutput(t *testing.T) {
	withOutputFormat(t, outputJSON)
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"cells": []map[string]any{
				{
					"cell_id":               "iad-a",
					"ingress_required":      true,
					"ingress_configured":    true,
					"ingress_reachable":     true,
					"status":                "ready",
					"queued":                1,
					"stuck":                 0,
					"task_dispatch_pending": 7,
					"catalog_total":         5,
				},
			},
		})
	})

	var buf bytes.Buffer
	if err := cellsStatus(&buf); err != nil {
		t.Fatal(err)
	}

	var result cellsStatusResult
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("invalid JSON output: %v\n%s", err, buf.String())
	}

	if len(result.Cells) != 1 || result.Cells[0].CellID != "iad-a" || result.Cells[0].TaskDispatchPending != 7 || result.Cells[0].CatalogTotal != 5 {
		t.Fatalf("unexpected cells status JSON: %+v", result)
	}
}

func TestCellsStatus_unexpectedStatus(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	if err := cellsStatus(io.Discard); err == nil {
		t.Fatal("expected error")
	}
}

func TestRunJob_sendsIdempotencyKey(t *testing.T) {
	oldKey := runIdemKey
	runIdemKey = "run-retry-key"
	t.Cleanup(func() { runIdemKey = oldKey })
	oldCell := runCellID
	runCellID = ""
	t.Cleanup(func() { runCellID = oldCell })

	jobPath := filepath.Join(t.TempDir(), "job.json")
	if err := os.WriteFile(jobPath, []byte(`{"root":{"id":"root","uses":"builtins/shell","with":{"command":"echo hi"}}}`), 0o600); err != nil {
		t.Fatal(err)
	}

	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/jobs/run" {
			t.Errorf("path=%s", r.URL.Path)
		}

		if got := r.Header.Get("Idempotency-Key"); got != "run-retry-key" {
			t.Errorf("Idempotency-Key=%q", got)
		}

		var body api.Job
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode body: %v", err)
		}

		if body.GetRoot() == nil {
			t.Errorf("expected raw job definition body, got root=nil")
		}

		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": "job-ephemeral", "run_id": "run-1",
		})
	})

	runJob(&cobra.Command{}, []string{jobPath})
}

func TestRunJob_sendsTargetCell(t *testing.T) {
	oldCell := runCellID
	runCellID = "pdx-b"
	t.Cleanup(func() { runCellID = oldCell })

	jobPath := filepath.Join(t.TempDir(), "job.json")
	if err := os.WriteFile(jobPath, []byte(`{"root":{"id":"root","uses":"builtins/shell","with":{"command":"echo hi"}}}`), 0o600); err != nil {
		t.Fatal(err)
	}

	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/jobs/run" {
			t.Errorf("path=%s", r.URL.Path)
		}

		var body struct {
			Job    api.Job `json:"job"`
			CellID string  `json:"cell_id"`
		}

		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode body: %v", err)
		}

		if body.CellID != "pdx-b" {
			t.Errorf("cell_id=%q, want pdx-b", body.CellID)
		}

		if body.Job.GetRoot() == nil {
			t.Errorf("expected wrapped job definition, got root=nil")
		}

		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": "job-ephemeral", "run_id": "run-pdx",
		})
	})

	runJob(&cobra.Command{}, []string{jobPath})
}

func TestWritePersistedToken_createsDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("XDG_CONFIG_HOME", tmpDir)

	path, _ := cliTokenFilePath()
	_ = os.RemoveAll(filepath.Dir(path))

	if err := writePersistedToken("tok"); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("token file not created: %v", err)
	}
}

func TestResetTargets(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	cacheDir := filepath.Join(tmpDir, "cache")
	deployDir := filepath.Join(tmpDir, "deploy")
	t.Setenv("HOME", tmpDir)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(tmpDir, "config"))
	t.Setenv("XDG_DATA_HOME", dataDir)
	t.Setenv("XDG_CACHE_HOME", cacheDir)
	t.Setenv(envDeployConfigDir, deployDir)

	configDir, err := os.UserConfigDir()
	if err != nil {
		t.Fatalf("config dir: %v", err)
	}

	cacheHome, err := os.UserCacheDir()
	if err != nil {
		t.Fatalf("cache dir: %v", err)
	}

	targets, err := resetTargets()
	if err != nil {
		t.Fatalf("reset targets: %v", err)
	}

	want := []string{
		filepath.Join(cacheHome, "vectis"),
		filepath.Join(configDir, "vectis"),
		filepath.Join(dataDir, "vectis"),
		filepath.Join(deployDir, "podman"),
	}
	sort.Strings(want)

	if strings.Join(targets, "\n") != strings.Join(want, "\n") {
		t.Fatalf("targets mismatch\ngot:\n%s\nwant:\n%s", strings.Join(targets, "\n"), strings.Join(want, "\n"))
	}
}

func TestDeployPodmanInit_jsonOutput(t *testing.T) {
	withOutputFormat(t, outputJSON)
	t.Setenv(envDeployConfigDir, t.TempDir())

	secrets, created, err := loadOrCreatePodmanSecrets(false)
	if err != nil {
		t.Fatal(err)
	}

	path, err := podmanSecretsPath()
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := writeJSON(&buf, podmanCommandResult{
		Status:         "initialized",
		SecretsPath:    path,
		SecretsCreated: created,
		BootstrapToken: secrets.BootstrapToken != "",
	}); err != nil {
		t.Fatal(err)
	}

	var result podmanCommandResult
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("invalid JSON output: %v\n%s", err, buf.String())
	}

	if result.Status != "initialized" || result.SecretsPath != path || !result.BootstrapToken {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestDeployPodmanRender_jsonMetadataForFileOutput(t *testing.T) {
	withOutputFormat(t, outputJSON)
	t.Setenv(envDeployConfigDir, t.TempDir())

	out := filepath.Join(t.TempDir(), "rendered.yaml")
	oldOut := podmanRenderOut
	podmanRenderOut = out
	t.Cleanup(func() { podmanRenderOut = oldOut })

	manifest, _, created, err := renderPodmanManifest(false)
	if err != nil {
		t.Fatal(err)
	}

	if err := os.MkdirAll(filepath.Dir(out), 0o700); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(out, manifest, 0o600); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := writeJSON(&buf, podmanCommandResult{
		Status:         "rendered",
		ManifestPath:   out,
		SecretsCreated: created,
	}); err != nil {
		t.Fatal(err)
	}

	var result podmanCommandResult
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("invalid JSON output: %v\n%s", err, buf.String())
	}

	if result.Status != "rendered" || result.ManifestPath != out {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestDeployPodmanRender_HAProfileAddsReplicaTopology(t *testing.T) {
	t.Setenv(envDeployConfigDir, t.TempDir())

	oldProfile := podmanProfile
	oldKubeSpec := podmanKubeSpec
	podmanProfile = podmanProfileHA
	podmanKubeSpec = defaultPodmanKubeSpec
	t.Cleanup(func() {
		podmanProfile = oldProfile
		podmanKubeSpec = oldKubeSpec
	})

	manifestBytes, _, _, err := renderPodmanManifest(false)
	if err != nil {
		t.Fatal(err)
	}

	docs := decodeYAMLDocuments(t, manifestBytes)
	pod := findYAMLDocument(t, docs, "Pod", "vectis")
	tlsEnv := configMapData(t, docs, "vectis-grpc-tls-env")
	if got, want := tlsEnv["VECTIS_DISCOVERY_REGISTRY_ADDRESSES"], "127.0.0.1:8082,127.0.0.1:8182,127.0.0.1:8282"; got != want {
		t.Fatalf("registry addresses = %q, want %q", got, want)
	}

	requireHostPort(t, findContainer(t, pod, "api-2"), 8180)
	assertEnv(t, findContainer(t, pod, "queue-2"), "VECTIS_QUEUE_INSTANCE_ID", "queue-2")
	assertEnv(t, findContainer(t, pod, "queue-2"), "VECTIS_QUEUE_PERSISTENCE_DIR", "/data/vectis/queue/local-ha/queue-2")
	assertEnv(t, findContainer(t, pod, "log-2"), "VECTIS_LOG_STORAGE_DIR", "/data/vectis/jobs/log-2")
	assertEnv(t, findContainer(t, pod, "worker-2"), "VECTIS_WORKER_METRICS_PORT", "9182")
	assertEnv(t, findContainer(t, pod, "cron-2"), "VECTIS_CRON_INSTANCE_ID", "cron-2")
	assertEnv(t, findContainer(t, pod, "reconciler-2"), "VECTIS_RECONCILER_METRICS_PORT", "9185")
	findContainer(t, pod, "registry-3")

	if _, ok := envMap(t, findContainer(t, pod, "worker"))["VECTIS_CRON_INSTANCE_ID"]; ok {
		t.Fatalf("base worker inherited cron instance env")
	}

	assertStringSlice(t, prometheusTargets(t, docs, "vectis-queue"), []string{"127.0.0.1:9081", "127.0.0.1:9181"})
}

func TestDeployPodmanRender_SimpleProfileKeepsSingleReplicaTopology(t *testing.T) {
	t.Setenv(envDeployConfigDir, t.TempDir())

	oldProfile := podmanProfile
	oldKubeSpec := podmanKubeSpec
	podmanProfile = podmanProfileSimple
	podmanKubeSpec = defaultPodmanKubeSpec
	t.Cleanup(func() {
		podmanProfile = oldProfile
		podmanKubeSpec = oldKubeSpec
	})

	manifestBytes, _, _, err := renderPodmanManifest(false)
	if err != nil {
		t.Fatal(err)
	}

	docs := decodeYAMLDocuments(t, manifestBytes)
	pod := findYAMLDocument(t, docs, "Pod", "vectis")
	tlsEnv := configMapData(t, docs, "vectis-grpc-tls-env")
	if _, ok := tlsEnv["VECTIS_DISCOVERY_REGISTRY_ADDRESSES"]; ok {
		t.Fatalf("simple manifest unexpectedly included discovery registry addresses")
	}

	for _, name := range []string{"registry-2", "api-2", "queue-2", "log-2", "worker-2"} {
		if findContainerOK(t, pod, name) {
			t.Fatalf("simple manifest unexpectedly included container %s", name)
		}
	}

	assertStringSlice(t, prometheusTargets(t, docs, "vectis-queue"), []string{"127.0.0.1:9081"})
}

func TestDeployPodmanRender_InvalidProfileFails(t *testing.T) {
	oldProfile := podmanProfile
	podmanProfile = "weird"
	t.Cleanup(func() { podmanProfile = oldProfile })

	if _, _, _, err := renderPodmanManifest(false); err == nil {
		t.Fatalf("expected invalid profile error")
	}
}

func decodeYAMLDocuments(t *testing.T, manifest []byte) []map[string]any {
	t.Helper()

	dec := yaml.NewDecoder(bytes.NewReader(manifest))
	var docs []map[string]any
	for {
		var doc map[string]any
		err := dec.Decode(&doc)
		if err == io.EOF {
			break
		}

		if err != nil {
			t.Fatalf("decode YAML manifest: %v", err)
		}

		if len(doc) > 0 {
			docs = append(docs, doc)
		}
	}

	return docs
}

func findYAMLDocument(t *testing.T, docs []map[string]any, kind, name string) map[string]any {
	t.Helper()

	for _, doc := range docs {
		if stringValue(doc["kind"]) != kind {
			continue
		}

		metadata := mapValue(t, doc["metadata"])
		if stringValue(metadata["name"]) == name {
			return doc
		}
	}

	t.Fatalf("YAML document %s/%s not found", kind, name)
	return nil
}

func configMapData(t *testing.T, docs []map[string]any, name string) map[string]string {
	t.Helper()

	doc := findYAMLDocument(t, docs, "ConfigMap", name)
	data := mapValue(t, doc["data"])
	out := make(map[string]string, len(data))
	for key, value := range data {
		out[key] = stringValue(value)
	}

	return out
}

func findContainer(t *testing.T, pod map[string]any, name string) map[string]any {
	t.Helper()

	if container, ok := lookupContainer(pod, name); ok {
		return container
	}

	t.Fatalf("container %s not found", name)
	return nil
}

func findContainerOK(t *testing.T, pod map[string]any, name string) bool {
	t.Helper()

	_, ok := lookupContainer(pod, name)
	return ok
}

func lookupContainer(pod map[string]any, name string) (map[string]any, bool) {
	spec, ok := pod["spec"].(map[string]any)
	if !ok {
		return nil, false
	}

	containers, ok := spec["containers"].([]any)
	if !ok {
		return nil, false
	}

	for _, raw := range containers {
		container, ok := raw.(map[string]any)
		if ok && stringValue(container["name"]) == name {
			return container, true
		}
	}

	return nil, false
}

func envMap(t *testing.T, container map[string]any) map[string]string {
	t.Helper()

	out := map[string]string{}
	rawEnv, ok := container["env"]
	if !ok {
		return out
	}

	for _, raw := range sliceValue(t, rawEnv) {
		item := mapValue(t, raw)
		name := stringValue(item["name"])
		if name == "" {
			continue
		}

		if value, ok := item["value"]; ok {
			out[name] = stringValue(value)
		}
	}

	return out
}

func assertEnv(t *testing.T, container map[string]any, name, want string) {
	t.Helper()

	if got := envMap(t, container)[name]; got != want {
		t.Fatalf("%s env %s = %q, want %q", stringValue(container["name"]), name, got, want)
	}
}

func requireHostPort(t *testing.T, container map[string]any, want int) {
	t.Helper()

	for _, raw := range sliceValue(t, container["ports"]) {
		port := mapValue(t, raw)
		if intValue(port["hostPort"]) == want {
			return
		}
	}

	t.Fatalf("%s missing hostPort %d", stringValue(container["name"]), want)
}

func prometheusTargets(t *testing.T, docs []map[string]any, jobName string) []string {
	t.Helper()

	data := configMapData(t, docs, "vectis-prometheus-config")
	var prom map[string]any
	if err := yaml.Unmarshal([]byte(data["prometheus.yml"]), &prom); err != nil {
		t.Fatalf("decode prometheus.yml: %v", err)
	}

	for _, rawJob := range sliceValue(t, prom["scrape_configs"]) {
		job := mapValue(t, rawJob)
		if stringValue(job["job_name"]) != jobName {
			continue
		}

		staticConfigs := sliceValue(t, job["static_configs"])
		if len(staticConfigs) == 0 {
			t.Fatalf("prometheus job %s has no static_configs", jobName)
		}

		targets := sliceValue(t, mapValue(t, staticConfigs[0])["targets"])
		out := make([]string, 0, len(targets))
		for _, target := range targets {
			out = append(out, stringValue(target))
		}

		return out
	}

	t.Fatalf("prometheus job %s not found", jobName)
	return nil
}

func assertStringSlice(t *testing.T, got, want []string) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("slice length = %d, want %d; got %#v", len(got), len(want), got)
	}

	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("slice[%d] = %q, want %q; got %#v", i, got[i], want[i], got)
		}
	}
}

func mapValue(t *testing.T, value any) map[string]any {
	t.Helper()

	m, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", value)
	}

	return m
}

func sliceValue(t *testing.T, value any) []any {
	t.Helper()

	s, ok := value.([]any)
	if !ok {
		t.Fatalf("expected slice, got %T", value)
	}

	return s
}

func stringValue(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case fmt.Stringer:
		return v.String()
	case nil:
		return ""
	default:
		return fmt.Sprint(v)
	}
}

func intValue(value any) int {
	switch v := value.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case uint64:
		return int(v)
	default:
		return 0
	}
}

// rewriteTransport rewrites all outgoing requests to a test server URL.
type rewriteTransport struct {
	testURL    string
	underlying http.RoundTripper
}

func (rt *rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.URL.Scheme = "http"
	req.URL.Host = strings.TrimPrefix(rt.testURL, "http://")
	return rt.underlying.RoundTrip(req)
}

func setupTestAPIClient(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	oldClient := apiHTTPClient
	apiHTTPClient = &http.Client{
		Transport: &rewriteTransport{testURL: srv.URL, underlying: http.DefaultTransport},
	}
	t.Cleanup(func() { apiHTTPClient = oldClient })

	return srv
}

func withOutputFormat(t *testing.T, format string) {
	t.Helper()
	oldFormat := cliOutputFormat
	cliOutputFormat = format
	t.Cleanup(func() { cliOutputFormat = oldFormat })
}

func TestTokenList_success(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/tokens" {
			t.Errorf("path=%s", r.URL.Path)
		}

		if auth := r.Header.Get("Authorization"); auth != "Bearer test-token" {
			t.Errorf("Authorization=%q", auth)
		}

		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"id": 1, "label": "prod", "expires_at": nil, "created_at": "2024-01-01", "last_used_at": nil},
		})
	})

	t.Setenv("VECTIS_API_TOKEN", "test-token")

	var buf bytes.Buffer
	if err := tokenList(&buf); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	if !strings.Contains(out, "prod") {
		t.Fatalf("expected output to contain 'prod', got: %s", out)
	}
}

func TestTokenList_jsonOutput(t *testing.T) {
	withOutputFormat(t, outputJSON)
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"id": 1, "label": "prod", "expires_at": nil, "created_at": "2024-01-01", "last_used_at": nil},
		})
	})

	var buf bytes.Buffer
	if err := tokenList(&buf); err != nil {
		t.Fatal(err)
	}

	var tokens []map[string]any
	if err := json.Unmarshal(buf.Bytes(), &tokens); err != nil {
		t.Fatalf("invalid JSON output: %v\n%s", err, buf.String())
	}

	if len(tokens) != 1 || tokens[0]["label"] != "prod" {
		t.Fatalf("unexpected JSON output: %#v", tokens)
	}
}

func TestTokenList_unexpectedStatus(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	if err := tokenList(io.Discard); err == nil {
		t.Fatal("expected error")
	}
}

func TestTokenCreate_success(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/tokens" {
			t.Errorf("path=%s", r.URL.Path)
		}

		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["label"] != "my-label" {
			t.Errorf("label=%v", body["label"])
		}

		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": 42, "label": "my-label", "token": "secret-token", "expires_at": "",
		})
	})

	var buf bytes.Buffer
	if err := tokenCreate("my-label", "never", 0, &buf); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	if !strings.Contains(out, "secret-token") {
		t.Fatalf("expected token in output, got: %s", out)
	}
}

func TestTokenCreate_forbidden(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	})

	if err := tokenCreate("x", "never", 0, io.Discard); err == nil {
		t.Fatal("expected error")
	}
}

func TestTokenDelete_success(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/tokens/7" {
			t.Errorf("path=%s", r.URL.Path)
		}

		w.WriteHeader(http.StatusNoContent)
	})

	if err := tokenDelete("7", io.Discard); err != nil {
		t.Fatal(err)
	}
}

func TestTokenDelete_notFound(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})

	if err := tokenDelete("99", io.Discard); err == nil {
		t.Fatal("expected error")
	}
}

func TestListJobNames_tableOutput(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/jobs" {
			t.Errorf("path=%s", r.URL.Path)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{
				{"name": "z-job", "namespace": "/prod"},
				{"name": "a-job"},
			},
		})
	})

	var buf bytes.Buffer
	if err := listJobNames(&buf, false, 0, 0); err != nil {
		t.Fatal(err)
	}

	if got, want := buf.String(), "NAME   NAMESPACE\na-job  -\nz-job  /prod\n"; got != want {
		t.Fatalf("output: want %q, got %q", want, got)
	}
}

func TestListJobNames_quietOutput(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{
				{"name": "z-job"},
				{"name": "a-job"},
			},
		})
	})

	var buf bytes.Buffer
	if err := listJobNames(&buf, true, 0, 0); err != nil {
		t.Fatal(err)
	}

	if got, want := buf.String(), "a-job\nz-job\n"; got != want {
		t.Fatalf("output: want %q, got %q", want, got)
	}
}

func TestListJobNames_jsonOutput(t *testing.T) {
	withOutputFormat(t, outputJSON)
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("cursor"); got != "7" {
			t.Errorf("cursor=%q, want 7", got)
		}

		if got := r.URL.Query().Get("limit"); got != "25" {
			t.Errorf("limit=%q, want 25", got)
		}

		next := int64(9)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{
				{"name": "z-job", "namespace": "/prod"},
				{"name": "a-job"},
			},
			"next_cursor": next,
		})
	})

	var buf bytes.Buffer
	if err := listJobNames(&buf, false, 7, 25); err != nil {
		t.Fatal(err)
	}

	var resp struct {
		Data []struct {
			Name      string `json:"name"`
			Namespace string `json:"namespace,omitempty"`
		} `json:"data"`
		NextCursor *int64 `json:"next_cursor,omitempty"`
	}

	if err := json.Unmarshal(buf.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON output: %v\n%s", err, buf.String())
	}

	names := []string{resp.Data[0].Name, resp.Data[1].Name}
	if got, want := strings.Join(names, ","), "a-job,z-job"; got != want {
		t.Fatalf("names: want %q, got %q", want, got)
	}

	if resp.NextCursor == nil || *resp.NextCursor != 9 {
		t.Fatalf("next_cursor: want 9, got %+v", resp.NextCursor)
	}
}

func TestListJobNames_rejectsUnexpectedShape(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"name": "legacy-shape"},
		})
	})

	if err := listJobNames(io.Discard, false, 0, 0); err == nil {
		t.Fatal("expected error")
	}
}

func TestNamespaceList_success(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/namespaces" {
			t.Errorf("path=%s", r.URL.Path)
		}

		parent := int64(1)
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"id": 2, "name": "team-a", "path": "/team-a", "parent_id": parent, "break_inheritance": false},
		})
	})

	var buf bytes.Buffer
	if err := namespaceList(&buf); err != nil {
		t.Fatal(err)
	}

	if got := buf.String(); !strings.Contains(got, "2\t/team-a\tname=team-a\tparent=1") {
		t.Fatalf("unexpected output: %s", got)
	}
}

func TestUserCreate_success(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/users" {
			t.Errorf("path=%s", r.URL.Path)
		}

		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["username"] != "alice" || body["password"] != "secret-password" {
			t.Errorf("unexpected body: %v", body)
		}

		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": 3, "username": "alice", "enabled": true, "created_at": "2026-05-09T00:00:00Z",
		})
	})

	var buf bytes.Buffer
	if err := userCreate("alice", "secret-password", &buf); err != nil {
		t.Fatal(err)
	}

	if got := buf.String(); !strings.Contains(got, "User created: 3 alice") {
		t.Fatalf("unexpected output: %s", got)
	}
}

func TestBindingDelete_escapesRole(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/namespaces/2/bindings/3" {
			t.Errorf("path=%s", r.URL.Path)
		}

		if role := r.URL.Query().Get("role"); role != "admin:*" {
			t.Errorf("role=%q", role)
		}

		w.WriteHeader(http.StatusNoContent)
	})

	if err := bindingDelete(2, 3, "admin:*"); err != nil {
		t.Fatal(err)
	}
}

func TestGetRun_success(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/runs/run-1" {
			t.Errorf("path=%s", r.URL.Path)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"run_id":         "run-1",
			"run_index":      3,
			"status":         "failed",
			"owning_cell":    "pdx-b",
			"failure_code":   "execution",
			"failure_reason": "exit code 1",
		})
	})

	var buf bytes.Buffer
	if err := getRun("run-1", &buf); err != nil {
		t.Fatal(err)
	}

	want := strings.Join([]string{
		"run_id=run-1",
		"run_index=3",
		"status=failed",
		"owning_cell=pdx-b",
		"failure_code=execution",
		"failure_reason=exit code 1",
		"",
	}, "\n")
	if got := buf.String(); got != want {
		t.Fatalf("output: want %q, got %q", want, got)
	}
}

func TestGetRun_successIncludesAuditFields(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/runs/run-1" {
			t.Errorf("path=%s", r.URL.Path)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"run_id":                 "run-1",
			"run_index":              3,
			"status":                 "queued",
			"owning_cell":            "pdx-b",
			"definition_version":     4,
			"definition_hash":        "sha256:def",
			"trigger_invocation_id":  "inv-1",
			"trigger_type":           "manual",
			"trigger_payload_hash":   "sha256:trigger",
			"requested_cells":        []string{"iad-a", "pdx-b"},
			"execution_payload_hash": "sha256:payload",
		})
	})

	var buf bytes.Buffer
	if err := getRun("run-1", &buf); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{
		"definition_version=4",
		"definition_hash=sha256:def",
		"trigger_invocation_id=inv-1",
		"trigger_type=manual",
		"trigger_payload_hash=sha256:trigger",
		"requested_cells=iad-a,pdx-b",
		"execution_payload_hash=sha256:payload",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestGetRun_successIncludesTaskDispatch(t *testing.T) {
	lastAttempt := int64(1_000_000_000)
	errMsg := "queue unavailable"
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/runs/run-1" {
			t.Errorf("path=%s", r.URL.Path)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"run_id":      "run-1",
			"run_index":   3,
			"status":      "queued",
			"owning_cell": "pdx-b",
			"task_completion": map[string]any{
				"total":           3,
				"succeeded":       1,
				"terminal_failed": 1,
				"incomplete":      1,
			},
			"task_dispatch": map[string]any{
				"total":     2,
				"pending":   1,
				"failed":    1,
				"enqueued":  0,
				"truncated": true,
				"limit":     1,
				"intents": []map[string]any{
					{
						"execution_id":            "exec-1",
						"task_id":                 "task-1",
						"task_attempt_id":         "attempt-1",
						"cell_id":                 "iad-a",
						"state":                   "failed_pending",
						"enqueue_attempts":        2,
						"last_enqueue_attempt_at": lastAttempt,
						"last_enqueue_error":      errMsg,
						"created_at":              lastAttempt,
						"updated_at":              lastAttempt,
					},
				},
			},
		})
	})

	var buf bytes.Buffer
	if err := getRun("run-1", &buf); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{
		"task_completion: total=3 succeeded=1 terminal_failed=1 incomplete=1",
		"task_dispatch: total=2 pending=1 failed=1 enqueued=0 truncated=true limit=1",
		`failed_pending execution=exec-1 task=task-1 attempt=attempt-1 cell=iad-a enqueue_attempts=2 last_attempt=1970-01-01T00:00:01Z error="queue unavailable"`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestGetRun_jsonOutput(t *testing.T) {
	withOutputFormat(t, outputJSON)
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"run_id":                 "run-1",
			"run_index":              3,
			"status":                 "failed",
			"owning_cell":            "pdx-b",
			"execution_payload_hash": "sha256:payload",
		})
	})

	var buf bytes.Buffer
	if err := getRun("run-1", &buf); err != nil {
		t.Fatal(err)
	}

	var run map[string]any
	if err := json.Unmarshal(buf.Bytes(), &run); err != nil {
		t.Fatalf("invalid JSON output: %v\n%s", err, buf.String())
	}

	if run["run_id"] != "run-1" || run["status"] != "failed" || run["owning_cell"] != "pdx-b" || run["execution_payload_hash"] != "sha256:payload" {
		t.Fatalf("unexpected JSON output: %#v", run)
	}
}

func TestGetRun_jsonOutputIncludesTaskCompletion(t *testing.T) {
	withOutputFormat(t, outputJSON)
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"run_id":      "run-1",
			"run_index":   3,
			"status":      "failed",
			"owning_cell": "pdx-b",
			"task_completion": map[string]any{
				"total":           4,
				"succeeded":       2,
				"terminal_failed": 1,
				"incomplete":      1,
			},
		})
	})

	var buf bytes.Buffer
	if err := getRun("run-1", &buf); err != nil {
		t.Fatal(err)
	}

	var run runDetail
	if err := json.Unmarshal(buf.Bytes(), &run); err != nil {
		t.Fatalf("invalid JSON output: %v\n%s", err, buf.String())
	}

	if run.TaskCompletion == nil || run.TaskCompletion.Total != 4 || run.TaskCompletion.Succeeded != 2 || run.TaskCompletion.TerminalFailed != 1 || run.TaskCompletion.Incomplete != 1 {
		t.Fatalf("unexpected task completion JSON: %+v", run.TaskCompletion)
	}
}

func TestGetRunExecutionPayload_success(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/runs/run-1/execution-payload" {
			t.Errorf("path=%s", r.URL.Path)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"run_id":          "run-1",
			"payload_hash":    "sha256:payload",
			"definition_hash": "sha256:def",
			"payload": map[string]any{
				"job": map[string]any{
					"id":    "job-1",
					"runId": "run-1",
				},
			},
		})
	})

	var buf bytes.Buffer
	if err := getRunExecutionPayload("run-1", &buf); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{"run_id=run-1", "payload_hash=sha256:payload", "definition_hash=sha256:def", `"runId": "run-1"`} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestGetRunExecutionPayload_jsonOutput(t *testing.T) {
	withOutputFormat(t, outputJSON)
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"run_id":       "run-1",
			"payload_hash": "sha256:payload",
			"payload": map[string]any{
				"job": map[string]any{"id": "job-1"},
			},
		})
	})

	var buf bytes.Buffer
	if err := getRunExecutionPayload("run-1", &buf); err != nil {
		t.Fatal(err)
	}

	var result runExecutionPayloadResult
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("invalid JSON output: %v\n%s", err, buf.String())
	}

	if result.RunID != "run-1" || result.PayloadHash != "sha256:payload" || !strings.Contains(string(result.Payload), "job-1") {
		t.Fatalf("unexpected payload JSON: %+v payload=%s", result, string(result.Payload))
	}
}

func TestGetRunExecutionPayload_notFound(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})

	if err := getRunExecutionPayload("missing", io.Discard); err == nil {
		t.Fatal("expected error")
	}
}

func TestGetRunTasks_success(t *testing.T) {
	lastObserved := int64(1_000_000_000)
	next := int64(17)
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/runs/run-1/tasks" {
			t.Errorf("path=%s", r.URL.Path)
		}

		if got := r.URL.Query().Get("limit"); got != "2" {
			t.Errorf("limit=%q, want 2", got)
		}

		if got := r.URL.Query().Get("cursor"); got != "9" {
			t.Errorf("cursor=%q, want 9", got)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{
				{
					"task_id":  "run-1:root",
					"run_id":   "run-1",
					"task_key": "root",
					"name":     "Root",
					"status":   "succeeded",
					"attempts": []map[string]any{
						{
							"attempt_id":       "attempt-root",
							"task_id":          "run-1:root",
							"run_id":           "run-1",
							"cell_id":          "local",
							"attempt":          1,
							"status":           "succeeded",
							"finished_at":      "2026-06-01T12:00:00Z",
							"last_observed_at": lastObserved,
							"event_sequence":   3,
						},
					},
				},
				{
					"task_id":        "run-1:child",
					"run_id":         "run-1",
					"parent_task_id": "run-1:root",
					"task_key":       "child",
					"name":           "Child",
					"status":         "running",
					"spec_hash":      "sha256:child",
					"attempts": []map[string]any{
						{
							"attempt_id":     "attempt-child",
							"task_id":        "run-1:child",
							"run_id":         "run-1",
							"cell_id":        "pdx-b",
							"attempt":        1,
							"status":         "running",
							"accepted_at":    "2026-06-01T12:01:00Z",
							"started_at":     "2026-06-01T12:01:01Z",
							"event_sequence": 2,
						},
					},
				},
			},
			"next_cursor": next,
		})
	})

	var buf bytes.Buffer
	if err := getRunTasks("run-1", 2, 9, &buf); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{
		"task_id=run-1:root parent=- key=root name=Root status=succeeded attempts=1",
		"attempt=1 id=attempt-root cell=local status=succeeded event_sequence=3 finished_at=2026-06-01T12:00:00Z last_observed_at=1970-01-01T00:00:01Z",
		"task_id=run-1:child parent=run-1:root key=child name=Child status=running attempts=1 spec_hash=sha256:child",
		"attempt=1 id=attempt-child cell=pdx-b status=running event_sequence=2 accepted_at=2026-06-01T12:01:00Z started_at=2026-06-01T12:01:01Z",
		"More tasks available. Continue with --cursor 17.",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestGetRunTasks_jsonOutput(t *testing.T) {
	withOutputFormat(t, outputJSON)
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{
				{
					"task_id":  "run-1:root",
					"run_id":   "run-1",
					"task_key": "root",
					"name":     "Root",
					"status":   "succeeded",
					"attempts": []map[string]any{},
				},
			},
		})
	})

	var buf bytes.Buffer
	if err := getRunTasks("run-1", 0, 0, &buf); err != nil {
		t.Fatal(err)
	}

	var result runTasksResult
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("invalid JSON output: %v\n%s", err, buf.String())
	}

	if len(result.Data) != 1 || result.Data[0].TaskID != "run-1:root" || result.Data[0].Status != "succeeded" {
		t.Fatalf("unexpected tasks JSON: %+v", result)
	}
}

func TestGetRunTasks_notFound(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})

	if err := getRunTasks("missing", 0, 0, io.Discard); err == nil {
		t.Fatal("expected error")
	}
}

func TestReplayRun_success(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/runs/run-1/replay" {
			t.Errorf("path=%s", r.URL.Path)
		}

		if got := r.Header.Get("Idempotency-Key"); got != "idem-1" {
			t.Errorf("idempotency key=%q", got)
		}

		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		if body["cell_id"] != "pdx-b" {
			t.Fatalf("cell_id=%q", body["cell_id"])
		}

		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"job_id":           "job-1",
			"run_id":           "run-2",
			"run_index":        8,
			"cell_id":          "pdx-b",
			"replay_of_run_id": "run-1",
		})
	})

	var buf bytes.Buffer
	if err := replayRun("run-1", "pdx-b", "idem-1", &buf); err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{"replay_of_run_id=run-1", "run_id=run-2", "run_index=8", "job_id=job-1", "cell_id=pdx-b"} {
		if !strings.Contains(buf.String(), want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, buf.String())
		}
	}
}

func TestReplayRun_jsonOutput(t *testing.T) {
	withOutputFormat(t, outputJSON)
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"job_id":           "job-1",
			"run_id":           "run-2",
			"run_index":        8,
			"replay_of_run_id": "run-1",
		})
	})

	var buf bytes.Buffer
	if err := replayRun("run-1", "", "", &buf); err != nil {
		t.Fatal(err)
	}

	var result runReplayResult
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("invalid JSON output: %v\n%s", err, buf.String())
	}

	if result.RunID != "run-2" || result.ReplayOfRunID != "run-1" || result.RunIndex != 8 {
		t.Fatalf("unexpected replay JSON: %+v", result)
	}
}

func TestReplayRun_conflict(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
	})

	if err := replayRun("run-active", "", "", io.Discard); err == nil {
		t.Fatal("expected conflict error")
	}
}

func TestGetRun_notFound(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})

	if err := getRun("missing", io.Discard); err == nil {
		t.Fatal("expected error")
	}
}

func TestListRuns_sinceDateUsesSinceQuery(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("since"); got != "2026-05-15" {
			t.Errorf("since=%q, want 2026-05-15", got)
		}

		if got := r.URL.Query().Get("cursor"); got != "42" {
			t.Errorf("cursor=%q, want 42", got)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{}})
	})

	if err := listRuns("job-1", 0, 42, "2026-05-15", "", io.Discard); err != nil {
		t.Fatal(err)
	}
}

func TestListRuns_cellFilterAndTableOutput(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/jobs/job-1/runs" {
			t.Errorf("path=%s", r.URL.Path)
		}

		if got := r.URL.Query().Get("cell_id"); got != "pdx-b" {
			t.Errorf("cell_id=%q, want pdx-b", got)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{
				{
					"run_id":      "run-pdx",
					"run_index":   2,
					"status":      "queued",
					"owning_cell": "pdx-b",
				},
				{
					"run_id":    "run-local",
					"run_index": 3,
					"status":    "succeeded",
				},
			},
		})
	})

	var buf bytes.Buffer
	if err := listRuns("job-1", 0, 0, "", "pdx-b", &buf); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{"RUN ID", "INDEX", "CELL", "STATUS", "run-pdx", "pdx-b", "run-local"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestListRuns_jsonOutputIncludesOwningCell(t *testing.T) {
	withOutputFormat(t, outputJSON)
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{
				{
					"run_id":                 "run-pdx",
					"run_index":              2,
					"status":                 "queued",
					"owning_cell":            "pdx-b",
					"execution_payload_hash": "sha256:payload",
				},
			},
		})
	})

	var buf bytes.Buffer
	if err := listRuns("job-1", 0, 0, "", "", &buf); err != nil {
		t.Fatal(err)
	}

	var result runListResult
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("invalid JSON output: %v\n%s", err, buf.String())
	}

	if len(result.Data) != 1 || result.Data[0].OwningCell != "pdx-b" || result.Data[0].ExecutionPayloadHash != "sha256:payload" {
		t.Fatalf("unexpected runs JSON: %+v", result)
	}
}

func TestDecodeJobRuns_paginatedResponse(t *testing.T) {
	runs, err := decodeJobRuns(strings.NewReader(`{"data":[{"run_id":"run-1","run_index":1}]}`))
	if err != nil {
		t.Fatal(err)
	}

	if len(runs) != 1 || runs[0].RunID != "run-1" || runs[0].RunIndex != 1 {
		t.Fatalf("unexpected runs: %+v", runs)
	}
}

func TestLatestRunForJob_paginatesToNewestRun(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/jobs/job-1/runs" {
			t.Errorf("path=%s", r.URL.Path)
		}

		switch r.URL.Query().Get("cursor") {
		case "":
			next := int64(10)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data":        []map[string]any{{"run_id": "run-1", "run_index": 1}},
				"next_cursor": next,
			})
		case "10":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{{"run_id": "run-2", "run_index": 2}},
			})
		default:
			t.Errorf("unexpected cursor=%q", r.URL.Query().Get("cursor"))
			w.WriteHeader(http.StatusBadRequest)
		}
	})

	run, ok, err := latestRunForJob("job-1")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected latest run")
	}
	if run.RunID != "run-2" || run.RunIndex != 2 {
		t.Fatalf("unexpected latest run: %+v", run)
	}
}

func TestRunLogStream_allowsQuietStreamPastAPIClientTimeout(t *testing.T) {
	oldClient := apiHTTPClient
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/runs/run-1/logs" {
			t.Errorf("path=%s", r.URL.Path)
		}

		if got := r.Header.Get("Accept"); got != "text/event-stream" {
			t.Errorf("Accept=%q, want text/event-stream", got)
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}

		time.Sleep(75 * time.Millisecond)
		entry := LogEntry{
			Stream: int(api.Stream_STREAM_CONTROL.Number()),
			Data:   `{"event":"completed","status":"success"}`,
		}

		b, err := json.Marshal(entry)
		if err != nil {
			t.Fatal(err)
		}

		fmt.Fprintf(w, "data: %s\n\n", b)
	}))

	t.Cleanup(srv.Close)
	apiHTTPClient = &http.Client{
		Timeout:   20 * time.Millisecond,
		Transport: &rewriteTransport{testURL: srv.URL, underlying: http.DefaultTransport},
	}
	t.Cleanup(func() { apiHTTPClient = oldClient })

	if err := runLogStream("run-1", false, false); err != nil {
		t.Fatal(err)
	}
}

func TestCancelRun_success(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/runs/run-1/cancel" {
			t.Errorf("path=%s", r.URL.Path)
		}

		w.WriteHeader(http.StatusNoContent)
	})

	var buf bytes.Buffer
	if err := cancelRun("run-1", &buf); err != nil {
		t.Fatal(err)
	}

	if got := buf.String(); !strings.Contains(got, "Run run-1 cancel requested.") {
		t.Fatalf("unexpected output: %s", got)
	}
}

func TestCancelRun_acceptedPending(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	})

	var buf bytes.Buffer
	if err := cancelRun("run-1", &buf); err != nil {
		t.Fatal(err)
	}

	if got := buf.String(); !strings.Contains(got, "Run run-1 cancel requested.") {
		t.Fatalf("unexpected output: %s", got)
	}
}

func TestCancelRun_conflict(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
	})

	if err := cancelRun("done", io.Discard); err == nil {
		t.Fatal("expected error")
	}
}

func TestMarkRunForRepair_success(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/runs/run-1/repair/mark-failed" {
			t.Errorf("path=%s", r.URL.Path)
		}

		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}

		if body["reason"] != "worker exited" {
			t.Errorf("reason=%q", body["reason"])
		}

		w.WriteHeader(http.StatusNoContent)
	})

	var buf bytes.Buffer
	if err := markRunForRepair("run-1", "failed", "worker exited", &buf); err != nil {
		t.Fatal(err)
	}

	if got := buf.String(); !strings.Contains(got, "Repair recorded: run run-1 marked failed.") {
		t.Fatalf("unexpected output: %s", got)
	}
}

func TestMarkRunForRepair_jsonOutput(t *testing.T) {
	withOutputFormat(t, outputJSON)
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	var buf bytes.Buffer
	if err := markRunForRepair("run-1", "queued", "", &buf); err != nil {
		t.Fatal(err)
	}

	var result runRepairResult
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("invalid JSON output: %v\n%s", err, buf.String())
	}

	if result.Status != "marked" || result.RunID != "run-1" || result.State != "queued" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestMarkRunForRepair_conflict(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
	})

	if err := markRunForRepair("done", "queued", "", io.Discard); err == nil {
		t.Fatal("expected error")
	}
}

func TestDoLogin_success(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/login" {
			t.Errorf("path=%s", r.URL.Path)
		}

		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["username"] != "admin" || body["password"] != "secret" {
			t.Errorf("unexpected body: %v", body)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"token": "login-token", "user_id": 1, "expires_at": "2025-01-01",
		})
	})

	token, err := doLogin("admin", "secret")
	if err != nil {
		t.Fatal(err)
	}

	if token != "login-token" {
		t.Fatalf("expected login-token, got %s", token)
	}
}

func TestDoLogin_unauthorized(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	})

	_, err := doLogin("admin", "wrong")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDoLogin_serviceUnavailable(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "not ready"})
	})

	_, err := doLogin("admin", "secret")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDoLogin_unexpectedStatus(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})

	_, err := doLogin("admin", "secret")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDoctor_success(t *testing.T) {
	seen := map[string]int{}
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		seen[r.URL.Path]++
		switch r.URL.Path {
		case "/health/live", "/health/ready":
			w.WriteHeader(http.StatusOK)
		case "/api/v1/setup/status":
			_ = json.NewEncoder(w).Encode(map[string]bool{"setup_complete": true, "auth_enabled": true})
		case "/api/v1/schema/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"current_version": 5, "has_schema": true})
		case "/api/v1/reconciler/heartbeat":
			_ = json.NewEncoder(w).Encode(map[string]any{"active": true, "last_activity_unix": 1715000000})
		case "/api/v1/audit/drops":
			_ = json.NewEncoder(w).Encode(map[string]any{"dropped": 0})
		case "/api/v1/db/pool-stats":
			_ = json.NewEncoder(w).Encode(map[string]any{"open_connections": 3, "in_use": 1, "wait_count": 0})
		case "/api/v1/queue/backlog":
			_ = json.NewEncoder(w).Encode(map[string]any{"queued": 0})
		case "/api/v1/reconciler/stuck-runs":
			_ = json.NewEncoder(w).Encode(map[string]any{"stuck": 0})
		case "/api/v1/cells/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"cells": []map[string]any{}})
		case "/api/v1/log/reachable":
			_ = json.NewEncoder(w).Encode(map[string]any{"reachable": true, "state": "READY"})
		case "/api/v1/audit/flush-failures":
			_ = json.NewEncoder(w).Encode(map[string]any{"flush_failures": 0})
		case "/api/v1/catalog/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"pending": 0, "applied": 4, "failed": 0, "total": 4})
		default:
			t.Errorf("unexpected path=%s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	})

	t.Setenv("VECTIS_API_TOKEN", "test-token")

	var buf bytes.Buffer
	if err := doctor(&buf); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{
		"Vectis health check",
		"Overall: PASS  18 passed, 0 warnings, 0 failed",
		"Core",
		"OK    API liveness",
		"OK    API readiness",
		"OK    Initial setup",
		"OK    CLI token",
		"Database",
		"OK    Schema",
		"OK    Connection pool",
		"Queue",
		"OK    Backlog",
		"OK    Persistence filesystem",
		"Reconciler",
		"OK    Recovery activity",
		"reconciler recovery activity recorded",
		"OK    Stuck runs",
		"Cells",
		"OK    Ingress routes",
		"Catalog",
		"OK    Cell event inbox",
		"catalog inbox ok: 0 pending",
		"Logging",
		"OK    Log service",
		"OK    Log storage",
		"OK    Forwarder spool",
		"Audit",
		"OK    Recent drops",
		"OK    Flush failures",
		"TLS",
		"OK    Files",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in output:\n%s", want, out)
		}
	}

	for _, path := range []string{"/health/live", "/health/ready", "/api/v1/setup/status", "/api/v1/schema/status", "/api/v1/reconciler/heartbeat", "/api/v1/audit/drops", "/api/v1/db/pool-stats", "/api/v1/queue/backlog", "/api/v1/reconciler/stuck-runs", "/api/v1/cells/status", "/api/v1/log/reachable", "/api/v1/audit/flush-failures", "/api/v1/catalog/status"} {
		if seen[path] != 1 {
			t.Fatalf("expected one request to %s, got %d", path, seen[path])
		}
	}
}

func TestDoctor_warnsForIncompleteSetupAndMissingToken(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health/live", "/health/ready":
			w.WriteHeader(http.StatusOK)
		case "/api/v1/setup/status":
			_ = json.NewEncoder(w).Encode(map[string]bool{"setup_complete": false, "auth_enabled": true})
		case "/api/v1/schema/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"current_version": 5, "has_schema": true})
		case "/api/v1/reconciler/heartbeat":
			_ = json.NewEncoder(w).Encode(map[string]any{"active": true, "last_activity_unix": 1715000000})
		case "/api/v1/audit/drops":
			_ = json.NewEncoder(w).Encode(map[string]any{"dropped": 0})
		case "/api/v1/db/pool-stats":
			_ = json.NewEncoder(w).Encode(map[string]any{"open_connections": 3, "in_use": 1, "wait_count": 0})
		case "/api/v1/queue/backlog":
			_ = json.NewEncoder(w).Encode(map[string]any{"queued": 0})
		case "/api/v1/reconciler/stuck-runs":
			_ = json.NewEncoder(w).Encode(map[string]any{"stuck": 0})
		case "/api/v1/cells/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"cells": []map[string]any{}})
		case "/api/v1/log/reachable":
			_ = json.NewEncoder(w).Encode(map[string]any{"reachable": true, "state": "READY"})
		case "/api/v1/audit/flush-failures":
			_ = json.NewEncoder(w).Encode(map[string]any{"flush_failures": 0})
		case "/api/v1/catalog/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"pending": 0, "applied": 4, "failed": 0, "total": 4})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})

	t.Setenv("VECTIS_API_TOKEN", "")
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("XDG_CONFIG_HOME", tmpDir)

	var buf bytes.Buffer
	if err := doctor(&buf); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{
		"Overall: WARN  16 passed, 2 warnings, 0 failed",
		"WARN  Initial setup",
		"initial setup is not complete",
		"WARN  CLI token",
		"no CLI API token configured",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in output:\n%s", want, out)
		}
	}
}

func TestDoctor_setupAndTokenPassWhenAuthDisabled(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health/live", "/health/ready":
			w.WriteHeader(http.StatusOK)
		case "/api/v1/setup/status":
			_ = json.NewEncoder(w).Encode(map[string]bool{"setup_complete": false, "auth_enabled": false})
		case "/api/v1/schema/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"current_version": 5, "has_schema": true})
		case "/api/v1/reconciler/heartbeat":
			_ = json.NewEncoder(w).Encode(map[string]any{"active": false})
		case "/api/v1/audit/drops":
			_ = json.NewEncoder(w).Encode(map[string]any{"dropped": 0})
		case "/api/v1/db/pool-stats":
			_ = json.NewEncoder(w).Encode(map[string]any{"open_connections": 3, "in_use": 1, "wait_count": 0})
		case "/api/v1/queue/backlog":
			_ = json.NewEncoder(w).Encode(map[string]any{"queued": 0})
		case "/api/v1/reconciler/stuck-runs":
			_ = json.NewEncoder(w).Encode(map[string]any{"stuck": 0})
		case "/api/v1/cells/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"cells": []map[string]any{}})
		case "/api/v1/log/reachable":
			_ = json.NewEncoder(w).Encode(map[string]any{"reachable": true, "state": "READY"})
		case "/api/v1/audit/flush-failures":
			_ = json.NewEncoder(w).Encode(map[string]any{"flush_failures": 0})
		case "/api/v1/catalog/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"pending": 0, "applied": 4, "failed": 0, "total": 4})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})

	t.Setenv("VECTIS_API_TOKEN", "")
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("XDG_CONFIG_HOME", tmpDir)

	var buf bytes.Buffer
	if err := doctor(&buf); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{
		"Overall: PASS  18 passed, 0 warnings, 0 failed",
		"initial setup not required; API auth is disabled",
		"CLI API token not required; API auth is disabled",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in output:\n%s", want, out)
		}
	}
}

func TestDoctor_failsWhenRequiredCheckFails(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health/live":
			w.WriteHeader(http.StatusOK)
		case "/health/ready":
			w.WriteHeader(http.StatusServiceUnavailable)
		case "/api/v1/setup/status":
			_ = json.NewEncoder(w).Encode(map[string]bool{"setup_complete": true, "auth_enabled": true})
		case "/api/v1/schema/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"current_version": 5, "has_schema": true})
		case "/api/v1/reconciler/heartbeat":
			_ = json.NewEncoder(w).Encode(map[string]any{"active": true, "last_activity_unix": 1715000000})
		case "/api/v1/audit/drops":
			_ = json.NewEncoder(w).Encode(map[string]any{"dropped": 0})
		case "/api/v1/db/pool-stats":
			_ = json.NewEncoder(w).Encode(map[string]any{"open_connections": 3, "in_use": 1, "wait_count": 0})
		case "/api/v1/queue/backlog":
			_ = json.NewEncoder(w).Encode(map[string]any{"queued": 0})
		case "/api/v1/reconciler/stuck-runs":
			_ = json.NewEncoder(w).Encode(map[string]any{"stuck": 0})
		case "/api/v1/cells/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"cells": []map[string]any{}})
		case "/api/v1/log/reachable":
			_ = json.NewEncoder(w).Encode(map[string]any{"reachable": true, "state": "READY"})
		case "/api/v1/audit/flush-failures":
			_ = json.NewEncoder(w).Encode(map[string]any{"flush_failures": 0})
		case "/api/v1/catalog/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"pending": 0, "applied": 4, "failed": 0, "total": 4})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})

	t.Setenv("VECTIS_API_TOKEN", "test-token")

	var buf bytes.Buffer
	err := doctor(&buf)
	if err == nil {
		t.Fatal("expected error")
	}

	out := buf.String()
	if !strings.Contains(out, "Overall: FAIL  17 passed, 0 warnings, 1 failed") ||
		!strings.Contains(out, "FAIL  API readiness") ||
		!strings.Contains(out, "unexpected status: 503 Service Unavailable") {
		t.Fatalf("missing readiness failure in output:\n%s", out)
	}
}

func TestDoctor_jsonOutput(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health/live", "/health/ready":
			w.WriteHeader(http.StatusOK)
		case "/api/v1/setup/status":
			_ = json.NewEncoder(w).Encode(map[string]bool{"setup_complete": true, "auth_enabled": true})
		case "/api/v1/schema/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"current_version": 5, "has_schema": true})
		case "/api/v1/reconciler/heartbeat":
			_ = json.NewEncoder(w).Encode(map[string]any{"active": true, "last_activity_unix": 1715000000})
		case "/api/v1/audit/drops":
			_ = json.NewEncoder(w).Encode(map[string]any{"dropped": 0})
		case "/api/v1/db/pool-stats":
			_ = json.NewEncoder(w).Encode(map[string]any{"open_connections": 3, "in_use": 1, "wait_count": 0})
		case "/api/v1/queue/backlog":
			_ = json.NewEncoder(w).Encode(map[string]any{"queued": 0})
		case "/api/v1/reconciler/stuck-runs":
			_ = json.NewEncoder(w).Encode(map[string]any{"stuck": 0})
		case "/api/v1/cells/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"cells": []map[string]any{}})
		case "/api/v1/log/reachable":
			_ = json.NewEncoder(w).Encode(map[string]any{"reachable": true, "state": "READY"})
		case "/api/v1/audit/flush-failures":
			_ = json.NewEncoder(w).Encode(map[string]any{"flush_failures": 0})
		case "/api/v1/catalog/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"pending": 0, "applied": 4, "failed": 0, "total": 4})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})

	t.Setenv("VECTIS_API_TOKEN", "test-token")
	doctorJSON = true
	defer func() { doctorJSON = false }()

	var buf bytes.Buffer
	if err := doctor(&buf); err != nil {
		t.Fatal(err)
	}

	var report doctorReport
	if err := json.Unmarshal(buf.Bytes(), &report); err != nil {
		t.Fatalf("invalid JSON output: %v\n%s", err, buf.String())
	}

	if report.Status != doctorOK || report.Passed != 18 || report.Warnings != 0 || report.Failed != 0 {
		t.Fatalf("unexpected report summary: %+v", report)
	}

	if len(report.Checks) != 18 {
		t.Fatalf("expected 18 checks, got %d", len(report.Checks))
	}

	// Verify structure of first check
	c := report.Checks[0]
	if c.ID != "api.live" {
		t.Fatalf("first check ID = %q, want api.live", c.ID)
	}

	if c.Status != doctorOK {
		t.Fatalf("first check status = %q, want pass", c.Status)
	}

	if c.Title == "" {
		t.Fatal("first check title is empty")
	}

	if c.Severity == "" {
		t.Fatal("first check severity is empty")
	}
}

func TestDoctor_jsonOutputStillFailsOnFailedCheck(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health/live":
			w.WriteHeader(http.StatusOK)
		case "/health/ready":
			w.WriteHeader(http.StatusServiceUnavailable)
		case "/api/v1/setup/status":
			_ = json.NewEncoder(w).Encode(map[string]bool{"setup_complete": true, "auth_enabled": true})
		case "/api/v1/schema/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"current_version": 5, "has_schema": true})
		case "/api/v1/reconciler/heartbeat":
			_ = json.NewEncoder(w).Encode(map[string]any{"active": true, "last_activity_unix": 1715000000})
		case "/api/v1/audit/drops":
			_ = json.NewEncoder(w).Encode(map[string]any{"dropped": 0})
		case "/api/v1/db/pool-stats":
			_ = json.NewEncoder(w).Encode(map[string]any{"open_connections": 3, "in_use": 1, "wait_count": 0})
		case "/api/v1/queue/backlog":
			_ = json.NewEncoder(w).Encode(map[string]any{"queued": 0})
		case "/api/v1/reconciler/stuck-runs":
			_ = json.NewEncoder(w).Encode(map[string]any{"stuck": 0})
		case "/api/v1/cells/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"cells": []map[string]any{}})
		case "/api/v1/log/reachable":
			_ = json.NewEncoder(w).Encode(map[string]any{"reachable": true, "state": "READY"})
		case "/api/v1/audit/flush-failures":
			_ = json.NewEncoder(w).Encode(map[string]any{"flush_failures": 0})
		case "/api/v1/catalog/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"pending": 0, "applied": 4, "failed": 0, "total": 4})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})

	t.Setenv("VECTIS_API_TOKEN", "test-token")
	doctorJSON = true
	defer func() { doctorJSON = false }()

	var buf bytes.Buffer
	err := doctor(&buf)
	if err == nil {
		t.Fatal("expected JSON doctor to fail when a check fails")
	}

	var report doctorReport
	if err := json.Unmarshal(buf.Bytes(), &report); err != nil {
		t.Fatalf("invalid JSON output: %v\n%s", err, buf.String())
	}

	if report.Status != doctorFail || report.Failed != 1 {
		t.Fatalf("unexpected report summary: %+v", report)
	}

	if len(report.Checks) != 18 {
		t.Fatalf("expected 18 checks, got %d", len(report.Checks))
	}
}

func TestDoctorTLSFiles_validConfiguredFiles(t *testing.T) {
	certFile, keyFile := writeTestCertificate(t, time.Now().Add(30*24*time.Hour))
	t.Setenv("VECTIS_GRPC_TLS_INSECURE", "false")
	t.Setenv("VECTIS_GRPC_TLS_CA_FILE", certFile)
	t.Setenv("VECTIS_GRPC_TLS_CERT_FILE", certFile)
	t.Setenv("VECTIS_GRPC_TLS_KEY_FILE", keyFile)

	check := doctorTLSFiles()
	if check.Status != doctorOK {
		t.Fatalf("expected TLS check to pass, got %#v", check)
	}
}

func TestDoctorTLSFiles_rejectsMismatchedPair(t *testing.T) {
	certFile, _ := writeTestCertificate(t, time.Now().Add(30*24*time.Hour))
	_, keyFile := writeTestCertificate(t, time.Now().Add(30*24*time.Hour))
	t.Setenv("VECTIS_GRPC_TLS_CERT_FILE", certFile)
	t.Setenv("VECTIS_GRPC_TLS_KEY_FILE", keyFile)

	check := doctorTLSFiles()
	if check.Status != doctorFail {
		t.Fatalf("expected TLS check to fail, got %#v", check)
	}

	if !strings.Contains(check.Summary, "certificate/key mismatch") {
		t.Fatalf("expected mismatch summary, got %q", check.Summary)
	}
}

func TestDoctorTLSFiles_warnsForSoonExpiringCertificate(t *testing.T) {
	certFile, keyFile := writeTestCertificate(t, time.Now().Add(24*time.Hour))
	t.Setenv("VECTIS_GRPC_TLS_CERT_FILE", certFile)
	t.Setenv("VECTIS_GRPC_TLS_KEY_FILE", keyFile)

	check := doctorTLSFiles()
	if check.Status != doctorWarn {
		t.Fatalf("expected TLS expiry warning, got %#v", check)
	}

	if !strings.Contains(check.Summary, "expires at") {
		t.Fatalf("expected expiry summary, got %q", check.Summary)
	}
}

func TestDoctorFilesystemPressure_existingWritableDirectory(t *testing.T) {
	dir := t.TempDir()
	check := doctorFilesystemPressure("test.fs", "Test filesystem", "test path", dir)
	if check.Status != doctorOK {
		t.Fatalf("expected filesystem check to pass, got %#v", check)
	}

	if !strings.Contains(check.Evidence, "path="+dir) {
		t.Fatalf("expected path evidence, got %q", check.Evidence)
	}
}

func writeTestCertificate(t *testing.T, notAfter time.Time) (string, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     notAfter,
		DNSNames:     []string{"localhost"},
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IsCA:         true,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}

	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyBytes := x509.MarshalPKCS1PrivateKey(key)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: keyBytes})

	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}

	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}

	return certFile, keyFile
}

func TestDoctor_strictWarnsExitNonzero(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health/live", "/health/ready":
			w.WriteHeader(http.StatusOK)
		case "/api/v1/setup/status":
			_ = json.NewEncoder(w).Encode(map[string]bool{"setup_complete": false, "auth_enabled": true})
		case "/api/v1/schema/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"current_version": 5, "has_schema": true})
		case "/api/v1/reconciler/heartbeat":
			_ = json.NewEncoder(w).Encode(map[string]any{"active": true, "last_activity_unix": 1715000000})
		case "/api/v1/audit/drops":
			_ = json.NewEncoder(w).Encode(map[string]any{"dropped": 0})
		case "/api/v1/db/pool-stats":
			_ = json.NewEncoder(w).Encode(map[string]any{"open_connections": 3, "in_use": 1, "wait_count": 0})
		case "/api/v1/queue/backlog":
			_ = json.NewEncoder(w).Encode(map[string]any{"queued": 0})
		case "/api/v1/reconciler/stuck-runs":
			_ = json.NewEncoder(w).Encode(map[string]any{"stuck": 0})
		case "/api/v1/cells/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"cells": []map[string]any{}})
		case "/api/v1/log/reachable":
			_ = json.NewEncoder(w).Encode(map[string]any{"reachable": true, "state": "READY"})
		case "/api/v1/audit/flush-failures":
			_ = json.NewEncoder(w).Encode(map[string]any{"flush_failures": 0})
		case "/api/v1/catalog/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"pending": 0, "applied": 4, "failed": 0, "total": 4})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})

	t.Setenv("VECTIS_API_TOKEN", "")
	doctorStrict = true
	defer func() { doctorStrict = false }()

	var buf bytes.Buffer
	err := doctor(&buf)
	if err == nil {
		t.Fatal("expected error due to --strict with warnings")
	}

	out := buf.String()
	if !strings.Contains(out, "WARN  Initial setup") {
		t.Fatalf("expected warning in output:\n%s", out)
	}
}

func TestDoctor_reconcilerNoRecoveryActivityIsHealthy(t *testing.T) {
	check := doctorCheckReconcilerActivityResponse(t, map[string]any{"active": false})
	if check.Status != doctorOK {
		t.Fatalf("expected no recovery activity to pass, got %#v", check)
	}

	if !strings.Contains(check.Summary, "no reconciler recovery activity recorded") {
		t.Fatalf("unexpected summary: %q", check.Summary)
	}
}

func TestDoctor_queueBacklogEvidenceIncludesCells(t *testing.T) {
	check := doctorCheckQueueBacklogResponse(t, map[string]any{
		"queued": 101,
		"cells": []map[string]any{
			{"cell_id": "iad-a", "queued": 75},
			{"cell_id": "pdx-b", "queued": 26},
		},
	})

	if check.Status != doctorWarn {
		t.Fatalf("expected queue backlog to warn, got %#v", check)
	}

	for _, want := range []string{"queued=101", "iad-a:75", "pdx-b:26"} {
		if !strings.Contains(check.Evidence, want) {
			t.Fatalf("expected evidence to contain %q, got %q", want, check.Evidence)
		}
	}
}

func TestDoctor_cellIngressRoutesWarnsForUnhealthyCells(t *testing.T) {
	check := doctorCheckCellsStatusResponse(t, map[string]any{
		"cells": []map[string]any{
			{"cell_id": "iad-a", "ingress_required": true, "ingress_configured": true, "ingress_reachable": true, "status": "ready"},
			{"cell_id": "pdx-b", "ingress_required": true, "ingress_configured": false, "ingress_reachable": false, "status": "missing_route"},
		},
	})

	if check.Status != doctorWarn {
		t.Fatalf("expected unhealthy cell ingress route to warn, got %#v", check)
	}

	for _, want := range []string{"iad-a:ready", "pdx-b:missing_route"} {
		if !strings.Contains(check.Evidence, want) {
			t.Fatalf("expected evidence to contain %q, got %q", want, check.Evidence)
		}
	}
}

func TestDoctor_stuckRunsEvidenceIncludesCells(t *testing.T) {
	check := doctorCheckStuckRunsResponse(t, map[string]any{
		"stuck": 3,
		"cells": []map[string]any{
			{"cell_id": "iad-a", "stuck": 2},
			{"cell_id": "pdx-b", "stuck": 1},
		},
	})

	if check.Status != doctorWarn {
		t.Fatalf("expected stuck runs to warn, got %#v", check)
	}

	for _, want := range []string{"stuck=3", "iad-a:2", "pdx-b:1"} {
		if !strings.Contains(check.Evidence, want) {
			t.Fatalf("expected evidence to contain %q, got %q", want, check.Evidence)
		}
	}
}

func TestDoctor_stuckRunsWarnsForPendingTaskDispatch(t *testing.T) {
	check := doctorCheckStuckRunsResponse(t, map[string]any{
		"stuck":                 0,
		"task_dispatch_pending": 2,
		"task_dispatch_cells": []map[string]any{
			{"cell_id": "iad-a", "pending": 2},
		},
	})

	if check.Status != doctorWarn {
		t.Fatalf("expected pending task dispatch to warn, got %#v", check)
	}

	if !strings.Contains(check.Summary, "2 pending task continuations detected") {
		t.Fatalf("unexpected summary: %q", check.Summary)
	}

	for _, want := range []string{"stuck=0", "task_dispatch_pending=2", "task_cells=iad-a:2"} {
		if !strings.Contains(check.Evidence, want) {
			t.Fatalf("expected evidence to contain %q, got %q", want, check.Evidence)
		}
	}
}

func TestDoctor_catalogInboxWarnsForFailedEvents(t *testing.T) {
	check := doctorCheckCatalogStatusResponse(t, map[string]any{
		"pending": 2,
		"applied": 10,
		"failed":  1,
		"total":   13,
		"sources": []map[string]any{
			{"source_cell": "iad-a", "pending": 2, "applied": 10, "failed": 1, "total": 13},
		},
	})

	if check.Status != doctorWarn {
		t.Fatalf("expected failed catalog events to warn, got %#v", check)
	}

	if !strings.Contains(check.Summary, "1 catalog event failed") {
		t.Fatalf("unexpected summary: %q", check.Summary)
	}

	if !strings.Contains(check.Evidence, "pending=2") || !strings.Contains(check.Evidence, "failed=1") || !strings.Contains(check.Evidence, "iad-a:p=2/f=1") {
		t.Fatalf("unexpected evidence: %q", check.Evidence)
	}
}

func TestDoctor_catalogInboxWarnsForHighPendingBacklog(t *testing.T) {
	check := doctorCheckCatalogStatusResponse(t, map[string]any{
		"pending": 101,
		"applied": 10,
		"failed":  0,
		"total":   111,
		"sources": []map[string]any{
			{"source_cell": "pdx-b", "pending": 101, "applied": 10, "failed": 0, "total": 111},
		},
	})

	if check.Status != doctorWarn {
		t.Fatalf("expected pending catalog backlog to warn, got %#v", check)
	}

	if !strings.Contains(check.Summary, "catalog inbox backlog high") {
		t.Fatalf("unexpected summary: %q", check.Summary)
	}

	if !strings.Contains(check.Evidence, "pdx-b:p=101/f=0") {
		t.Fatalf("unexpected evidence: %q", check.Evidence)
	}
}

func doctorCheckReconcilerActivityResponse(t *testing.T, body map[string]any) doctorCheck {
	t.Helper()
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/reconciler/heartbeat" {
			t.Errorf("unexpected path=%s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		_ = json.NewEncoder(w).Encode(body)
	})

	return doctorReconcilerActive()
}

func doctorCheckCatalogStatusResponse(t *testing.T, body map[string]any) doctorCheck {
	t.Helper()
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/catalog/status" {
			t.Errorf("unexpected path=%s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		_ = json.NewEncoder(w).Encode(body)
	})

	return doctorCatalogInbox()
}

func doctorCheckQueueBacklogResponse(t *testing.T, body map[string]any) doctorCheck {
	t.Helper()
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/queue/backlog" {
			t.Errorf("unexpected path=%s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		_ = json.NewEncoder(w).Encode(body)
	})

	return doctorQueueBacklog()
}

func doctorCheckCellsStatusResponse(t *testing.T, body map[string]any) doctorCheck {
	t.Helper()
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/cells/status" {
			t.Errorf("unexpected path=%s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		_ = json.NewEncoder(w).Encode(body)
	})

	return doctorCellIngressRoutes()
}

func doctorCheckStuckRunsResponse(t *testing.T, body map[string]any) doctorCheck {
	t.Helper()
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/reconciler/stuck-runs" {
			t.Errorf("unexpected path=%s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		_ = json.NewEncoder(w).Encode(body)
	})

	return doctorStuckRuns()
}
