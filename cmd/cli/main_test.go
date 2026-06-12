package main

import (
	"bytes"
	"context"
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
			ArtifactBlobs:   &cutoff,
		},
		Counts: retention.Counts{
			TerminalRuns:      3,
			RunDispatchEvents: 4,
			RunArtifacts:      5,
			RunTasks:          6,
			TaskAttempts:      7,
			RunSegments:       8,
			SegmentExecutions: 9,
			JobDefinitions:    10,
			IdempotencyKeys:   11,
			AuditLog:          12,
		},
	}

	var buf bytes.Buffer
	printRetentionReport(&buf, report, retention.FileReport{RunLogFiles: 14, RunLogBytes: 15, ArtifactBlobFiles: 16, ArtifactBlobBytes: 17})

	out := buf.String()
	for _, want := range []string{
		"would_delete.terminal_runs=3",
		"would_delete.run_dispatch_events=4",
		"would_delete.run_artifacts=5",
		"would_delete.run_tasks=6",
		"would_delete.task_attempts=7",
		"would_delete.run_segments=8",
		"would_delete.segment_executions=9",
		"would_delete.job_definitions=10",
		"would_delete.idempotency_keys=11",
		"would_delete.audit_log=12",
		"would_delete.run_log_files=14",
		"would_delete.run_log_bytes=15",
		"would_delete.artifact_blob_files=16",
		"would_delete.artifact_blob_bytes=17",
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

func TestPersistJobFromSource_sendsCreateRequestAndPrintsProvenance(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/jobs/source/build" {
			t.Errorf("path=%s", r.URL.Path)
		}

		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type=%q", got)
		}

		var body jobSourceRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode body: %v", err)
		}

		if body.Namespace != "/team-a" ||
			body.RepositoryID != "vectis" ||
			body.Ref != "main" ||
			body.Path != ".vectis/jobs/build.json" {
			t.Errorf("create source body mismatch: %+v", body)
		}

		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"job_id":          "build",
			"version":         3,
			"definition_hash": "hash",
			"source": map[string]any{
				"repository_id":   "vectis",
				"requested_ref":   "main",
				"resolved_commit": "0123456789abcdef",
				"path":            ".vectis/jobs/build.json",
				"blob_sha":        "abcdef0123456789",
			},
		})
	})

	var buf bytes.Buffer
	err := persistJobFromSourceWithOutput(&buf, http.MethodPost, "build", "vectis", ".vectis/jobs/build.json", "/team-a", "main")
	if err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{"job_id=build", "version=3", "definition_hash=hash", "repository_id=vectis", "requested_ref=main", "resolved_commit=0123456789abcdef", "path=.vectis/jobs/build.json", "blob_sha=abcdef0123456789"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestPersistJobFromSource_sendsUpdateRequest(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/jobs/source/build" {
			t.Errorf("path=%s", r.URL.Path)
		}

		var body jobSourceRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode body: %v", err)
		}

		if body.Namespace != "" ||
			body.RepositoryID != "vectis" ||
			body.Ref != "release/1" ||
			body.Path != ".vectis/jobs/build.json" {
			t.Errorf("update source body mismatch: %+v", body)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"job_id":          "build",
			"version":         4,
			"definition_hash": "hash-2",
			"source": map[string]any{
				"repository_id":   "vectis",
				"requested_ref":   "release/1",
				"resolved_commit": "fedcba9876543210",
				"path":            ".vectis/jobs/build.json",
				"blob_sha":        "abcdef0123456789",
			},
		})
	})

	var buf bytes.Buffer
	err := persistJobFromSourceWithOutput(&buf, http.MethodPut, "build", "vectis", ".vectis/jobs/build.json", "", "release/1")
	if err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{"job_id=build", "version=4", "definition_hash=hash-2", "requested_ref=release/1", "resolved_commit=fedcba9876543210"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestShowStoredJobSource_sendsVersionAndPrintsProvenance(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/jobs/build/source" {
			t.Errorf("path=%s", r.URL.Path)
		}

		if got := r.URL.Query().Get("version"); got != "2" {
			t.Errorf("version=%q", got)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"job_id":          "build",
			"version":         2,
			"definition_hash": "hash",
			"source": map[string]any{
				"repository_id":   "vectis",
				"requested_ref":   "main",
				"resolved_commit": "0123456789abcdef",
				"path":            ".vectis/jobs/build.json",
				"blob_sha":        "abcdef0123456789",
			},
		})
	})

	var buf bytes.Buffer
	if err := showStoredJobSourceWithOutput(&buf, "build", 2); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{"job_id=build", "version=2", "repository_id=vectis", "requested_ref=main", "resolved_commit=0123456789abcdef", "path=.vectis/jobs/build.json"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestShowStoredJobSourceDefinition_sendsVersionAndPrintsDefinition(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/jobs/build/source/definition" {
			t.Errorf("path=%s", r.URL.Path)
		}

		if got := r.URL.Query().Get("version"); got != "2" {
			t.Errorf("version=%q", got)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"job_id":          "build",
			"version":         2,
			"definition_hash": "hash",
			"definition": map[string]any{
				"root": map[string]any{
					"id":   "root",
					"uses": "builtins/shell",
					"with": map[string]any{"command": "true"},
				},
			},
			"source": map[string]any{
				"repository_id":   "vectis",
				"requested_ref":   "main",
				"resolved_commit": "0123456789abcdef",
				"path":            ".vectis/jobs/build.json",
				"blob_sha":        "abcdef0123456789",
			},
		})
	})

	cmd := &cobra.Command{}
	cmd.Flags().Bool("raw", false, "")
	var buf bytes.Buffer
	if err := showStoredJobSourceDefinitionWithOutput(cmd, &buf, "build", 2); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{`"root"`, `"builtins/shell"`} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestTriggerSourceJob_sendsOptionsAndIdempotencyKey(t *testing.T) {
	oldRef := sourceTriggerRef
	oldPath := sourceTriggerPath
	oldCell := sourceTriggerCellID
	oldKey := sourceTriggerIdemKey
	sourceTriggerRef = "feature/source"
	sourceTriggerPath = ".vectis/jobs/custom.json"
	sourceTriggerCellID = "pdx-b"
	sourceTriggerIdemKey = "source-trigger-key"
	t.Cleanup(func() {
		sourceTriggerRef = oldRef
		sourceTriggerPath = oldPath
		sourceTriggerCellID = oldCell
		sourceTriggerIdemKey = oldKey
	})

	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/source-repositories/vectis/jobs/build/trigger" {
			t.Errorf("path=%s", r.URL.Path)
		}

		if got := r.Header.Get("Idempotency-Key"); got != "source-trigger-key" {
			t.Errorf("Idempotency-Key=%q", got)
		}

		var body sourceTriggerRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode body: %v", err)
		}

		if body.Ref != "feature/source" || body.Path != ".vectis/jobs/custom.json" || body.CellID != "pdx-b" {
			t.Errorf("trigger body mismatch: %+v", body)
		}

		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"job_id":             "build",
			"run_id":             "run-source",
			"run_index":          7,
			"definition_version": 1,
			"definition_hash":    "hash",
			"source":             map[string]any{"repository_id": "vectis", "requested_ref": "feature/source", "resolved_commit": "0123456789abcdef", "path": ".vectis/jobs/custom.json"},
		})
	})

	cmd := &cobra.Command{}
	cmd.Flags().Bool("follow", false, "")
	var buf bytes.Buffer
	if err := triggerSourceJobWithOutput(cmd, &buf, "vectis", "build"); err != nil {
		t.Fatal(err)
	}

	if got := strings.TrimSpace(buf.String()); got != "run-source" {
		t.Fatalf("output=%q, want run-source", got)
	}
}

func TestListSourceJobs_sendsQueryAndPrintsJobs(t *testing.T) {
	oldRef := sourceJobsRef
	oldPath := sourceJobsPath
	oldLimit := sourceJobsLimit
	oldQuiet := sourceJobsQuiet
	sourceJobsRef = "main"
	sourceJobsPath = ".vectis/jobs"
	sourceJobsLimit = 5
	sourceJobsQuiet = false
	t.Cleanup(func() {
		sourceJobsRef = oldRef
		sourceJobsPath = oldPath
		sourceJobsLimit = oldLimit
		sourceJobsQuiet = oldQuiet
	})

	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/source-repositories/vectis/jobs" {
			t.Errorf("path=%s", r.URL.Path)
		}

		q := r.URL.Query()
		if q.Get("ref") != "main" || q.Get("path") != ".vectis/jobs" || q.Get("limit") != "5" {
			t.Errorf("query=%s", r.URL.RawQuery)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"repository_id":   "vectis",
			"requested_ref":   "main",
			"resolved_commit": "0123456789abcdef0123456789abcdef01234567",
			"path":            ".vectis/jobs",
			"limit":           5,
			"jobs": []map[string]any{
				{
					"job_id":   "build",
					"path":     ".vectis/jobs/build.json",
					"name":     "build.json",
					"blob_sha": "abcdef0123456789abcdef0123456789abcdef01",
					"source": map[string]any{
						"repository_id":   "vectis",
						"requested_ref":   "main",
						"resolved_commit": "0123456789abcdef0123456789abcdef01234567",
						"path":            ".vectis/jobs/build.json",
						"blob_sha":        "abcdef0123456789abcdef0123456789abcdef01",
					},
				},
			},
		})
	})

	var buf bytes.Buffer
	if err := listSourceJobsWithOutput(&buf, "vectis"); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{"JOB ID", "build", ".vectis/jobs/build.json", "0123456789ab", "abcdef012345"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestGetSource_sendsRequestAndPrintsRepository(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/source-repositories/vectis" {
			t.Errorf("path=%s", r.URL.Path)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"repository_id":  "vectis",
			"namespace":      "/team-a",
			"source_kind":    "local_checkout",
			"checkout_path":  "/srv/vectis/source",
			"checkout_mode":  "managed",
			"authoring_mode": "local_commit",
			"authoring":      map[string]any{"mode": "local_commit", "write_definitions": true, "local_commits": true},
			"canonical_url":  "https://git.example.com/acme/vectis.git",
			"default_ref":    "main",
			"credential_ref": "git-creds",
			"enabled":        true,
			"sync":           map[string]any{"status": "succeeded", "ref": "main", "commit": "0123456789abcdef"},
		})
	})

	var buf bytes.Buffer
	if err := getSourceWithOutput(&buf, "vectis"); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{"repository_id=vectis", "namespace=/team-a", "checkout_mode=managed", "authoring_mode=local_commit", "write_definitions=true", "canonical_url=https://git.example.com/acme/vectis.git", "default_ref=main", "credential_ref=git-creds", "sync_status=succeeded"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestListSourceSchedules_sendsNamespaceQueryAndPrintsSchedules(t *testing.T) {
	oldNamespace := sourceSchedulesNamespace
	oldQuiet := sourceSchedulesQuiet
	oldOverridesOnly := sourceSchedulesOverrideOnly
	oldStaleOnly := sourceSchedulesStaleOnly
	sourceSchedulesNamespace = "/team-a"
	sourceSchedulesQuiet = false
	sourceSchedulesOverrideOnly = false
	sourceSchedulesStaleOnly = false
	t.Cleanup(func() {
		sourceSchedulesNamespace = oldNamespace
		sourceSchedulesQuiet = oldQuiet
		sourceSchedulesOverrideOnly = oldOverridesOnly
		sourceSchedulesStaleOnly = oldStaleOnly
	})

	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/source-schedules" {
			t.Errorf("path=%s", r.URL.Path)
		}

		if r.URL.Query().Get("namespace") != "/team-a" {
			t.Errorf("query=%s", r.URL.RawQuery)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"namespace": "/team-a",
			"schedules": []map[string]any{
				{
					"schedule_id":     "nightly-build",
					"repository_id":   "vectis",
					"namespace":       "/team-a",
					"job_id":          "build",
					"cron_spec":       "30 8 * * *",
					"next_run_at":     "2026-05-01T08:30:00Z",
					"ref":             "main",
					"path":            ".vectis/jobs/build.json",
					"path_derived":    true,
					"configured_ref":  "main",
					"configured_path": "",
					"declared":        true,
					"override": map[string]any{
						"ref":             "hotfix/build",
						"path":            ".vectis/jobs/build-hotfix.json",
						"reason":          "production hotfix",
						"created_at_unix": 1770000000,
					},
					"enabled": true,
				},
			},
		})
	})

	var buf bytes.Buffer
	if err := listSourceSchedulesWithOutput(&buf, ""); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{"SCHEDULE", "DECLARED", "OVERRIDE", "nightly-build", "vectis", "build", "30 8 * * *", "2026-05-01T08:30:00Z", ".vectis/jobs/build.json (derived)", "ref=hotfix/build", "path=.vectis/jobs/build-hotfix.json", "reason=production hotfix", "true"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestListSourceSchedules_filtersOverrides(t *testing.T) {
	oldNamespace := sourceSchedulesNamespace
	oldQuiet := sourceSchedulesQuiet
	oldOverridesOnly := sourceSchedulesOverrideOnly
	oldStaleOnly := sourceSchedulesStaleOnly
	sourceSchedulesNamespace = "/team-a"
	sourceSchedulesQuiet = false
	sourceSchedulesOverrideOnly = true
	sourceSchedulesStaleOnly = false
	t.Cleanup(func() {
		sourceSchedulesNamespace = oldNamespace
		sourceSchedulesQuiet = oldQuiet
		sourceSchedulesOverrideOnly = oldOverridesOnly
		sourceSchedulesStaleOnly = oldStaleOnly
	})

	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/source-schedules" {
			t.Errorf("path=%s", r.URL.Path)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"namespace": "/team-a",
			"schedules": []map[string]any{
				{
					"schedule_id":     "nightly-build",
					"repository_id":   "vectis",
					"namespace":       "/team-a",
					"job_id":          "build",
					"cron_spec":       "30 8 * * *",
					"next_run_at":     "2026-05-01T08:30:00Z",
					"ref":             "hotfix/build",
					"path":            ".vectis/jobs/build-hotfix.json",
					"configured_ref":  "main",
					"configured_path": "",
					"declared":        true,
					"override": map[string]any{
						"ref":             "hotfix/build",
						"path":            ".vectis/jobs/build-hotfix.json",
						"reason":          "production hotfix",
						"created_at_unix": 1770000000,
					},
					"enabled": true,
				},
				{
					"schedule_id":     "hourly-test",
					"repository_id":   "vectis",
					"namespace":       "/team-a",
					"job_id":          "test",
					"cron_spec":       "0 * * * *",
					"next_run_at":     "2026-05-01T09:00:00Z",
					"ref":             "main",
					"path":            ".vectis/jobs/test.json",
					"configured_ref":  "main",
					"configured_path": "",
					"declared":        true,
					"enabled":         true,
				},
			},
		})
	})

	var buf bytes.Buffer
	if err := listSourceSchedulesWithOutput(&buf, ""); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{"nightly-build", "ref=hotfix/build", "reason=production hotfix"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
	if strings.Contains(out, "hourly-test") {
		t.Fatalf("expected overrides filter to hide hourly-test, got:\n%s", out)
	}
}

func TestListSourceSchedules_filtersStale(t *testing.T) {
	oldNamespace := sourceSchedulesNamespace
	oldQuiet := sourceSchedulesQuiet
	oldOverridesOnly := sourceSchedulesOverrideOnly
	oldStaleOnly := sourceSchedulesStaleOnly
	sourceSchedulesNamespace = "/team-a"
	sourceSchedulesQuiet = false
	sourceSchedulesOverrideOnly = false
	sourceSchedulesStaleOnly = true
	t.Cleanup(func() {
		sourceSchedulesNamespace = oldNamespace
		sourceSchedulesQuiet = oldQuiet
		sourceSchedulesOverrideOnly = oldOverridesOnly
		sourceSchedulesStaleOnly = oldStaleOnly
	})

	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/source-schedules" {
			t.Errorf("path=%s", r.URL.Path)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"namespace": "/team-a",
			"schedules": []map[string]any{
				{
					"schedule_id":     "nightly-build",
					"repository_id":   "vectis",
					"namespace":       "/team-a",
					"job_id":          "build",
					"cron_spec":       "30 8 * * *",
					"next_run_at":     "2026-05-01T08:30:00Z",
					"ref":             "main",
					"path":            ".vectis/jobs/build.json",
					"configured_ref":  "main",
					"configured_path": "",
					"declared":        true,
					"enabled":         true,
				},
				{
					"schedule_id":     "old-nightly",
					"repository_id":   "vectis",
					"namespace":       "/team-a",
					"job_id":          "old",
					"cron_spec":       "0 3 * * *",
					"next_run_at":     "2026-05-01T03:00:00Z",
					"ref":             "main",
					"path":            ".vectis/jobs/old.json",
					"configured_ref":  "main",
					"configured_path": "",
					"declared":        false,
					"enabled":         false,
				},
			},
		})
	})

	var buf bytes.Buffer
	if err := listSourceSchedulesWithOutput(&buf, ""); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	if !strings.Contains(out, "old-nightly") || !strings.Contains(out, "false") {
		t.Fatalf("expected stale schedule in output, got:\n%s", out)
	}

	if strings.Contains(out, "nightly-build") {
		t.Fatalf("expected stale filter to hide declared schedule, got:\n%s", out)
	}
}

func TestListSourceSchedules_sendsRepositoryPathAndQuietOutput(t *testing.T) {
	oldNamespace := sourceSchedulesNamespace
	oldQuiet := sourceSchedulesQuiet
	oldOverridesOnly := sourceSchedulesOverrideOnly
	oldStaleOnly := sourceSchedulesStaleOnly
	sourceSchedulesNamespace = "/ignored"
	sourceSchedulesQuiet = true
	sourceSchedulesOverrideOnly = false
	sourceSchedulesStaleOnly = false
	t.Cleanup(func() {
		sourceSchedulesNamespace = oldNamespace
		sourceSchedulesQuiet = oldQuiet
		sourceSchedulesOverrideOnly = oldOverridesOnly
		sourceSchedulesStaleOnly = oldStaleOnly
	})

	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/source-repositories/vectis/schedules" {
			t.Errorf("path=%s", r.URL.Path)
		}

		if r.URL.RawQuery != "" {
			t.Errorf("expected no query for repository schedule list, got %s", r.URL.RawQuery)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"namespace":     "/",
			"repository_id": "vectis",
			"schedules": []map[string]any{
				{"schedule_id": "nightly-build", "repository_id": "vectis", "job_id": "build", "cron_spec": "30 8 * * *", "next_run_at": "2026-05-01T08:30:00Z", "path": ".vectis/jobs/build.json", "enabled": true},
				{"schedule_id": "hourly-test", "repository_id": "vectis", "job_id": "test", "cron_spec": "0 * * * *", "next_run_at": "2026-05-01T09:00:00Z", "path": ".vectis/jobs/test.json", "enabled": false},
			},
		})
	})

	var buf bytes.Buffer
	if err := listSourceSchedulesWithOutput(&buf, "vectis"); err != nil {
		t.Fatal(err)
	}

	if got := strings.TrimSpace(buf.String()); got != "nightly-build\nhourly-test" {
		t.Fatalf("quiet output=%q", got)
	}
}

func TestSetSourceScheduleOverride_sendsBodyAndPrintsResult(t *testing.T) {
	oldRef := sourceOverrideRef
	oldPath := sourceOverridePath
	oldReason := sourceOverrideReason
	sourceOverrideRef = "hotfix/build"
	sourceOverridePath = ".vectis/jobs/build-hotfix.json"
	sourceOverrideReason = "production hotfix"
	t.Cleanup(func() {
		sourceOverrideRef = oldRef
		sourceOverridePath = oldPath
		sourceOverrideReason = oldReason
	})

	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/source-schedules/nightly-build/override" {
			t.Errorf("path=%s", r.URL.Path)
		}

		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("content-type=%q", got)
		}

		var body sourceScheduleOverrideRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}

		if body.Ref != "hotfix/build" || body.Path != ".vectis/jobs/build-hotfix.json" || body.Reason != "production hotfix" {
			t.Fatalf("override body mismatch: %+v", body)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"schedule_id":   "nightly-build",
			"repository_id": "vectis",
			"job_id":        "build",
			"cron_spec":     "30 8 * * *",
			"next_run_at":   "2026-05-01T08:30:00Z",
			"ref":           "hotfix/build",
			"path":          ".vectis/jobs/build-hotfix.json",
			"enabled":       true,
			"override": map[string]any{
				"ref":             "hotfix/build",
				"path":            ".vectis/jobs/build-hotfix.json",
				"reason":          "production hotfix",
				"created_at_unix": 1770000000,
			},
		})
	})

	var buf bytes.Buffer
	if err := setSourceScheduleOverrideWithOutput(&buf, "nightly-build"); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{"override set", "hotfix/build", ".vectis/jobs/build-hotfix.json", "production hotfix"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestSetSourceScheduleOverride_requiresRefOrPath(t *testing.T) {
	oldRef := sourceOverrideRef
	oldPath := sourceOverridePath
	oldReason := sourceOverrideReason
	sourceOverrideRef = ""
	sourceOverridePath = ""
	sourceOverrideReason = ""
	t.Cleanup(func() {
		sourceOverrideRef = oldRef
		sourceOverridePath = oldPath
		sourceOverrideReason = oldReason
	})

	var buf bytes.Buffer
	if err := setSourceScheduleOverrideWithOutput(&buf, "nightly-build"); err == nil {
		t.Fatal("expected missing ref/path error")
	}
}

func TestClearSourceScheduleOverride_sendsDeleteAndPrintsResult(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/source-schedules/nightly-build/override" {
			t.Errorf("path=%s", r.URL.Path)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"schedule_id":   "nightly-build",
			"repository_id": "vectis",
			"job_id":        "build",
			"cron_spec":     "30 8 * * *",
			"next_run_at":   "2026-05-01T08:30:00Z",
			"ref":           "main",
			"path":          ".vectis/jobs/build.json",
			"enabled":       true,
		})
	})

	var buf bytes.Buffer
	if err := clearSourceScheduleOverrideWithOutput(&buf, "nightly-build"); err != nil {
		t.Fatal(err)
	}

	if out := buf.String(); !strings.Contains(out, "override cleared") || !strings.Contains(out, "nightly-build") {
		t.Fatalf("unexpected clear output:\n%s", out)
	}
}

func TestUpdateSource_sendsOnlyChangedFieldsAndPrintsRepository(t *testing.T) {
	oldSourceKind := sourceUpdateSourceKind
	oldCheckoutPath := sourceUpdateCheckoutPath
	oldCheckoutMode := sourceUpdateCheckoutMode
	oldAuthoringMode := sourceUpdateAuthoringMode
	oldCanonicalURL := sourceUpdateCanonicalURL
	oldDefaultRef := sourceUpdateDefaultRef
	oldCredentialRef := sourceUpdateCredentialRef
	oldEnable := sourceUpdateEnable
	oldDisable := sourceUpdateDisable
	t.Cleanup(func() {
		sourceUpdateSourceKind = oldSourceKind
		sourceUpdateCheckoutPath = oldCheckoutPath
		sourceUpdateCheckoutMode = oldCheckoutMode
		sourceUpdateAuthoringMode = oldAuthoringMode
		sourceUpdateCanonicalURL = oldCanonicalURL
		sourceUpdateDefaultRef = oldDefaultRef
		sourceUpdateCredentialRef = oldCredentialRef
		sourceUpdateEnable = oldEnable
		sourceUpdateDisable = oldDisable
	})

	cmd := &cobra.Command{}
	configureSourcesUpdateFlags(cmd)
	for name, value := range map[string]string{
		"checkout-mode":  "managed",
		"authoring-mode": "local_commit",
		"canonical-url":  "https://git.example.com/acme/vectis.git",
		"default-ref":    "main",
		"credential-ref": "git-creds",
		"disable":        "true",
	} {
		if err := cmd.Flags().Set(name, value); err != nil {
			t.Fatalf("set %s: %v", name, err)
		}
	}

	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/source-repositories/vectis" {
			t.Errorf("path=%s", r.URL.Path)
		}

		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type=%q", got)
		}

		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode body: %v", err)
		}

		want := map[string]any{
			"checkout_mode":  "managed",
			"authoring_mode": "local_commit",
			"canonical_url":  "https://git.example.com/acme/vectis.git",
			"default_ref":    "main",
			"credential_ref": "git-creds",
			"enabled":        false,
		}

		if len(body) != len(want) {
			t.Errorf("body len=%d, want %d (%v)", len(body), len(want), body)
		}

		for key, value := range want {
			if got := body[key]; got != value {
				t.Errorf("%s=%v, want %v", key, got, value)
			}
		}

		if _, ok := body["checkout_path"]; ok {
			t.Errorf("checkout_path should not be sent when flag was omitted")
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"repository_id":  "vectis",
			"namespace":      "/",
			"source_kind":    "local_checkout",
			"checkout_mode":  "managed",
			"authoring_mode": "local_commit",
			"authoring":      map[string]any{"mode": "local_commit", "write_definitions": true, "local_commits": true},
			"canonical_url":  "https://git.example.com/acme/vectis.git",
			"default_ref":    "main",
			"credential_ref": "git-creds",
			"enabled":        false,
			"sync":           map[string]any{"status": "never"},
		})
	})

	var buf bytes.Buffer
	if err := updateSourceWithOutput(cmd, &buf, "vectis"); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{"repository_id=vectis", "checkout_mode=managed", "authoring_mode=local_commit", "enabled=false", "sync_status=never"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestDeleteSource_requiresConfirmation(t *testing.T) {
	var buf bytes.Buffer
	if err := deleteSourceWithOutput(&buf, "vectis", false); err == nil {
		t.Fatal("expected confirmation error")
	}
}

func TestDeleteSource_sendsRequestAndPrintsResult(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/source-repositories/vectis" {
			t.Errorf("path=%s", r.URL.Path)
		}

		w.WriteHeader(http.StatusNoContent)
	})

	var buf bytes.Buffer
	if err := deleteSourceWithOutput(&buf, "vectis", true); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{"Source repository", "vectis", "deleted"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestShowSourceStatus_sendsRequestAndPrintsStatus(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/source-repositories/vectis/status" {
			t.Errorf("path=%s", r.URL.Path)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"repository_id":        "vectis",
			"namespace":            "/",
			"source_kind":          "local_checkout",
			"enabled":              true,
			"status":               "ready",
			"checkout_mode":        "managed",
			"authoring_mode":       "local_commit",
			"authoring":            map[string]any{"mode": "local_commit", "write_definitions": true, "local_commits": true},
			"checkout_path":        "/srv/vectis/source",
			"path_exists":          true,
			"path_is_directory":    true,
			"git_repository":       true,
			"work_tree_path":       "/srv/vectis/source",
			"head_ref":             "main",
			"default_ref":          "main",
			"default_ref_resolved": true,
			"resolved_commit":      "0123456789abcdef",
			"sync":                 map[string]any{"status": "succeeded", "ref": "main", "commit": "0123456789abcdef"},
		})
	})

	var buf bytes.Buffer
	if err := showSourceStatusWithOutput(&buf, "vectis"); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{"repository_id=vectis", "status=ready", "checkout_mode=managed", "write_definitions=true", "default_ref=main", "resolved_commit=0123456789abcdef", "sync_status=succeeded"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestListSourceBranches_sendsQueryAndPrintsBranches(t *testing.T) {
	oldPrefix := sourceBranchesPrefix
	oldLimit := sourceBranchesLimit
	oldQuiet := sourceBranchesQuiet
	sourceBranchesPrefix = "feature/"
	sourceBranchesLimit = 3
	sourceBranchesQuiet = false
	t.Cleanup(func() {
		sourceBranchesPrefix = oldPrefix
		sourceBranchesLimit = oldLimit
		sourceBranchesQuiet = oldQuiet
	})

	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/source-repositories/vectis/refs/branches" {
			t.Errorf("path=%s", r.URL.Path)
		}

		q := r.URL.Query()
		if q.Get("prefix") != "feature/" || q.Get("limit") != "3" {
			t.Errorf("query=%s", r.URL.RawQuery)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"repository_id": "vectis",
			"prefix":        "feature/",
			"limit":         3,
			"branches": []map[string]any{
				{"name": "feature/source", "ref": "refs/remotes/origin/feature/source", "commit": "0123456789abcdef", "remote": "origin"},
			},
		})
	})

	var buf bytes.Buffer
	if err := listSourceBranchesWithOutput(&buf, "vectis"); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{"NAME", "feature/source", "refs/remotes/origin/feature/source", "0123456789ab", "origin"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestListSourceTree_sendsQueryAndPrintsEntries(t *testing.T) {
	oldRef := sourceTreeRef
	oldPath := sourceTreePath
	oldLimit := sourceTreeLimit
	oldRecursive := sourceTreeRecursive
	oldQuiet := sourceTreeQuiet
	sourceTreeRef = "main"
	sourceTreePath = ".vectis"
	sourceTreeLimit = 10
	sourceTreeRecursive = true
	sourceTreeQuiet = false
	t.Cleanup(func() {
		sourceTreeRef = oldRef
		sourceTreePath = oldPath
		sourceTreeLimit = oldLimit
		sourceTreeRecursive = oldRecursive
		sourceTreeQuiet = oldQuiet
	})

	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/source-repositories/vectis/tree" {
			t.Errorf("path=%s", r.URL.Path)
		}

		q := r.URL.Query()
		if q.Get("ref") != "main" || q.Get("path") != ".vectis" || q.Get("limit") != "10" || q.Get("recursive") != "true" {
			t.Errorf("query=%s", r.URL.RawQuery)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"repository_id":   "vectis",
			"requested_ref":   "main",
			"resolved_commit": "0123456789abcdef",
			"path":            ".vectis",
			"recursive":       true,
			"limit":           10,
			"entries": []map[string]any{
				{"path": ".vectis/jobs", "name": "jobs", "type": "tree", "mode": "040000", "object_sha": "abcdef0123456789"},
				{"path": ".vectis/jobs/build.json", "name": "build.json", "type": "blob", "mode": "100644", "object_sha": "fedcba9876543210", "size_bytes": 120},
			},
		})
	})

	var buf bytes.Buffer
	if err := listSourceTreeWithOutput(&buf, "vectis"); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{"PATH", ".vectis/jobs", "tree", "abcdef012345", ".vectis/jobs/build.json", "120"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestListSourceDefinitions_sendsQueryAndPrintsDefinitions(t *testing.T) {
	oldRef := sourceDefinitionsRef
	oldPath := sourceDefinitionsPath
	oldLimit := sourceDefinitionsLimit
	oldQuiet := sourceDefinitionsQuiet
	sourceDefinitionsRef = "main"
	sourceDefinitionsPath = ".vectis/jobs"
	sourceDefinitionsLimit = 7
	sourceDefinitionsQuiet = false
	t.Cleanup(func() {
		sourceDefinitionsRef = oldRef
		sourceDefinitionsPath = oldPath
		sourceDefinitionsLimit = oldLimit
		sourceDefinitionsQuiet = oldQuiet
	})

	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/source-repositories/vectis/definitions" {
			t.Errorf("path=%s", r.URL.Path)
		}

		q := r.URL.Query()
		if q.Get("ref") != "main" || q.Get("path") != ".vectis/jobs" || q.Get("limit") != "7" {
			t.Errorf("query=%s", r.URL.RawQuery)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"repository_id":   "vectis",
			"requested_ref":   "main",
			"resolved_commit": "0123456789abcdef",
			"path":            ".vectis/jobs",
			"limit":           7,
			"definitions": []map[string]any{
				{"path": ".vectis/jobs/build.json", "name": "build.json", "blob_sha": "abcdef0123456789", "size_bytes": 98},
			},
		})
	})

	var buf bytes.Buffer
	if err := listSourceDefinitionsWithOutput(&buf, "vectis"); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{"PATH", ".vectis/jobs/build.json", "abcdef012345", "98"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestResolveSourceDefinition_sendsBodyAndPrintsDefinition(t *testing.T) {
	oldRef := sourceResolveRef
	sourceResolveRef = "main"
	t.Cleanup(func() { sourceResolveRef = oldRef })

	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/source-repositories/vectis/definitions/resolve" {
			t.Errorf("path=%s", r.URL.Path)
		}

		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type=%q", got)
		}

		var body sourceDefinitionRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode body: %v", err)
		}
		if body.Ref != "main" || body.Path != ".vectis/jobs/build.json" {
			t.Errorf("resolve body mismatch: %+v", body)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"repository_id":   "vectis",
			"definition_hash": "hash",
			"definition": map[string]any{
				"root": map[string]any{
					"id":   "root",
					"uses": "builtins/shell",
					"with": map[string]any{"command": "true"},
				},
			},
			"source": map[string]any{
				"repository_id":   "vectis",
				"requested_ref":   "main",
				"resolved_commit": "0123456789abcdef",
				"path":            ".vectis/jobs/build.json",
				"blob_sha":        "abcdef",
			},
		})
	})

	cmd := &cobra.Command{}
	cmd.Flags().Bool("raw", false, "")
	var buf bytes.Buffer
	if err := resolveSourceDefinitionWithOutput(cmd, &buf, "vectis", ".vectis/jobs/build.json"); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{`"root"`, `"builtins/shell"`} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestImportSourceDefinitions_sendsBodyAndPrintsSummary(t *testing.T) {
	oldRef := sourceImportRef
	oldPath := sourceImportPath
	oldLimit := sourceImportLimit
	oldDryRun := sourceImportDryRun
	oldUpdateExisting := sourceImportUpdateExisting
	sourceImportRef = "main"
	sourceImportPath = ".vectis/jobs"
	sourceImportLimit = 5
	sourceImportDryRun = true
	sourceImportUpdateExisting = true
	t.Cleanup(func() {
		sourceImportRef = oldRef
		sourceImportPath = oldPath
		sourceImportLimit = oldLimit
		sourceImportDryRun = oldDryRun
		sourceImportUpdateExisting = oldUpdateExisting
	})

	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/source-repositories/vectis/definitions/import" {
			t.Errorf("path=%s", r.URL.Path)
		}

		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type=%q", got)
		}

		var body sourceDefinitionsImportRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode body: %v", err)
		}
		if body.Ref != "main" ||
			body.Path != ".vectis/jobs" ||
			body.Limit != 5 ||
			!body.DryRun ||
			!body.UpdateExisting {
			t.Errorf("import body mismatch: %+v", body)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"repository_id":   "vectis",
			"requested_ref":   "main",
			"resolved_commit": "0123456789abcdef",
			"path":            ".vectis/jobs",
			"limit":           5,
			"dry_run":         true,
			"update_existing": true,
			"summary":         map[string]any{"total": 1, "would_create": 1},
			"results": []map[string]any{
				{
					"job_id":          "build",
					"status":          "would_create",
					"version":         1,
					"definition_hash": "hash",
					"source": map[string]any{
						"repository_id":   "vectis",
						"requested_ref":   "main",
						"resolved_commit": "0123456789abcdef",
						"path":            ".vectis/jobs/build.json",
						"blob_sha":        "abcdef",
					},
				},
			},
		})
	})

	var buf bytes.Buffer
	if err := importSourceDefinitionsWithOutput(&buf, "vectis"); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{"repository_id=vectis", "dry_run=true", "update_existing=true", "would_create=1", "JOB ID", "build", "would_create", ".vectis/jobs/build.json"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestShowSourceJob_sendsQueryAndPrintsDefinition(t *testing.T) {
	oldRef := sourceShowRef
	oldPath := sourceShowPath
	sourceShowRef = "main"
	sourceShowPath = ".vectis/jobs/custom.json"
	t.Cleanup(func() {
		sourceShowRef = oldRef
		sourceShowPath = oldPath
	})

	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/source-repositories/vectis/jobs/build/definition" {
			t.Errorf("path=%s", r.URL.Path)
		}

		q := r.URL.Query()
		if q.Get("ref") != "main" || q.Get("path") != ".vectis/jobs/custom.json" {
			t.Errorf("query=%s", r.URL.RawQuery)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"job_id":          "build",
			"definition_hash": "hash",
			"definition": map[string]any{
				"root": map[string]any{
					"id":   "root",
					"uses": "builtins/shell",
					"with": map[string]any{"command": "true"},
				},
			},
			"source": map[string]any{
				"repository_id":   "vectis",
				"requested_ref":   "main",
				"resolved_commit": "0123456789abcdef",
				"path":            ".vectis/jobs/custom.json",
				"blob_sha":        "abcdef",
			},
		})
	})

	cmd := &cobra.Command{}
	cmd.Flags().Bool("raw", false, "")
	var buf bytes.Buffer
	if err := showSourceJobWithOutput(cmd, &buf, "vectis", "build"); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{`"root"`, `"builtins/shell"`} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestWriteSourceJob_sendsOptionsAndPrintsProvenance(t *testing.T) {
	oldRef := sourceWriteRef
	oldBranch := sourceWriteBranch
	oldPath := sourceWritePath
	oldMessage := sourceWriteMessage
	oldExpectedHead := sourceWriteExpectedHead
	oldQuiet := sourceWriteQuiet
	sourceWriteRef = "main"
	sourceWriteBranch = "feature/source-authoring"
	sourceWritePath = ".vectis/jobs/custom.json"
	sourceWriteMessage = "update build"
	sourceWriteExpectedHead = "fedcba9876543210"
	sourceWriteQuiet = false
	t.Cleanup(func() {
		sourceWriteRef = oldRef
		sourceWriteBranch = oldBranch
		sourceWritePath = oldPath
		sourceWriteMessage = oldMessage
		sourceWriteExpectedHead = oldExpectedHead
		sourceWriteQuiet = oldQuiet
	})

	definitionPath := filepath.Join(t.TempDir(), "build.json")
	if err := os.WriteFile(definitionPath, []byte(`{"root":{"id":"root","uses":"builtins/shell","with":{"command":"true"}}}`), 0o600); err != nil {
		t.Fatalf("write definition fixture: %v", err)
	}

	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/source-repositories/vectis/jobs/build/definition" {
			t.Errorf("path=%s", r.URL.Path)
		}

		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type=%q", got)
		}

		var body sourceRepositoryJobDefinitionWriteRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode body: %v", err)
		}

		if body.Ref != "main" ||
			body.Branch != "feature/source-authoring" ||
			body.Path != ".vectis/jobs/custom.json" ||
			body.Message != "update build" ||
			body.ExpectedHead != "fedcba9876543210" {
			t.Errorf("write body mismatch: %+v", body)
		}

		if !strings.Contains(string(body.Definition), `"builtins/shell"`) {
			t.Errorf("definition body=%s", string(body.Definition))
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"job_id":          "build",
			"definition_hash": "hash",
			"definition": map[string]any{
				"root": map[string]any{
					"id":   "root",
					"uses": "builtins/shell",
					"with": map[string]any{"command": "true"},
				},
			},
			"source": map[string]any{
				"repository_id":   "vectis",
				"requested_ref":   "feature/source-authoring",
				"resolved_commit": "0123456789abcdef",
				"path":            ".vectis/jobs/custom.json",
				"blob_sha":        "abcdef0123456789",
			},
		})
	})

	var buf bytes.Buffer
	if err := writeSourceJobWithOutput(&buf, "vectis", "build", definitionPath); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{"job_id=build", "commit=0123456789abcdef", "path=.vectis/jobs/custom.json", "blob_sha=abcdef0123456789", "definition_hash=hash", "requested_ref=feature/source-authoring"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestListSourceRuns_sendsQueryAndPrintsRuns(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/source-repositories/vectis/jobs/build/runs" {
			t.Errorf("path=%s", r.URL.Path)
		}

		q := r.URL.Query()
		if q.Get("limit") != "5" || q.Get("cursor") != "12" || q.Get("since") != "2026-01-01" || q.Get("cell_id") != "pdx-b" {
			t.Errorf("query=%s", r.URL.RawQuery)
		}

		next := int64(99)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{
				{
					"run_id":             "run-source",
					"run_index":          3,
					"status":             "success",
					"owning_cell":        "pdx-b",
					"created_at":         "2026-01-01T00:00:00Z",
					"definition_version": 1,
					"definition_hash":    "hash",
				},
			},
			"next_cursor": next,
		})
	})

	var buf bytes.Buffer
	if err := listSourceRunsWithOutput(&buf, "vectis", "build", 5, 12, "2026-01-01", "pdx-b"); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{"run-source", "success", "pdx-b", "More runs", "99"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestLatestRunForSourceJob_paginatesToNewestRun(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/source-repositories/vectis/jobs/build/runs" {
			t.Errorf("path=%s", r.URL.Path)
		}

		if got := r.URL.Query().Get("limit"); got != "200" {
			t.Errorf("limit=%q, want 200", got)
		}

		switch r.URL.Query().Get("cursor") {
		case "":
			next := int64(10)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data":        []map[string]any{{"run_id": "run-source-1", "run_index": 1}},
				"next_cursor": next,
			})
		case "10":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{{"run_id": "run-source-2", "run_index": 2}},
			})
		default:
			t.Errorf("unexpected cursor=%q", r.URL.Query().Get("cursor"))
			w.WriteHeader(http.StatusBadRequest)
		}
	})

	run, ok, err := latestRunForSourceJob("vectis", "build")
	if err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Fatal("expected latest run")
	}

	if run.RunID != "run-source-2" || run.RunIndex != 2 {
		t.Fatalf("unexpected latest run: %+v", run)
	}
}

func TestRunSourceLogStream_usesSourceScopedPath(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/source-repositories/vectis/jobs/build/runs/run-source/logs" {
			t.Errorf("path=%s", r.URL.Path)
		}

		if got := r.Header.Get("Accept"); got != "text/event-stream" {
			t.Errorf("Accept=%q, want text/event-stream", got)
		}

		w.Header().Set("Content-Type", "text/event-stream")
		entry := LogEntry{
			Stream: int(api.Stream_STREAM_CONTROL.Number()),
			Data:   `{"event":"completed","status":"success"}`,
		}

		b, err := json.Marshal(entry)
		if err != nil {
			t.Fatal(err)
		}

		fmt.Fprintf(w, "data: %s\n\n", b)
	})

	if err := runSourceLogStream("vectis", "build", "run-source", false, false); err != nil {
		t.Fatal(err)
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
					"cell_id":                   "pdx-b",
					"ready":                     false,
					"ingress_required":          true,
					"ingress_configured":        false,
					"ingress_reachable":         false,
					"status":                    "missing_route",
					"queued":                    3,
					"stuck":                     2,
					"task_continuation_pending": 5,
					"task_finalization_pending": 2,
					"catalog_pending":           4,
					"catalog_failed":            1,
					"catalog_total":             9,
					"error":                     "cell ingress endpoint is not configured",
					"checks": []map[string]string{
						{"id": "ingress", "status": "fail"},
						{"id": "dispatch", "status": "warn"},
						{"id": "catalog", "status": "fail"},
					},
				},
				{
					"cell_id":                   "iad-a",
					"ready":                     true,
					"ingress_required":          true,
					"ingress_configured":        true,
					"ingress_reachable":         true,
					"status":                    "ready",
					"queued":                    1,
					"stuck":                     0,
					"task_continuation_pending": 0,
					"task_finalization_pending": 0,
					"catalog_pending":           0,
					"catalog_failed":            0,
					"catalog_total":             5,
				},
			},
		})
	})

	var buf bytes.Buffer
	if err := cellsStatus(&buf); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{"CELL", "READY", "ROUTE", "TASK REPAIR C/F", "CATALOG P/F/T", "CHECKS", "iad-a", "yes", "ready", "0/0", "0/0/5", "pdx-b", "no", "missing_route", "5/2", "4/1/9", "ingress:fail,dispatch:warn,catalog:fail"} {
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
					"cell_id":                   "iad-a",
					"ready":                     true,
					"ingress_required":          true,
					"ingress_configured":        true,
					"ingress_reachable":         true,
					"status":                    "ready",
					"queued":                    1,
					"stuck":                     0,
					"task_continuation_pending": 2,
					"task_finalization_pending": 1,
					"catalog_total":             5,
					"checks": []map[string]string{
						{"id": "ingress", "status": "pass"},
					},
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

	if len(result.Cells) != 1 || result.Cells[0].CellID != "iad-a" || !result.Cells[0].Ready || result.Cells[0].TaskContinuationPending != 2 || result.Cells[0].TaskFinalizationPending != 1 || result.Cells[0].CatalogTotal != 5 || len(result.Cells[0].Checks) != 1 {
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
	queueDir := filepath.Join(tmpDir, "custom", "queue")
	logDir := filepath.Join(tmpDir, "custom", "log")
	spoolDir := filepath.Join(tmpDir, "custom", "log-forwarder", "spool")
	artifactDir := filepath.Join(tmpDir, "custom", "artifact")

	t.Setenv("HOME", tmpDir)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(tmpDir, "config"))
	t.Setenv("XDG_DATA_HOME", dataDir)
	t.Setenv("XDG_CACHE_HOME", cacheDir)
	t.Setenv(envDeployConfigDir, deployDir)
	t.Setenv("VECTIS_QUEUE_PERSISTENCE_DIR", queueDir)
	t.Setenv("VECTIS_LOG_STORAGE_DIR", logDir)
	t.Setenv("VECTIS_LOG_FORWARDER_SPOOL_DIR", spoolDir)
	t.Setenv("VECTIS_ARTIFACT_STORAGE_DIR", artifactDir)

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
		artifactDir,
		filepath.Join(cacheHome, "vectis"),
		filepath.Join(configDir, "vectis"),
		filepath.Join(dataDir, "vectis"),
		filepath.Join(deployDir, "podman"),
		logDir,
		queueDir,
		spoolDir,
	}
	sort.Strings(want)

	if strings.Join(targets, "\n") != strings.Join(want, "\n") {
		t.Fatalf("targets mismatch\ngot:\n%s\nwant:\n%s", strings.Join(targets, "\n"), strings.Join(want, "\n"))
	}
}

func TestResetTargetsSkipsCoveredAndDisabledStoragePaths(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	t.Setenv("HOME", tmpDir)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(tmpDir, "config"))
	t.Setenv("XDG_DATA_HOME", dataDir)
	t.Setenv("XDG_CACHE_HOME", filepath.Join(tmpDir, "cache"))
	t.Setenv("VECTIS_QUEUE_PERSISTENCE_DIR", "")
	t.Setenv("VECTIS_LOG_FORWARDER_SPOOL_DIR", filepath.Join(dataDir, "vectis", "log-forwarder", "spool"))

	targets, err := resetTargets()
	if err != nil {
		t.Fatalf("reset targets: %v", err)
	}

	covered := filepath.Join(dataDir, "vectis", "log-forwarder", "spool")
	for _, target := range targets {
		if target == covered {
			t.Fatalf("covered spool target %q should be omitted when %q is already reset; targets=%v", covered, filepath.Join(dataDir, "vectis"), targets)
		}
		if target == "" {
			t.Fatalf("empty queue persistence env should not add an empty target; targets=%v", targets)
		}
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

	if strings.TrimSpace(secrets.EncryptedFSKey) == "" {
		t.Fatalf("encryptedfs key was not generated")
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

const (
	testPodmanSmokeSecretRef       = "encryptedfs://team/smoke-token"
	testPodmanSmokeSecretPlaintext = "spiffe-secret"
	testPodmanSmokeSecretRoot      = "/data/vectis/secrets/encryptedfs"
	testPodmanSmokeSecretKeyFile   = "/secrets/encryptedfs.key"
	testPodmanSmokePollInterval    = 10 * time.Millisecond
)

type testCommandRunner func(context.Context, string, []string, string) (string, string, error)

type podmanSmokeTestResult struct {
	SecretRef string
	JobID     string
	RunID     string
	RunIndex  int
	RunStatus string
}

func runPodmanSecretsSmokeTestStep(ctx context.Context, jobBody []byte, timeout time.Duration, runner testCommandRunner) (podmanSmokeTestResult, error) {
	if timeout <= 0 {
		return podmanSmokeTestResult{}, fmt.Errorf("timeout must be positive")
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if _, stderr, err := testSeedPodmanExampleSecret(ctx, runner); err != nil {
		if strings.TrimSpace(stderr) != "" {
			return podmanSmokeTestResult{}, fmt.Errorf("seed podman smoke secret: %w: %s", err, strings.TrimSpace(stderr))
		}

		return podmanSmokeTestResult{}, fmt.Errorf("seed podman smoke secret: %w", err)
	}

	submitted, err := submitJobDefinitionBody(jobBody, "", "")
	if err != nil {
		return podmanSmokeTestResult{}, fmt.Errorf("submit podman secrets smoke job: %w", err)
	}

	run, err := waitForTestRunTerminal(ctx, submitted.RunID, testPodmanSmokePollInterval)
	if err != nil {
		return podmanSmokeTestResult{}, err
	}

	if !testRunStatusSucceeded(run.Status) {
		return podmanSmokeTestResult{}, fmt.Errorf("podman secrets smoke run %s finished with status %s", submitted.RunID, run.Status)
	}

	return podmanSmokeTestResult{
		SecretRef: testPodmanSmokeSecretRef,
		JobID:     submitted.JobID,
		RunID:     submitted.RunID,
		RunIndex:  submitted.RunIndex,
		RunStatus: run.Status,
	}, nil
}

func testSeedPodmanExampleSecret(ctx context.Context, runner testCommandRunner) (string, string, error) {
	args := []string{
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
		testPodmanSmokeSecretRef,
		"--root",
		testPodmanSmokeSecretRoot,
		"--key-file",
		testPodmanSmokeSecretKeyFile,
		"--force",
	}

	return runner(ctx, "podman", args, testPodmanSmokeSecretPlaintext)
}

func waitForTestRunTerminal(ctx context.Context, runID string, interval time.Duration) (runDetail, error) {
	for {
		run, err := fetchRunDetail(runID)
		if err != nil {
			return runDetail{}, fmt.Errorf("fetch smoke run status: %w", err)
		}

		if testRunStatusTerminal(run.Status) {
			return run, nil
		}

		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return runDetail{}, fmt.Errorf("timed out waiting for podman secrets smoke run %s: %w", runID, ctx.Err())
		case <-timer.C:
		}
	}
}

func testRunStatusSucceeded(status string) bool {
	return strings.TrimSpace(strings.ToLower(status)) == "succeeded"
}

func testRunStatusTerminal(status string) bool {
	switch strings.TrimSpace(strings.ToLower(status)) {
	case "succeeded", "failed", "orphaned", "cancelled", "abandoned", "aborted":
		return true
	default:
		return false
	}
}

func TestSeedPodmanExampleSecretUsesPodVolumesAndPlaintext(t *testing.T) {
	var gotName, gotStdin string
	var gotArgs []string

	stdout, stderr, err := testSeedPodmanExampleSecret(context.Background(), func(ctx context.Context, name string, args []string, stdin string) (string, string, error) {
		gotName = name
		gotArgs = append([]string(nil), args...)
		gotStdin = stdin
		return "seeded\n", "", nil
	})

	if err != nil {
		t.Fatal(err)
	}

	if stdout != "seeded\n" || stderr != "" {
		t.Fatalf("unexpected command output: stdout=%q stderr=%q", stdout, stderr)
	}

	wantArgs := []string{
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
		testPodmanSmokeSecretRef,
		"--root",
		testPodmanSmokeSecretRoot,
		"--key-file",
		testPodmanSmokeSecretKeyFile,
		"--force",
	}

	if gotName != "podman" {
		t.Fatalf("command name = %q, want podman", gotName)
	}

	if strings.Join(gotArgs, "\x00") != strings.Join(wantArgs, "\x00") {
		t.Fatalf("args mismatch\ngot:  %#v\nwant: %#v", gotArgs, wantArgs)
	}

	if gotStdin != testPodmanSmokeSecretPlaintext {
		t.Fatalf("stdin = %q, want smoke plaintext", gotStdin)
	}
}

func loadPodmanSecretsSmokeExample(t *testing.T) []byte {
	t.Helper()

	example, err := os.ReadFile(filepath.Join("..", "..", "examples", "secrets.json"))
	if err != nil {
		t.Fatal(err)
	}

	return example
}

func TestRunPodmanSmokeSecretsSeedsSubmitsAndWaits(t *testing.T) {
	var requestedPaths []string
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		requestedPaths = append(requestedPaths, r.Method+" "+r.URL.Path)

		switch r.URL.Path {
		case "/api/v1/jobs/run":
			if r.Method != http.MethodPost {
				t.Errorf("method=%s", r.Method)
			}

			var job map[string]any
			if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
				t.Errorf("decode job body: %v", err)
			}

			if _, ok := job["root"].(map[string]any); !ok {
				t.Errorf("expected raw job definition body, got root=nil")
			}

			if job["id"] != "secret-example" {
				t.Errorf("job id=%v, want secret-example", job["id"])
			}

			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id":        "job-ephemeral",
				"run_id":    "run-1",
				"run_index": 7,
			})
		case "/api/v1/runs/run-1":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"run_id":    "run-1",
				"run_index": 7,
				"status":    "succeeded",
			})
		default:
			t.Errorf("unexpected path: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	})

	var commandRan bool
	result, err := runPodmanSecretsSmokeTestStep(context.Background(), loadPodmanSecretsSmokeExample(t), time.Second, func(ctx context.Context, name string, args []string, stdin string) (string, string, error) {
		commandRan = true
		if name != "podman" {
			t.Errorf("command name=%q", name)
		}
		if stdin != testPodmanSmokeSecretPlaintext {
			t.Errorf("stdin=%q", stdin)
		}
		return "", "", nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if !commandRan {
		t.Fatal("expected podman seed command to run")
	}

	if strings.Join(requestedPaths, ",") != "POST /api/v1/jobs/run,GET /api/v1/runs/run-1" {
		t.Fatalf("unexpected API requests: %v", requestedPaths)
	}

	if result.SecretRef != testPodmanSmokeSecretRef || result.RunID != "run-1" || result.RunIndex != 7 || result.RunStatus != "succeeded" {
		t.Fatalf("unexpected smoke result: %+v", result)
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
	assertEnv(t, findContainer(t, pod, "artifact-2"), "VECTIS_ARTIFACT_STORAGE_DIR", "/data/vectis/artifact/artifact-2")
	assertEnv(t, findContainer(t, pod, "orchestrator"), "VECTIS_ORCHESTRATOR_ADVERTISE_ADDRESS", "127.0.0.1:8087")
	assertEnv(t, findContainer(t, pod, "worker-2"), "VECTIS_WORKER_METRICS_PORT", "9182")
	assertEnv(t, findContainer(t, pod, "worker-core"), "VECTIS_WORKER_CORE_SOCKET", "/run/vectis/worker-core/worker-core.sock")
	assertEnv(t, findContainer(t, pod, "worker"), "VECTIS_WORKER_CORE_SOCKET", "/run/vectis/worker-core/worker-core.sock")
	assertEnv(t, findContainer(t, pod, "worker"), "VECTIS_WORKER_CORE_SHELL_SOCKET", "/run/vectis/worker-core/worker-shell.sock")
	assertEnv(t, findContainer(t, pod, "worker-2"), "VECTIS_WORKER_CORE_SHELL_SOCKET", "/run/vectis/worker-core/worker-2-shell.sock")
	assertEnv(t, findContainer(t, pod, "worker-2"), "VECTIS_WORKER_SECRETS_ADDRESS", "127.0.0.1:8090")
	assertEnv(t, findContainer(t, pod, "cron-2"), "VECTIS_CRON_INSTANCE_ID", "cron-2")
	assertEnv(t, findContainer(t, pod, "reconciler-2"), "VECTIS_RECONCILER_METRICS_PORT", "9185")
	findContainer(t, pod, "registry-3")

	if _, ok := envMap(t, findContainer(t, pod, "worker"))["VECTIS_CRON_INSTANCE_ID"]; ok {
		t.Fatalf("base worker inherited cron instance env")
	}

	assertStringSlice(t, prometheusTargets(t, docs, "vectis-queue"), []string{"127.0.0.1:9081", "127.0.0.1:9181"})
	assertStringSlice(t, prometheusTargets(t, docs, "vectis-artifact"), []string{"127.0.0.1:9089", "127.0.0.1:9189"})
	assertStringSlice(t, prometheusTargets(t, docs, "vectis-orchestrator"), []string{"127.0.0.1:9090"})
	assertStringSlice(t, prometheusTargets(t, docs, "vectis-secrets"), []string{"127.0.0.1:9091"})
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

	for _, name := range []string{"registry-2", "api-2", "queue-2", "log-2", "artifact-2", "orchestrator-2", "worker-2"} {
		if findContainerOK(t, pod, name) {
			t.Fatalf("simple manifest unexpectedly included container %s", name)
		}
	}

	assertEnv(t, findContainer(t, pod, "orchestrator"), "VECTIS_ORCHESTRATOR_ADVERTISE_ADDRESS", "127.0.0.1:8087")
	assertEnv(t, findContainer(t, pod, "worker-core"), "VECTIS_WORKER_CORE_SOCKET", "/run/vectis/worker-core/worker-core.sock")
	assertEnv(t, findContainer(t, pod, "worker"), "VECTIS_WORKER_CORE_SHELL_SOCKET", "/run/vectis/worker-core/worker-shell.sock")
	findInitContainer(t, pod, "vectis-spiffe-init")
	findInitContainer(t, pod, "vectis-client-ca-bundle-init")
	findContainer(t, pod, "spiffe")
	assertEnv(t, findContainer(t, pod, "secrets"), "VECTIS_GRPC_TLS_CLIENT_CA_FILE", "/run/vectis/grpc-tls/client-ca-bundle.pem")
	assertEnv(t, findContainer(t, pod, "secrets"), "VECTIS_SECRETS_ENCRYPTEDFS_KEY_FILE", "/run/vectis/secrets/encryptedfs.key")
	assertEnv(t, findContainer(t, pod, "worker"), "VECTIS_WORKER_SPIFFE_ENABLED", "true")
	assertEnv(t, findContainer(t, pod, "worker"), "VECTIS_WORKER_SPIFFE_WORKLOAD_API_ADDRESS", "unix:///run/vectis/spiffe/workload.sock")
	assertEnv(t, findContainer(t, pod, "worker"), "VECTIS_WORKER_SPIFFE_REGISTRATION_SERVER_ADDRESS", "unix:///run/vectis/spiffe/registration.sock")
	assertEnv(t, findContainer(t, pod, "worker"), "VECTIS_WORKER_SECRETS_ADDRESS", "127.0.0.1:8090")
	assertStringSlice(t, prometheusTargets(t, docs, "vectis-queue"), []string{"127.0.0.1:9081"})
	assertStringSlice(t, prometheusTargets(t, docs, "vectis-artifact"), []string{"127.0.0.1:9089"})
	assertStringSlice(t, prometheusTargets(t, docs, "vectis-orchestrator"), []string{"127.0.0.1:9090"})
	assertStringSlice(t, prometheusTargets(t, docs, "vectis-secrets"), []string{"127.0.0.1:9091"})
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

func findInitContainer(t *testing.T, pod map[string]any, name string) map[string]any {
	t.Helper()

	if container, ok := lookupPodContainer(pod, "initContainers", name); ok {
		return container
	}

	t.Fatalf("init container %s not found", name)
	return nil
}

func findContainerOK(t *testing.T, pod map[string]any, name string) bool {
	t.Helper()

	_, ok := lookupContainer(pod, name)
	return ok
}

func lookupContainer(pod map[string]any, name string) (map[string]any, bool) {
	return lookupPodContainer(pod, "containers", name)
}

func lookupPodContainer(pod map[string]any, field, name string) (map[string]any, bool) {
	spec, ok := pod["spec"].(map[string]any)
	if !ok {
		return nil, false
	}

	containers, ok := spec[field].([]any)
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
			"next_action":    "security_gate_failed",
			"owning_cell":    "pdx-b",
			"failure_code":   "execution",
			"failure_reason": "exit code 1",
			"latest_failed_security_event": map[string]any{
				"id":              1,
				"run_id":          "run-1",
				"task_id":         "run-1:root",
				"task_attempt_id": "run-1:root:attempt:1",
				"execution_id":    "execution-1",
				"event_type":      "secret_resolution",
				"outcome":         "denied",
				"reason":          "authorization_denied",
				"provider":        "encryptedfs",
				"secret_count":    1,
				"created_at":      123,
			},
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
		"next_action=security_gate_failed",
		"owning_cell=pdx-b",
		"latest_failed_security_event=secret_resolution:denied/authorization_denied provider=encryptedfs secrets=1",
		"retry_guidance=fix_security_gate_before_retry_or_replay",
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

func TestGetRun_successIncludesTaskCompletion(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/runs/run-1" {
			t.Errorf("path=%s", r.URL.Path)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"run_id":      "run-1",
			"run_index":   3,
			"status":      "queued",
			"next_action": "task_completion_pending",
			"owning_cell": "pdx-b",
			"task_completion": map[string]any{
				"total":           3,
				"succeeded":       1,
				"terminal_failed": 1,
				"incomplete":      1,
			},
		})
	})

	var buf bytes.Buffer
	if err := getRun("run-1", &buf); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{
		"next_action=task_completion_pending",
		"task_completion: total=3 succeeded=1 terminal_failed=1 incomplete=1",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestGetRun_successIncludesDispatchSummary(t *testing.T) {
	lastFailure := "queue unavailable"
	cronAt := time.Date(2026, 6, 13, 12, 30, 0, 0, time.UTC).Unix()
	reconcilerAt := time.Date(2026, 6, 13, 12, 35, 0, 0, time.UTC).Unix()
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/runs/run-1" {
			t.Errorf("path=%s", r.URL.Path)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"run_id":      "run-1",
			"run_index":   3,
			"status":      "queued",
			"owning_cell": "pdx-b",
			"dispatch_summary": []map[string]any{
				{
					"source":          "cron",
					"attempts":        1,
					"failures":        1,
					"first_event_at":  cronAt,
					"last_event_at":   cronAt,
					"last_event_type": "failure",
					"last_message":    lastFailure,
				},
				{
					"source":          "reconciler",
					"attempts":        1,
					"successes":       1,
					"first_event_at":  reconcilerAt,
					"last_event_at":   reconcilerAt,
					"last_event_type": "success",
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
		"dispatch_summary:",
		"cron: accepted=0 attempts=1 successes=0 failures=1 last=failure at 2026-06-13T12:30:00Z: queue unavailable",
		"reconciler: accepted=0 attempts=1 successes=1 failures=0 last=success at 2026-06-13T12:35:00Z",
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
							"attempt_id":       "attempt-child",
							"task_id":          "run-1:child",
							"run_id":           "run-1",
							"execution_id":     "attempt-child:execution",
							"execution_status": "running",
							"cell_id":          "pdx-b",
							"lease_owner":      "worker-a",
							"lease_until":      1780000000,
							"attempt":          1,
							"status":           "running",
							"accepted_at":      "2026-06-01T12:01:00Z",
							"started_at":       "2026-06-01T12:01:01Z",
							"event_sequence":   2,
							"security_events": []map[string]any{
								{
									"id":              1,
									"run_id":          "run-1",
									"task_id":         "run-1:child",
									"task_attempt_id": "attempt-child",
									"execution_id":    "attempt-child:execution",
									"event_type":      "secret_resolution",
									"outcome":         "success",
									"reason":          "ok",
									"provider":        "encryptedfs",
									"secret_count":    2,
									"file_count":      1,
									"created_at":      1780000001,
								},
							},
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
		"attempt=1 id=attempt-child cell=pdx-b status=running event_sequence=2 execution_id=attempt-child:execution execution_status=running lease_owner=worker-a lease_until=2026-05-28T20:26:40Z accepted_at=2026-06-01T12:01:00Z started_at=2026-06-01T12:01:01Z security=[secret_resolution:success/ok provider=encryptedfs secrets=2 files=1]",
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

func TestGetRunArtifacts_success(t *testing.T) {
	next := int64(27)
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/runs/run-1/artifacts" {
			t.Errorf("path=%s", r.URL.Path)
		}

		if got := r.URL.Query().Get("limit"); got != "2" {
			t.Errorf("limit=%q, want 2", got)
		}

		if got := r.URL.Query().Get("cursor"); got != "9" {
			t.Errorf("cursor=%q, want 9", got)
		}

		if got := r.URL.Query().Get("task_id"); got != "task-a" {
			t.Errorf("task_id=%q, want task-a", got)
		}

		if got := r.URL.Query().Get("task_attempt_id"); got != "attempt-a" {
			t.Errorf("task_attempt_id=%q, want attempt-a", got)
		}

		if got := r.URL.Query().Get("execution_id"); got != "execution-a" {
			t.Errorf("execution_id=%q, want execution-a", got)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{
				{
					"id":                10,
					"run_id":            "run-1",
					"task_id":           "task-a",
					"task_attempt_id":   "attempt-a",
					"execution_id":      "execution-a",
					"name":              "coverage",
					"path":              "coverage/out.json",
					"content_type":      "application/json",
					"blob_key":          "sha256:aaaaaaaa",
					"blob_algorithm":    "sha256",
					"blob_digest":       "aaaaaaaa",
					"size_bytes":        123,
					"artifact_shard_id": "artifact-1",
					"metadata":          map[string]string{"kind": "coverage"},
				},
			},
			"next_cursor": next,
		})
	})

	var buf bytes.Buffer
	if err := getRunArtifacts("run-1", runArtifactsListOptions{
		Limit:         2,
		Cursor:        9,
		TaskID:        " task-a ",
		TaskAttemptID: "attempt-a",
		ExecutionID:   "execution-a",
	}, &buf); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	for _, want := range []string{
		"NAME",
		"TASK",
		"ATTEMPT",
		"EXECUTION",
		"coverage",
		"task-a",
		"attempt-a",
		"execution-a",
		"coverage/out.json",
		"application/json",
		"artifact-1",
		"More artifacts available. Continue with --cursor 27.",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestGetRunArtifacts_jsonOutput(t *testing.T) {
	withOutputFormat(t, outputJSON)
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{
				{
					"id":                10,
					"run_id":            "run-1",
					"name":              "coverage",
					"path":              "coverage/out.json",
					"blob_key":          "sha256:aaaaaaaa",
					"blob_algorithm":    "sha256",
					"blob_digest":       "aaaaaaaa",
					"size_bytes":        123,
					"artifact_shard_id": "artifact-1",
					"metadata":          map[string]string{"kind": "coverage"},
				},
			},
		})
	})

	var buf bytes.Buffer
	if err := getRunArtifacts("run-1", runArtifactsListOptions{}, &buf); err != nil {
		t.Fatal(err)
	}

	var result runArtifactsResult
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("invalid JSON output: %v\n%s", err, buf.String())
	}

	if len(result.Data) != 1 || result.Data[0].Name != "coverage" || !strings.Contains(string(result.Data[0].Metadata), "coverage") {
		t.Fatalf("unexpected artifacts JSON: %+v", result)
	}
}

func TestDownloadRunArtifact_file(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/runs/run-1/artifacts/coverage/download" {
			t.Errorf("path=%s", r.URL.Path)
		}

		if got := r.Header.Get("Accept"); got != "*/*" {
			t.Errorf("Accept=%q, want */*", got)
		}

		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("artifact bytes"))
	})

	outputPath := filepath.Join(t.TempDir(), "coverage.txt")
	var buf bytes.Buffer
	if err := downloadRunArtifact("run-1", "coverage", outputPath, &buf); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read downloaded file: %v", err)
	}

	if string(got) != "artifact bytes" {
		t.Fatalf("downloaded file = %q", got)
	}

	if out := buf.String(); !strings.Contains(out, "Downloaded artifact coverage") || !strings.Contains(out, "14 bytes") {
		t.Fatalf("unexpected download output:\n%s", out)
	}
}

func TestDownloadRunArtifact_stdout(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("raw bytes"))
	})

	var buf bytes.Buffer
	if err := downloadRunArtifact("run-1", "coverage", "-", &buf); err != nil {
		t.Fatal(err)
	}

	if buf.String() != "raw bytes" {
		t.Fatalf("stdout download = %q", buf.String())
	}
}

func TestDownloadRunArtifact_requiresOutput(t *testing.T) {
	if err := downloadRunArtifact("run-1", "coverage", "", io.Discard); err == nil {
		t.Fatal("expected missing output error")
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

		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["username"] != "admin" || body["password"] != "secret" || body["return_token"] != true {
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

func TestDoLogout_success(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/logout" {
			t.Errorf("path=%s", r.URL.Path)
		}

		if got := r.Header.Get("Authorization"); got != "Bearer session-token" {
			t.Errorf("Authorization=%q", got)
		}

		w.WriteHeader(http.StatusNoContent)
	})

	if err := doLogout("session-token"); err != nil {
		t.Fatal(err)
	}
}

func TestDoLogout_unauthorizedIgnored(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	})

	if err := doLogout("stale-token"); err != nil {
		t.Fatal(err)
	}
}

func TestDoLogout_emptyToken(t *testing.T) {
	if err := doLogout(""); err != nil {
		t.Fatal(err)
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
		case "/api/v1/cron/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"schedule_count": 0, "due_count": 0, "claimed_count": 0})
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
		"Overall: PASS  20 passed, 0 warnings, 0 failed",
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
		"Cron",
		"OK    Schedules",
		"no enabled cron schedules",
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
		"Artifacts",
		"OK    Artifact storage",
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

	for _, path := range []string{"/health/live", "/health/ready", "/api/v1/setup/status", "/api/v1/schema/status", "/api/v1/reconciler/heartbeat", "/api/v1/audit/drops", "/api/v1/db/pool-stats", "/api/v1/queue/backlog", "/api/v1/reconciler/stuck-runs", "/api/v1/cron/status", "/api/v1/cells/status", "/api/v1/log/reachable", "/api/v1/audit/flush-failures", "/api/v1/catalog/status"} {
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
		case "/api/v1/cron/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"schedule_count": 0, "due_count": 0, "claimed_count": 0})
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
		"Overall: WARN  18 passed, 2 warnings, 0 failed",
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
		case "/api/v1/cron/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"schedule_count": 0, "due_count": 0, "claimed_count": 0})
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
		"Overall: PASS  20 passed, 0 warnings, 0 failed",
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
		case "/api/v1/cron/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"schedule_count": 0, "due_count": 0, "claimed_count": 0})
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
	if !strings.Contains(out, "Overall: FAIL  19 passed, 0 warnings, 1 failed") ||
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
		case "/api/v1/cron/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"schedule_count": 0, "due_count": 0, "claimed_count": 0})
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

	if report.Status != doctorOK || report.Passed != 20 || report.Warnings != 0 || report.Failed != 0 {
		t.Fatalf("unexpected report summary: %+v", report)
	}

	if len(report.Checks) != 20 {
		t.Fatalf("expected 20 checks, got %d", len(report.Checks))
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
		case "/api/v1/cron/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"schedule_count": 0, "due_count": 0, "claimed_count": 0})
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

	if len(report.Checks) != 20 {
		t.Fatalf("expected 20 checks, got %d", len(report.Checks))
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
		case "/api/v1/cron/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"schedule_count": 0, "due_count": 0, "claimed_count": 0})
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

func TestDoctor_cronSchedulesWarnsForDueSchedules(t *testing.T) {
	oldest := time.Date(2026, 6, 13, 12, 30, 0, 0, time.UTC).Unix()
	check := doctorCheckCronStatusResponse(t, map[string]any{
		"schedule_count":  3,
		"due_count":       2,
		"claimed_count":   1,
		"oldest_due_unix": oldest,
		"active":          true,
	})

	if check.Status != doctorWarn {
		t.Fatalf("expected due cron schedules to warn, got %#v", check)
	}

	if !strings.Contains(check.Summary, "2 cron schedules are due for dispatch") {
		t.Fatalf("unexpected summary: %q", check.Summary)
	}

	for _, want := range []string{"schedules=3", "due=2", "claimed=1", "oldest_due=2026-06-13T12:30:00Z"} {
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

func TestDoctor_stuckRunsWarnsForPendingTaskFinalization(t *testing.T) {
	check := doctorCheckStuckRunsResponse(t, map[string]any{
		"stuck":                     0,
		"task_finalization_pending": 2,
		"task_finalization_cells": []map[string]any{
			{"cell_id": "pdx-b", "pending": 2},
		},
	})

	if check.Status != doctorWarn {
		t.Fatalf("expected pending task finalization to warn, got %#v", check)
	}

	if !strings.Contains(check.Summary, "2 pending task finalizations detected") {
		t.Fatalf("unexpected summary: %q", check.Summary)
	}

	for _, want := range []string{"stuck=0", "task_finalization_pending=2", "task_finalization_cells=pdx-b:2"} {
		if !strings.Contains(check.Evidence, want) {
			t.Fatalf("expected evidence to contain %q, got %q", want, check.Evidence)
		}
	}
}

func TestDoctor_stuckRunsWarnsForPendingTaskContinuation(t *testing.T) {
	check := doctorCheckStuckRunsResponse(t, map[string]any{
		"stuck":                     1,
		"task_continuation_pending": 2,
		"task_continuation_cells": []map[string]any{
			{"cell_id": "pdx-b", "pending": 2},
		},
	})

	if check.Status != doctorWarn {
		t.Fatalf("expected pending task continuation to warn, got %#v", check)
	}

	if !strings.Contains(check.Summary, "2 pending task continuations") {
		t.Fatalf("unexpected summary: %q", check.Summary)
	}

	for _, want := range []string{"stuck=1", "task_continuation_pending=2", "task_continuation_cells=pdx-b:2"} {
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

func doctorCheckCronStatusResponse(t *testing.T, body map[string]any) doctorCheck {
	t.Helper()
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/cron/status" {
			t.Errorf("unexpected path=%s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		_ = json.NewEncoder(w).Encode(body)
	})

	return doctorCronSchedules()
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
