//go:build e2e

package local_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestE2ELocalArtifactsExample(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("example requires sh and tar")
	}

	root := repoRoot(t)
	cli := e2eBinaryPath(t, root, "VECTIS_E2E_CLI", "vectis-cli")
	local := e2eBinaryPath(t, root, "VECTIS_E2E_LOCAL", "vectis-local")
	ports := localRestoreDrillPortSet()
	requireLocalPortsAvailable(t, append(ports.all(), 19092))

	dataHome := t.TempDir()
	env := localRestoreDrillEnvWithOverrides(localRestoreDrillEnv(t, root, dataHome, ports), map[string]string{
		"VECTIS_WORKER_CORE_METRICS_PORT": "19092", "VECTIS_CATALOG_INTERVAL": "100ms", "VECTIS_RECONCILER_INTERVAL": "100ms",
	})

	prepareLocalRestoreDrillEvidenceDirs(t, dataHome)
	proc := startVectisLocal(t, root, env, local, "--auth=false", "--setup=off", "--ui=false")
	t.Cleanup(func() { stopVectisLocal(t, proc) })
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("local stderr:\n%s", proc.stderr.String())
		}
	})

	waitForAPIReady(t, proc, ports.readyURL(), 2*time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	result, err := runCommand(ctx, root, env, nil, cli, "--format", "json", "jobs", "run", filepath.Join(root, "examples", "artifacts.json"))
	if err != nil {
		t.Fatal(err)
	}

	var submitted jobRunResult
	if err := json.Unmarshal([]byte(result.stdout), &submitted); err != nil {
		t.Fatal(err)
	}

	_, final := waitForRunTerminal(t, root, env, cli, submitted.RunID, 60*time.Second)
	if final.Status != "succeeded" {
		t.Fatalf("artifact demo status=%s", final.Status)
	}

	get := func(path string) (http.Header, []byte) {
		t.Helper()
		requestCtx, requestCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer requestCancel()

		req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, fmt.Sprintf("http://localhost:%d%s", ports.api, path), nil)
		if err != nil {
			t.Fatal(err)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
		if err != nil {
			t.Fatal(err)
		}

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s: %s: %s", path, resp.Status, body)
		}

		return resp.Header.Clone(), body
	}

	var manifests struct {
		Data []struct {
			Name          string `json:"name"`
			TaskID        string `json:"task_id"`
			TaskAttemptID string `json:"task_attempt_id"`
			ExecutionID   string `json:"execution_id"`
			SizeBytes     int64  `json:"size_bytes"`
		} `json:"data"`
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		_, body := get("/api/v1/runs/" + submitted.RunID + "/artifacts")
		if err := json.Unmarshal(body, &manifests); err != nil {
			t.Fatal(err)
		}

		if len(manifests.Data) == 2 {
			break
		}

		if time.Now().After(deadline) {
			t.Fatalf("expected two artifacts: %s", body)
		}

		time.Sleep(100 * time.Millisecond)
	}

	producers := map[string]bool{}
	for _, artifact := range manifests.Data {
		if artifact.TaskID == "" || artifact.TaskAttemptID == "" || artifact.ExecutionID == "" || artifact.SizeBytes <= 0 {
			t.Fatalf("missing artifact metadata: %+v", artifact)
		}

		producers[artifact.TaskID] = true
	}

	if len(producers) != 2 {
		t.Fatalf("expected two producing tasks, got %v", producers)
	}

	headers, report := get("/api/v1/runs/" + submitted.RunID + "/artifacts/demo-report/download")
	if string(report) != "Vectis artifact demo\nAll demonstration checks passed.\n" {
		t.Fatalf("unexpected report: %q", report)
	}

	if !strings.Contains(headers.Get("Content-Disposition"), "report.txt") {
		t.Fatalf("unexpected filename: %s", headers.Get("Content-Disposition"))
	}

	_, archive := get("/api/v1/runs/" + submitted.RunID + "/artifacts/demo-bundle/download")
	compressed, err := gzip.NewReader(bytes.NewReader(archive))
	if err != nil {
		t.Fatal(err)
	}
	defer compressed.Close()

	entries := tar.NewReader(compressed)
	header, err := entries.Next()
	if err != nil {
		t.Fatal(err)
	}

	if header.Name != "notes.txt" {
		t.Fatalf("archive entry: %s", header.Name)
	}

	note, err := io.ReadAll(io.LimitReader(entries, 1024))
	if err != nil || string(note) != "This file came from the Vectis artifact demo.\n" {
		t.Fatalf("archive contents: %q, %v", note, err)
	}
}
