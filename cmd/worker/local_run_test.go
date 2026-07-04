package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func writeLocalRunJob(t *testing.T, dir, command string) string {
	t.Helper()

	path := filepath.Join(dir, "job.json")
	body := `{
  "id": "local-test",
  "root": {
    "id": "script",
    "uses": "builtins/script",
    "with": {
      "script": ` + strconv.Quote(command) + `
    }
  }
}
`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write job: %v", err)
	}

	return path
}

func TestRunLocalJobSuccess(t *testing.T) {
	workspace := t.TempDir()
	jobPath := writeLocalRunJob(t, t.TempDir(), localRunSuccessScript())
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	if err := runLocalJob(context.Background(), jobPath, workspace, &stdout, &stderr); err != nil {
		t.Fatalf("run local job: %v\nstderr:\n%s", err, stderr.String())
	}

	if !strings.Contains(stdout.String(), "local-ok") {
		t.Fatalf("expected stdout log to contain script output, got %q", stdout.String())
	}

	if _, err := os.Stat(workspace); err != nil {
		t.Fatalf("expected workspace to remain: %v", err)
	}
}

func TestRunLocalJobDecodesFriendlySecretDeliveryType(t *testing.T) {
	workspace := t.TempDir()
	dir := t.TempDir()
	jobPath := filepath.Join(dir, "job.json")
	body := `{
  "id": "local-secret-alias",
  "secrets": [
    {
      "id": "token",
      "ref": "encryptedfs://team/token",
      "delivery": {"type": "file", "path": "token"},
      "task_keys": ["script"]
    }
  ],
  "root": {
    "id": "script",
    "uses": "builtins/script",
    "with": {"script": ` + strconv.Quote(localRunSecretScript()) + `}
  }
}`

	if err := os.WriteFile(jobPath, []byte(body), 0o644); err != nil {
		t.Fatalf("write job: %v", err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := runLocalJob(context.Background(), jobPath, workspace, &stdout, &stderr); err != nil {
		t.Fatalf("run local job: %v\nstderr:\n%s", err, stderr.String())
	}

	if !strings.Contains(stdout.String(), "local-secret-alias-ok") {
		t.Fatalf("expected stdout log to contain command output, got %q", stdout.String())
	}
}

func TestRunLocalJobFailure(t *testing.T) {
	workspace := t.TempDir()
	jobPath := writeLocalRunJob(t, t.TempDir(), localRunFailureScript())
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := runLocalJob(context.Background(), jobPath, workspace, &stdout, &stderr)
	if err == nil {
		t.Fatal("expected local job failure")
	}

	if !strings.Contains(err.Error(), "script failed") {
		t.Fatalf("expected script failure, got %v", err)
	}

	if !strings.Contains(stdout.String(), "local-fail") {
		t.Fatalf("expected stdout log to contain command output, got %q", stdout.String())
	}
}

func TestRunLocalJobValidation(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	if err := runLocalJob(context.Background(), "", t.TempDir(), &stdout, &stderr); err == nil {
		t.Fatal("expected missing job path error")
	}

	if err := runLocalJob(context.Background(), "missing.json", "", &stdout, &stderr); err == nil {
		t.Fatal("expected missing workspace error")
	}

	fileWorkspace := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(fileWorkspace, []byte("not a dir"), 0o644); err != nil {
		t.Fatalf("write workspace file: %v", err)
	}

	if err := runLocalJob(context.Background(), "missing.json", fileWorkspace, &stdout, &stderr); err == nil || !strings.Contains(err.Error(), "workspace is not a directory") {
		t.Fatalf("expected workspace directory error, got %v", err)
	}
}

func localRunSuccessScript() string {
	if runtime.GOOS == "windows" {
		return "Write-Output local-ok"
	}

	return "printf local-ok"
}

func localRunSecretScript() string {
	if runtime.GOOS == "windows" {
		return "Write-Output local-secret-alias-ok"
	}

	return "printf local-secret-alias-ok"
}

func localRunFailureScript() string {
	if runtime.GOOS == "windows" {
		return "Write-Output local-fail; exit 7"
	}

	return "printf local-fail; exit 7"
}
